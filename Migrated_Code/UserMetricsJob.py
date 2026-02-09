class UserMetricsJob:
    import pyspark.sql.functions as F
    from pyspark.sql import SparkSession
    from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, TimestampType

    @staticmethod
    def main(events_path="sample_data/events.csv", users_path="sample_data/users.csv", out_path="out/user_metrics_parquet", min_date="1970-01-01", max_date="2100-01-01", use_udf=False):
        spark = SparkSession.builder \
            .appName("UserMetricsJob") \
            .config("spark.sql.adaptive.enabled", "true") \
            .config("spark.sql.shuffle.partitions", "8") \
            .getOrCreate()

        try:
            print(f"Starting job with events={events_path}, users={users_path}, out={out_path}, window=[{min_date}, {max_date}], useUdf={use_udf}")

            events = UserMetricsJob.load_events(spark, events_path)
            users = UserMetricsJob.load_users(spark, users_path)

            transformed = UserMetricsJob.transform(events, users, min_date, max_date, use_udf)

            # Write Parquet output
            transformed.coalesce(1).write.mode("overwrite").format("parquet").save(out_path)

            # For quick visibility in tests/logs
            transformed.show(truncate=False)

            print(f"Job completed successfully. Output: {out_path}")
        except Exception as e:
            print(f"Unexpected error: {e}")
            raise RuntimeError("Unhandled exception")
        finally:
            spark.stop()

    @staticmethod
    def load_events(spark, path):
        schema = StructType([
            StructField("user_id", StringType(), True),
            StructField("event_type", StringType(), True),
            StructField("score", IntegerType(), True),
            StructField("amount", DoubleType(), True),
            StructField("ts", TimestampType(), True)
        ])
        return spark.read.option("header", "true").schema(schema).csv(path)

    @staticmethod
    def load_users(spark, path):
        schema = StructType([
            StructField("user_id", StringType(), False),
            StructField("country", StringType(), True)
        ])
        return spark.read.option("header", "true").schema(schema).csv(path)

    @staticmethod
    def transform(events, users, min_date_inclusive, max_date_exclusive, use_udf_bucket):
        in_window = (F.col("ts") >= F.to_timestamp(F.lit(min_date_inclusive))) & (F.col("ts") < F.to_timestamp(F.lit(max_date_exclusive)))

        filtered = events.filter(F.col("event_type").isin("click", "purchase")).filter(in_window)

        if use_udf_bucket:
            filtered = filtered.withColumn("score_bucket", F.udf(lambda score: "unknown" if score is None else "high" if score >= 80 else "low")(F.col("score")))
        else:
            filtered = filtered.withColumn("score_bucket", F.when(F.col("score").isNull(), F.lit("unknown")).when(F.col("score") >= F.lit(80), F.lit("high")).otherwise(F.lit("low")))

        aggregated = filtered.groupBy("user_id").agg(
            F.sum("amount").alias("revenue"),
            F.count("event_type").alias("event_count")
        )

        joined = aggregated.join(users, on="user_id", how="left")

        window_spec = Window.partitionBy("country").orderBy(F.desc("revenue"))
        ranked = joined.withColumn("country_rank", F.row_number().over(window_spec))

        return ranked.orderBy("country", "country_rank")