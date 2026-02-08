class UserMetricsJob:
    def __init__(self):
        pass

    @staticmethod
    def get_arg(args, key, default):
        if key in args:
            return args[key]
        return default

    @staticmethod
    def main(args):
        import logging
        from pyspark.sql import SparkSession
        from pyspark.sql import functions as F
        from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, TimestampType

        logging.basicConfig(level=logging.INFO)
        log = logging.getLogger("UserMetricsJob")

        events_path = UserMetricsJob.get_arg(args, "--events", "sample_data/events.csv")
        users_path = UserMetricsJob.get_arg(args, "--users", "sample_data/users.csv")
        out_path = UserMetricsJob.get_arg(args, "--out", "out/user_metrics_parquet")
        min_date = UserMetricsJob.get_arg(args, "--from", "1970-01-01")
        max_date = UserMetricsJob.get_arg(args, "--to", "2100-01-01")
        use_udf = UserMetricsJob.get_arg(args, "--useUdf", "false").lower() == "true"

        spark = SparkSession.builder \n            .appName("UserMetricsJob") \n            .config("spark.sql.adaptive.enabled", "true") \n            .config("spark.sql.shuffle.partitions", "8") \n            .getOrCreate()

        try:
            log.info(f"Starting job with events={events_path}, users={users_path}, out={out_path}, window=[{min_date}, {max_date}], useUdf={use_udf}")

            events = UserMetricsJob.load_events(spark, events_path)
            users = UserMetricsJob.load_users(spark, users_path)

            transformed = UserMetricsJob.transform(events, users, min_date, max_date, use_udf)

            transformed.coalesce(1).write.mode("overwrite").format("parquet").save(out_path)
            transformed.show(truncate=False)

            log.info(f"Job completed successfully. Output: {out_path}")
        except Exception as e:
            log.error(f"Unexpected error: {e}", exc_info=True)
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
    def transform(events, users, min_date, max_date, use_udf):
        in_window = (F.col("ts") >= F.to_timestamp(F.lit(min_date))) & (F.col("ts") < F.to_timestamp(F.lit(max_date)))

        filtered = events.filter((F.col("event_type").isin("click", "purchase")) & in_window)

        if use_udf:
            def bucket_score(score):
                if score is None:
                    return "unknown"
                elif score >= 80:
                    return "high"
                elif score >= 50:
                    return "medium"
                else:
                    return "low"

            spark.udf.register("bucketScore", bucket_score, StringType())
            filtered = filtered.withColumn("score_bucket", F.expr("bucketScore(score)"))
        else:
            filtered = filtered.withColumn(
                "score_bucket",
                F.when(F.col("score").isNull(), F.lit("unknown"))
                .when(F.col("score") >= 80, F.lit("high"))
                .when(F.col("score") >= 50, F.lit("medium"))
                .otherwise(F.lit("low"))
            )

        aggregated = filtered.groupBy("user_id").agg(
            F.sum("amount").alias("revenue"),
            F.count("event_type").alias("event_count")
        )

        joined = aggregated.join(users.hint("broadcast"), "user_id")

        window_spec = F.window.partitionBy("country").orderBy(F.desc("revenue"))
        ranked = joined.withColumn("country_rank", F.row_number().over(window_spec))

        return ranked.orderBy("country", "country_rank")

if __name__ == "__main__":
    import sys
    UserMetricsJob.main(sys.argv)