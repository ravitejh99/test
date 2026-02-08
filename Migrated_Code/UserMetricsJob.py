class UserMetricsJob:
    def __init__(self):
        pass

    def main(self, events_path, users_path, out_path, min_date, max_date, use_udf):
        from pyspark.sql import SparkSession
        from pyspark.sql.functions import col, lit, to_timestamp, when, call_udf
        from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, TimestampType

        spark = SparkSession.builder \
            .appName("UserMetricsJob") \
            .config("spark.sql.adaptive.enabled", "true") \
            .config("spark.sql.shuffle.partitions", "8") \
            .getOrCreate()

        try:
            events = self.load_events(spark, events_path)
            users = self.load_users(spark, users_path)

            transformed = self.transform(events, users, min_date, max_date, use_udf)

            transformed.coalesce(1).write.mode("overwrite").parquet(out_path)
            transformed.show()

        except Exception as e:
            print(f"Error: {e}")
        finally:
            spark.stop()

    def load_events(self, spark, path):
        schema = StructType([
            StructField("user_id", StringType(), True),
            StructField("event_type", StringType(), True),
            StructField("score", IntegerType(), True),
            StructField("amount", DoubleType(), True),
            StructField("ts", TimestampType(), True)
        ])

        return spark.read.option("header", "true").schema(schema).csv(path)

    def load_users(self, spark, path):
        schema = StructType([
            StructField("user_id", StringType(), False),
            StructField("country", StringType(), True)
        ])

        return spark.read.option("header", "true").schema(schema).csv(path)

    def transform(self, events, users, min_date, max_date, use_udf):
        in_window = (col("ts") >= to_timestamp(lit(min_date))) & (col("ts") < to_timestamp(lit(max_date)))

        filtered = events.filter(col("event_type").isin("click", "purchase")).filter(in_window)

        if use_udf:
            def bucket_score(score):
                if score is None:
                    return "unknown"
                elif score >= 80:
                    return "high"
                else:
                    return "low"

            spark.udf.register("bucketScore", bucket_score)
            filtered = filtered.withColumn("score_bucket", call_udf("bucketScore", col("score")))
        else:
            filtered = filtered.withColumn("score_bucket", when(col("score").isNull(), lit("unknown")).when(col("score") >= lit(80), lit("high")).otherwise(lit("low")))

        return filtered
