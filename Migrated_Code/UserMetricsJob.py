import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, to_timestamp, when, rank, broadcast
from pyspark.sql.window import Window

class UserMetricsJob:

    @staticmethod
    def load_events(spark, path):
        schema = """
            user_id STRING,
            event_type STRING,
            score INT,
            amount DOUBLE,
            ts TIMESTAMP
        """
        return spark.read.option("header", "true").schema(schema).csv(path)

    @staticmethod
    def load_users(spark, path):
        schema = """
            user_id STRING,
            country STRING
        """
        return spark.read.option("header", "true").schema(schema).csv(path)

    @staticmethod
    def transform(events, users, min_date, max_date, use_udf_bucket):
        in_window = (col("ts") >= to_timestamp(lit(min_date))) & (col("ts") < to_timestamp(lit(max_date)))
        filtered = events.filter((col("event_type").isin("click", "purchase")) & in_window)

        if use_udf_bucket:
            # Placeholder for UDF bucket logic
            pass
        else:
            filtered = filtered.withColumn(
                "score_bucket",
                when(col("score").isNull(), lit("unknown"))
                .when(col("score") >= 80, lit("high"))
                .otherwise(lit("low"))
            )

        aggregated = filtered.groupBy("user_id").agg(
            sum("amount").alias("revenue"),
            count("event_type").alias("event_count")
        )

        joined = aggregated.join(broadcast(users), "user_id", "left")

        window_spec = Window.partitionBy("country").orderBy(col("revenue").desc())
        ranked = joined.withColumn("country_rank", rank().over(window_spec))

        return ranked

    @staticmethod
    def main():
        spark = SparkSession.builder \
            .appName("UserMetricsJob") \
            .config("spark.sql.adaptive.enabled", "true") \
            .config("spark.sql.shuffle.partitions", "8") \
            .getOrCreate()

        events_path = "sample_data/events.csv"
        users_path = "sample_data/users.csv"
        out_path = "out/user_metrics_parquet"
        min_date = "1970-01-01"
        max_date = "2100-01-01"
        use_udf = False

        events = UserMetricsJob.load_events(spark, events_path)
        users = UserMetricsJob.load_users(spark, users_path)

        transformed = UserMetricsJob.transform(events, users, min_date, max_date, use_udf)

        transformed.coalesce(1).write.mode("overwrite").parquet(out_path)

        transformed.show()

        spark.stop()

if __name__ == "__main__":
    UserMetricsJob.main()