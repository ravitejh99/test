import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, when, rank, to_timestamp
from pyspark.sql.window import Window

# UserMetricsJob class
class UserMetricsJob:
    def __init__(self, events_path, users_path, out_path, min_date, max_date, use_udf):
        self.events_path = events_path
        self.users_path = users_path
        self.out_path = out_path
        self.min_date = min_date
        self.max_date = max_date
        self.use_udf = use_udf

    def spark_session(self):
        return SparkSession.builder \
            .appName("UserMetricsJob") \
            .config("spark.sql.adaptive.enabled", "true") \
            .config("spark.sql.shuffle.partitions", "8") \
            .getOrCreate()

    def load_events(self, spark):
        schema = "user_id STRING, event_type STRING, score INT, amount DOUBLE, ts TIMESTAMP"
        return spark.read.option("header", "true").schema(schema).csv(self.events_path)

    def load_users(self, spark):
        schema = "user_id STRING, country STRING"
        return spark.read.option("header", "true").schema(schema).csv(self.users_path)

    def transform(self, events, users):
        in_window = col("ts").geq(to_timestamp(lit(self.min_date))) \
            .and_(col("ts").lt(to_timestamp(lit(self.max_date)))
        filtered = events.filter(col("event_type").isin("click", "purchase")) \
            .filter(in_window)

        if self.use_udf:
            # Register UDF here if needed
            pass
        else:
            filtered = filtered.withColumn(
                "score_bucket",
                when(col("score").isNull(), lit("unknown"))
                .when(col("score") >= lit(80), lit("high"))
                .otherwise(lit("low"))
            )

        aggregated = filtered.groupBy("user_id", "country") \
            .agg(
                sum("amount").alias("revenue"),
                count("event_type").alias("event_count")
            )

        ranked = aggregated.withColumn(
            "country_rank",
            rank().over(Window.partitionBy("country").orderBy(col("revenue").desc()))
        )

        return ranked

    def run(self):
        spark = self.spark_session()
        try:
            events = self.load_events(spark)
            users = self.load_users(spark)
            transformed = self.transform(events, users)
            transformed.coalesce(1).write.mode("overwrite").parquet(self.out_path)
            transformed.show()
        finally:
            spark.stop()

# Example usage
if __name__ == "__main__":
    job = UserMetricsJob(
        events_path="sample_data/events.csv",
        users_path="sample_data/users.csv",
        out_path="out/user_metrics_parquet",
        min_date="1970-01-01",
        max_date="2100-01-01",
        use_udf=False
    )
    job.run()