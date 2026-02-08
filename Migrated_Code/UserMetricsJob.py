import pyspark.sql.functions as F
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, TimestampType
from pyspark.sql.window import Window

class UserMetricsJob:

    def __init__(self, events_path, users_path, out_path, min_date, max_date, use_udf):
        self.events_path = events_path
        self.users_path = users_path
        self.out_path = out_path
        self.min_date = min_date
        self.max_date = max_date
        self.use_udf = use_udf
        self.spark = SparkSession.builder \
            .appName("UserMetricsJob") \
            .config("spark.sql.adaptive.enabled", "true") \
            .config("spark.sql.shuffle.partitions", "8") \
            .getOrCreate()

    def load_events(self):
        schema = StructType([
            StructField("user_id", StringType(), True),
            StructField("event_type", StringType(), True),
            StructField("score", IntegerType(), True),
            StructField("amount", DoubleType(), True),
            StructField("ts", TimestampType(), True)
        ])
        return self.spark.read.option("header", "true").schema(schema).csv(self.events_path)

    def load_users(self):
        schema = StructType([
            StructField("user_id", StringType(), False),
            StructField("country", StringType(), True)
        ])
        return self.spark.read.option("header", "true").schema(schema).csv(self.users_path)

    def transform(self, events, users):
        in_window = (F.col("ts") >= F.to_timestamp(F.lit(self.min_date))) & \
                    (F.col("ts") < F.to_timestamp(F.lit(self.max_date)))

        filtered = events.filter(F.col("event_type").isin("click", "purchase")).filter(in_window)

        if self.use_udf:
            filtered = filtered.withColumn("score_bucket", F.when(F.col("score") >= 80, "high")
                                            .when(F.col("score") >= 50, "medium")
                                            .otherwise("low"))
        else:
            filtered = filtered.withColumn("score_bucket", F.when(F.col("score") >= 80, "high")
                                            .when(F.col("score") >= 50, "medium")
                                            .otherwise("low"))

        aggregated = filtered.groupBy("user_id").agg(
            F.sum("amount").alias("revenue"),
            F.count("event_type").alias("event_count")
        )

        joined = aggregated.join(users, "user_id", "left")

        window_spec = Window.partitionBy("country").orderBy(F.desc("revenue"))
        ranked = joined.withColumn("country_rank", F.rank().over(window_spec))

        return ranked

    def run(self):
        try:
            events = self.load_events()
            users = self.load_users()
            transformed = self.transform(events, users)
            transformed.write.mode("overwrite").parquet(self.out_path)
            transformed.show()
        except Exception as e:
            print(f"Error: {e}")
        finally:
            self.spark.stop()

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