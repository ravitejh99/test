import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, when, to_timestamp, broadcast, row_number
from pyspark.sql.window import Window
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, TimestampType

# UserMetricsJob
# PySpark equivalent of the Java-based Spark ETL job

def load_events(spark, path):
    schema = StructType([
        StructField("user_id", StringType(), True),
        StructField("event_type", StringType(), True),
        StructField("score", IntegerType(), True),
        StructField("amount", DoubleType(), True),
        StructField("ts", TimestampType(), True)
    ])
    return spark.read.option("header", "true").schema(schema).csv(path)

def load_users(spark, path):
    schema = StructType([
        StructField("user_id", StringType(), False),
        StructField("country", StringType(), True)
    ])
    return spark.read.option("header", "true").schema(schema).csv(path)

def transform(events, users, min_date, max_date, use_udf_bucket):
    in_window = col("ts").geq(to_timestamp(lit(min_date))).and(col("ts").lt(to_timestamp(lit(max_date)))
    filtered = events.filter(col("event_type").isin(["click", "purchase"])).filter(in_window)

    if use_udf_bucket:
        spark.udf.register("bucket_score", lambda score: "unknown" if score is None else "high" if score >= 80 else "medium" if score >= 50 else "low")
        filtered = filtered.withColumn("score_bucket", col("bucket_score"))
    else:
        filtered = filtered.withColumn("score_bucket", when(col("score").isNull(), lit("unknown")).when(col("score").geq(lit(80)), lit("high")).when(col("score").geq(lit(50)), lit("medium")).otherwise(lit("low")))

    aggregated = filtered.groupBy("user_id").agg(
        sum("amount").alias("revenue"),
        count("event_type").alias("event_count")
    )

    joined = broadcast(users).join(aggregated, "user_id")

    window_spec = Window.partitionBy("country").orderBy(col("revenue").desc())
    ranked = joined.withColumn("country_rank", row_number().over(window_spec))

    return ranked.orderBy("country", "country_rank")

def main():
    spark = SparkSession.builder.appName("UserMetricsJob").config("spark.sql.adaptive.enabled", "true").config("spark.sql.shuffle.partitions", "8").getOrCreate()

    events_path = "sample_data/events.csv"
    users_path = "sample_data/users.csv"
    out_path = "out/user_metrics_parquet"
    min_date = "1970-01-01"
    max_date = "2100-01-01"
    use_udf = False

    events = load_events(spark, events_path)
    users = load_users(spark, users_path)

    transformed = transform(events, users, min_date, max_date, use_udf)

    transformed.coalesce(1).write.mode("overwrite").format("parquet").save(out_path)
    transformed.show()

    spark.stop()

if __name__ == "__main__":
    main()