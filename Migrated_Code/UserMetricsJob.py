import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, when, to_timestamp, udf, broadcast
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, TimestampType
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("UserMetricsJob")

def get_arg(args, key, default):
    for arg in args:
        if arg.startswith(key):
            return arg.split("=", 1)[1]
    return default

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

def transform(events, users, min_date_inclusive, max_date_exclusive, use_udf_bucket):
    in_window = (col("ts") >= to_timestamp(lit(min_date_inclusive))) & (col("ts") < to_timestamp(lit(max_date_exclusive)))

    filtered = events.filter(col("event_type").isin("click", "purchase")).filter(in_window)

    if use_udf_bucket:
        @udf("string")
        def bucket_score(score):
            if score is None:
                return "unknown"
            elif score >= 80:
                return "high"
            elif score >= 50:
                return "medium"
            else:
                return "low"

        filtered = filtered.withColumn("score_bucket", bucket_score(col("score")))
    else:
        filtered = filtered.withColumn(
            "score_bucket",
            when(col("score").isNull(), lit("unknown"))
            .when(col("score") >= 80, lit("high"))
            .when(col("score") >= 50, lit("medium"))
            .otherwise(lit("low"))
        )

    aggregated = filtered.groupBy("user_id").agg(
        sum("amount").alias("revenue"),
        count("event_type").alias("event_count")
    )

    joined = aggregated.join(broadcast(users), "user_id", "inner")

    window_spec = Window.partitionBy("country").orderBy(col("revenue").desc())
    ranked = joined.withColumn("country_rank", rank().over(window_spec))

    return ranked.orderBy("country", "country_rank")

if __name__ == "__main__":
    import sys

    args = sys.argv[1:]
    events_path = get_arg(args, "--events", "sample_data/events.csv")
    users_path = get_arg(args, "--users", "sample_data/users.csv")
    out_path = get_arg(args, "--out", "out/user_metrics_parquet")
    min_date = get_arg(args, "--from", "1970-01-01")
    max_date = get_arg(args, "--to", "2100-01-01")
    use_udf = get_arg(args, "--useUdf", "false").lower() == "true"

    spark = SparkSession.builder \n        .appName("UserMetricsJob") \n        .config("spark.sql.adaptive.enabled", "true") \n        .config("spark.sql.shuffle.partitions", "8") \n        .getOrCreate()

    try:
        logger.info(f"Starting job with events={events_path}, users={users_path}, out={out_path}, window=[{min_date}, {max_date}], useUdf={use_udf}")

        events = load_events(spark, events_path)
        users = load_users(spark, users_path)

        transformed = transform(events, users, min_date, max_date, use_udf)

        transformed.coalesce(1).write.mode("overwrite").parquet(out_path)

        transformed.show(truncate=False)

        logger.info(f"Job completed successfully. Output: {out_path}")
    except Exception as e:
        logger.error(f"Unexpected error: {e}", exc_info=True)
        raise
    finally:
        spark.stop()