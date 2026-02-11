# Python code equivalent of UserMetricsJob in PySpark

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, when, to_timestamp, broadcast
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, TimestampType
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
log = logging.getLogger("UserMetricsJob")

def get_arg(args, key, default):
    if key in args:
        return args[args.index(key) + 1]
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

def transform(events, users, min_date, max_date, use_udf_bucket):
    in_window = (col("ts") >= to_timestamp(lit(min_date))) & (col("ts") < to_timestamp(lit(max_date)))

    filtered = events.filter((col("event_type").isin("click", "purchase")) & in_window)

    if use_udf_bucket:
        # Register UDF if needed
        pass
    else:
        filtered = filtered.withColumn(
            "score_bucket",
            when(col("score").isNull(), lit("unknown"))
            .when(col("score") >= 80, lit("high"))
            .otherwise(lit("low"))
        )

    agg = filtered.groupBy("user_id").agg(
        sum("amount").alias("revenue"),
        count("event_type").alias("event_count")
    )

    joined = agg.join(broadcast(users), "user_id", "inner")

    window_spec = Window.partitionBy("country").orderBy(col("revenue").desc())
    ranked = joined.withColumn("country_rank", rank().over(window_spec))

    return ranked.orderBy("country", "country_rank")

def main(args):
    events_path = get_arg(args, "--events", "sample_data/events.csv")
    users_path = get_arg(args, "--users", "sample_data/users.csv")
    out_path = get_arg(args, "--out", "out/user_metrics_parquet")
    min_date = get_arg(args, "--from", "1970-01-01")
    max_date = get_arg(args, "--to", "2100-01-01")
    use_udf = get_arg(args, "--useUdf", "false").lower() == "true"

    spark = SparkSession.builder \n        .appName("UserMetricsJob") \n        .config("spark.sql.adaptive.enabled", "true") \n        .config("spark.sql.shuffle.partitions", "8") \n        .getOrCreate()

    try:
        log.info("Starting job with events=%s, users=%s, out=%s, window=[%s, %s], useUdf=%s", 
                 events_path, users_path, out_path, min_date, max_date, use_udf)

        events = load_events(spark, events_path)
        users = load_users(spark, users_path)

        transformed = transform(events, users, min_date, max_date, use_udf)

        transformed.coalesce(1).write.mode("overwrite").parquet(out_path)

        transformed.show(truncate=False)

        log.info("Job completed successfully. Output: %s", out_path)

    except Exception as e:
        log.error("Unexpected error: %s", e)
        raise

    finally:
        spark.stop()

if __name__ == "__main__":
    import sys
    main(sys.argv)