import pyspark.sql.functions as F
from pyspark.sql import SparkSession, Window
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, TimestampType
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("UserMetricsJob")

def get_arg(args, key, default):
    return next((arg.split('=')[1] for arg in args if arg.startswith(key)), default)

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
    in_window = (F.col("ts") >= F.lit(min_date)) & (F.col("ts") < F.lit(max_date))
    filtered = events.filter(F.col("event_type").isin(["click", "purchase"]))
    filtered = filtered.filter(in_window)

    if use_udf_bucket:
        spark.udf.register("bucketScore", lambda score: "high" if score >= 80 else "low" if score < 80 else "unknown")
        filtered = filtered.withColumn("score_bucket", F.expr("bucketScore(score)"))
    else:
        filtered = filtered.withColumn("score_bucket", F.when(F.col("score") >= 80, "high")
                                         .when(F.col("score") < 80, "low")
                                         .otherwise("unknown"))

    aggregated = filtered.groupBy("user_id").agg(
        F.sum("amount").alias("revenue"),
        F.count("event_type").alias("event_count")
    )

    joined = aggregated.join(users, "user_id", "left")
    window_spec = Window.partitionBy("country").orderBy(F.desc("revenue"))
    ranked = joined.withColumn("country_rank", F.rank().over(window_spec))

    return ranked.orderBy("country", "country_rank")

def main(args):
    events_path = get_arg(args, "--events", "sample_data/events.csv")
    users_path = get_arg(args, "--users", "sample_data/users.csv")
    out_path = get_arg(args, "--out", "out/user_metrics_parquet")
    min_date = get_arg(args, "--from", "1970-01-01")
    max_date = get_arg(args, "--to", "2100-01-01")
    use_udf_bucket = bool(get_arg(args, "--useUdf", "false"))

    spark = SparkSession.builder.appName("UserMetricsJob").getOrCreate()

    try:
        logger.info("Starting job with events=%s, users=%s, out=%s, window=[%s, %s], useUdf=%s", events_path, users_path, out_path, min_date, max_date, use_udf_bucket)
        events = load_events(spark, events_path)
        users = load_users(spark, users_path)
        transformed = transform(events, users, min_date, max_date, use_udf_bucket)

        transformed.coalesce(1).write.mode("overwrite").parquet(out_path)
        transformed.show()

        logger.info("Job completed successfully. Output: %s", out_path)
    except Exception as e:
        logger.error("Error: %s", e)
    finally:
        spark.stop()

if __name__ == "__main__":
    import sys
    main(sys.argv[1:])