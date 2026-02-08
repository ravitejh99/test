import pyspark.sql.functions as F
from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, TimestampType
import logging

def get_arg(args, key, default):
    try:
        index = args.index(key)
        return args[index + 1]
    except ValueError:
        return default

def spark_register_bucket_udf(spark):
    def bucket_score(score):
        if score is None:
            return "unknown"
        elif score >= 80:
            return "high"
        elif score >= 50:
            return "medium"
        else:
            return "low"
    spark.udf.register("bucketScore", bucket_score)

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
    in_window = F.col("ts").geq(F.to_timestamp(F.lit(min_date_inclusive))).and(F.col("ts").lt(F.to_timestamp(F.lit(max_date_exclusive)))
    filtered = events.filter(F.col("event_type").isin("click", "purchase")).filter(in_window)
    if use_udf_bucket:
        spark_register_bucket_udf(filtered.sparkSession)
        filtered = filtered.withColumn("score_bucket", F.expr("bucketScore(score)"))
    else:
        filtered = filtered.withColumn(
            "score_bucket",
            F.when(F.col("score").isNull(), F.lit("unknown"))
            .when(F.col("score").geq(F.lit(80)), F.lit("high"))
            .when(F.col("score").geq(F.lit(50)), F.lit("medium"))
            .otherwise(F.lit("low"))
        )
    aggregated = filtered.groupBy("user_id").agg(
        F.sum("amount").alias("revenue"),
        F.count("event_type").alias("event_count")
    )
    joined = aggregated.join(users, "user_id", "left")
    window_spec = Window.partitionBy("country").orderBy(F.col("revenue").desc())
    ranked = joined.withColumn("country_rank", F.rank().over(window_spec))
    return ranked.orderBy("country", "country_rank")

def main(args):
    logging.basicConfig(level=logging.INFO)
    log = logging.getLogger("UserMetricsJob")

    events_path = get_arg(args, "--events", "sample_data/events.csv")
    users_path = get_arg(args, "--users", "sample_data/users.csv")
    out_path = get_arg(args, "--out", "out/user_metrics_parquet")
    min_date = get_arg(args, "--from", "1970-01-01")
    max_date = get_arg(args, "--to", "2100-01-01")
    use_udf = bool(get_arg(args, "--useUdf", "false"))

    spark = SparkSession.builder.appName("UserMetricsJob").config("spark.sql.adaptive.enabled", "true").config("spark.sql.shuffle.partitions", "8").getOrCreate()

    try:
        log.info("Starting job with events=%s, users=%s, out=%s, window=[%s, %s], useUdf=%s", events_path, users_path, out_path, min_date, max_date, use_udf)
        events = load_events(spark, events_path)
        users = load_users(spark, users_path)
        transformed = transform(events, users, min_date, max_date, use_udf)
        transformed.coalesce(1).write.mode("overwrite").format("parquet").save(out_path)
        transformed.show()
        log.info("Job completed successfully. Output: %s", out_path)
    except Exception as e:
        log.error("Unexpected error: %s", e)
        raise
    finally:
        spark.stop()

if __name__ == "__main__":
    import sys
    main(sys.argv)