import pyspark
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, TimestampType

# UserMetricsJob
# Demonstrates common Spark patterns your conversion/documentation agent should handle:
# - SparkSession config (AQE, shuffle)
# - Reading CSV with explicit schema
# - Filters with null/edge-case handling
# - UDF vs. built-in column expressions
# - Joins (with broadcast hint)
# - Window functions (rank per country)
# - Error handling + logging
# - Deterministic output ordering for validation

def main():
    events_path = "sample_data/events.csv"
    users_path = "sample_data/users.csv"
    out_path = "out/user_metrics_parquet"
    min_date = "1970-01-01"
    max_date = "2100-01-01"
    use_udf = False

    spark = SparkSession.builder \
        .appName("UserMetricsJob") \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.shuffle.partitions", "8") \
        .getOrCreate()

    try:
        print(f"Starting job with events={events_path}, users={users_path}, out={out_path}, window=[{min_date}, {max_date}], useUdf={use_udf}")

        events = load_events(spark, events_path)
        users = load_users(spark, users_path)

        transformed = transform(events, users, min_date, max_date, use_udf)

        # Write Parquet output
        transformed \
            .coalesce(1) \
            .write \
            .mode("overwrite") \
            .format("parquet") \
            .save(out_path)

        # For quick visibility in tests/logs
        transformed.show(truncate=False)

        print(f"Job completed successfully. Output: {out_path}")
    except Exception as e:
        print(f"Unexpected error: {e}")
        raise
    finally:
        spark.stop()


def load_events(spark, path):
    schema = StructType([
        StructField("user_id", StringType(), True),
        StructField("event_type", StringType(), True),
        StructField("score", IntegerType(), True),
        StructField("amount", DoubleType(), True),
        StructField("ts", TimestampType(), True)
    ])

    return spark.read \
        .option("header", "true") \
        .schema(schema) \
        .csv(path)


def load_users(spark, path):
    schema = StructType([
        StructField("user_id", StringType(), False),
        StructField("country", StringType(), True)
    ])

    return spark.read \
        .option("header", "true") \
        .schema(schema) \
        .csv(path)


def transform(events, users, min_date_inclusive, max_date_exclusive, use_udf_bucket):
    in_window = (F.col("ts") >= F.to_timestamp(F.lit(min_date_inclusive))) & (F.col("ts") < F.to_timestamp(F.lit(max_date_exclusive)))

    filtered = events \
        .filter(F.col("event_type").isin("click", "purchase")) \
        .filter(in_window)

    if use_udf_bucket:
        filtered = filtered.withColumn("score_bucket", F.udf(lambda score: "high" if score >= 80 else "low" if score >= 50 else "unknown")(F.col("score")))
    else:
        filtered = filtered.withColumn(
            "score_bucket",
            F.when(F.col("score").isNull(), F.lit("unknown"))
             .when(F.col("score") >= 80, F.lit("high"))
             .otherwise(F.lit("low"))
        )

    aggregated = filtered \
        .groupBy("user_id") \
        .agg(
            F.sum("amount").alias("revenue"),
            F.count("event_type").alias("event_count")
        )

    joined = aggregated.join(F.broadcast(users), "user_id", "inner")

    window_spec = Window.partitionBy("country").orderBy(F.desc("revenue"))

    ranked = joined \
        .withColumn("country_rank", F.rank().over(window_spec))

    return ranked.orderBy("country", "country_rank")


if __name__ == "__main__":
    main()