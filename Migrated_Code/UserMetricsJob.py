# Python equivalent code for UserMetricsJob.java

# Importing required libraries
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, when, to_timestamp, broadcast
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, TimestampType
import logging

# Setting up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("UserMetricsJob")

def main():
    # Reading arguments or setting default values
    events_path = "sample_data/events.csv"
    users_path = "sample_data/users.csv"
    out_path = "out/user_metrics_parquet"
    min_date = "1970-01-01"
    max_date = "2100-01-01"
    use_udf = False

    # Initializing SparkSession
    spark = SparkSession.builder \
        .appName("UserMetricsJob") \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.shuffle.partitions", "8") \
        .getOrCreate()

    try:
        logger.info(f"Starting job with events={events_path}, users={users_path}, out={out_path}, window=[{min_date}, {max_date}], useUdf={use_udf}")

        events = load_events(spark, events_path)
        users = load_users(spark, users_path)

        transformed = transform(events, users, min_date, max_date, use_udf)

        # Write Parquet output
        transformed.coalesce(1).write.mode("overwrite").format("parquet").save(out_path)

        # For quick visibility in logs
        transformed.show(truncate=False)

        logger.info(f"Job completed successfully. Output: {out_path}")
    except Exception as e:
        logger.error(f"Unexpected error: {e}")
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

    return spark.read.option("header", "true").schema(schema).csv(path)


def load_users(spark, path):
    schema = StructType([
        StructField("user_id", StringType(), False),
        StructField("country", StringType(), True)
    ])

    return spark.read.option("header", "true").schema(schema).csv(path)


def transform(events, users, min_date, max_date, use_udf):
    in_window = col("ts").geq(to_timestamp(lit(min_date))) & col("ts").lt(to_timestamp(lit(max_date)))

    filtered = events.filter(col("event_type").isin("click", "purchase")).filter(in_window)

    if use_udf:
        filtered = filtered.withColumn("score_bucket", when(col("score") >= 80, lit("high")).otherwise(lit("low")))

    aggregated = filtered.groupBy("user_id").agg({"amount": "sum", "event_type": "count"}).withColumnRenamed("sum(amount)", "revenue").withColumnRenamed("count(event_type)", "event_count")

    joined = broadcast(users).join(aggregated, "user_id")

    return joined

if __name__ == "__main__":
    main()