class UserMetricsJob:
    def __init__(self):
        pass

    # Conversion of Java methods to Python methods will go here
    # For example, the main method will be converted to a Python function

    def main(self, events_path='sample_data/events.csv', users_path='sample_data/users.csv', out_path='out/user_metrics_parquet', min_date='1970-01-01', max_date='2100-01-01', use_udf=False):
        from pyspark.sql import SparkSession
        from pyspark.sql.functions import col, lit, when, to_timestamp

        spark = SparkSession.builder \n            .appName('UserMetricsJob') \n            .config('spark.sql.adaptive.enabled', 'true') \n            .config('spark.sql.shuffle.partitions', '8') \n            .getOrCreate()

        try:
            print(f'Starting job with events={events_path}, users={users_path}, out={out_path}, window=[{min_date}, {max_date}], useUdf={use_udf}')

            events = self.load_events(spark, events_path)
            users = self.load_users(spark, users_path)

            transformed = self.transform(events, users, min_date, max_date, use_udf)

            transformed.coalesce(1).write.mode('overwrite').format('parquet').save(out_path)

            transformed.show(truncate=False)

            print(f'Job completed successfully. Output: {out_path}')
        except Exception as e:
            print(f'Unexpected error: {e}')
            raise e
        finally:
            spark.stop()

    def load_events(self, spark, path):
        from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, TimestampType
        schema = StructType([
            StructField('user_id', StringType(), True),
            StructField('event_type', StringType(), True),
            StructField('score', IntegerType(), True),
            StructField('amount', DoubleType(), True),
            StructField('ts', TimestampType(), True)
        ])
        return spark.read.option('header', 'true').schema(schema).csv(path)

    def load_users(self, spark, path):
        from pyspark.sql.types import StructType, StructField, StringType
        schema = StructType([
            StructField('user_id', StringType(), False),
            StructField('country', StringType(), True)
        ])
        return spark.read.option('header', 'true').schema(schema).csv(path)

    def transform(self, events, users, min_date_inclusive, max_date_exclusive, use_udf_bucket):
        from pyspark.sql.functions import col, lit, when, to_timestamp

        in_window = col('ts').geq(to_timestamp(lit(min_date_inclusive))).and_(col('ts').lt(to_timestamp(lit(max_date_exclusive))))

        filtered = events.filter(col('event_type').isin('click', 'purchase')).filter(in_window)

        if use_udf_bucket:
            # Register UDF here if necessary
            pass
        else:
            filtered = filtered.withColumn(
                'score_bucket',
                when(col('score').isNull(), lit('unknown')).when(col('score').geq(lit(80)), lit('high'))
            )

        # Additional transformations will be added here

        return filtered

if __name__ == '__main__':
    job = UserMetricsJob()
    job.main()