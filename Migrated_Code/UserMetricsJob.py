class UserMetricsJob:
    def __init__(self):
        pass

    @staticmethod
    def main(events_path='sample_data/events.csv', users_path='sample_data/users.csv', out_path='out/user_metrics_parquet', min_date='1970-01-01', max_date='2100-01-01', use_udf=False):
        from pyspark.sql import SparkSession
        from pyspark.sql.functions import col, lit, to_timestamp, when
        from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, TimestampType
        import logging

        logging.basicConfig(level=logging.INFO)
        log = logging.getLogger('UserMetricsJob')

        spark = SparkSession.builder \n            .appName('UserMetricsJob') \n            .config('spark.sql.adaptive.enabled', 'true') \n            .config('spark.sql.shuffle.partitions', '8') \n            .getOrCreate()

        try:
            log.info(f'Starting job with events={events_path}, users={users_path}, out={out_path}, window=[{min_date}, {max_date}], useUdf={use_udf}')

            events = UserMetricsJob.load_events(spark, events_path)
            users = UserMetricsJob.load_users(spark, users_path)

            transformed = UserMetricsJob.transform(events, users, min_date, max_date, use_udf)

            transformed.coalesce(1).write.mode('overwrite').format('parquet').save(out_path)
            transformed.show(truncate=False)

            log.info(f'Job completed successfully. Output: {out_path}')
        except Exception as e:
            log.error(f'Unexpected error: {e}')
            raise RuntimeError('Unhandled exception') from e
        finally:
            spark.stop()

    @staticmethod
    def load_events(spark, path):
        schema = StructType([
            StructField('user_id', StringType(), True),
            StructField('event_type', StringType(), True),
            StructField('score', IntegerType(), True),
            StructField('amount', DoubleType(), True),
            StructField('ts', TimestampType(), True)
        ])

        return spark.read.option('header', 'true').schema(schema).csv(path)

    @staticmethod
    def load_users(spark, path):
        schema = StructType([
            StructField('user_id', StringType(), False),
            StructField('country', StringType(), True)
        ])

        return spark.read.option('header', 'true').schema(schema).csv(path)

    @staticmethod
    def transform(events, users, min_date_inclusive, max_date_exclusive, use_udf_bucket):
        in_window = col('ts').geq(to_timestamp(lit(min_date_inclusive))) & col('ts').lt(to_timestamp(lit(max_date_exclusive)))

        filtered = events.filter(col('event_type').isin('click', 'purchase')).filter(in_window)

        if use_udf_bucket:
            # Placeholder for UDF registration
            pass
        else:
            filtered = filtered.withColumn(
                'score_bucket',
                when(col('score').isNull(), lit('unknown')).when(col('score') >= 80, lit('high')).otherwise(lit('low'))
            )

        return filtered

if __name__ == '__main__':
    UserMetricsJob.main()