import pyspark.sql.functions as F
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, DateType
from datetime import datetime

class OrderProcessor:
    def __init__(self, spark):
        self.spark = spark

    def read_orders_from_csv(self, path):
        schema = StructType([
            StructField('orderId', StringType(), True),
            StructField('customer', StringType(), True),
            StructField('status', StringType(), True),
            StructField('amount', DoubleType(), True),
            StructField('date', DateType(), True)
        ])
        return self.spark.read.csv(path, schema=schema, header=True)

    def process_orders(self, orders_df):
        delivered_df = orders_df.filter(orders_df.status == 'DELIVERED')
        total_revenue_delivered = delivered_df.agg(F.sum('amount').alias('total_revenue')).collect()[0]['total_revenue']
        cancelled_count = orders_df.filter(orders_df.status == 'CANCELLED').count()

        revenue_by_customer = delivered_df.groupBy('customer').agg(F.sum('amount').alias('revenue')).orderBy(F.desc('revenue'))
        top_customers = revenue_by_customer.limit(3).collect()

        monthly_revenue = delivered_df.withColumn('month', F.date_format('date', 'yyyy-MM')).groupBy('month').agg(F.sum('amount').alias('monthly_revenue')).orderBy('month')

        invalid_orders_count = orders_df.filter(
            (orders_df.orderId.isNull()) | (orders_df.customer.isNull()) | (orders_df.amount < 0) | (orders_df.date.isNull())
        ).count()

        print("=== Order Summary ===")
        print(f"Total orders: {orders_df.count()}")
        print(f"Delivered revenue: {total_revenue_delivered}")
        print(f"Cancelled count: {cancelled_count}")
        print(f"Invalid orders: {invalid_orders_count}")

        print("\n=== Revenue by Customer (DELIVERED) ===")
        revenue_by_customer.show()

        print("\n=== Top Customers ===")
        for idx, row in enumerate(top_customers):
            print(f"{idx + 1}) {row['customer']}: {row['revenue']}")

        print("\n=== Monthly Revenue (DELIVERED) ===")
        monthly_revenue.show()

if __name__ == '__main__':
    spark = SparkSession.builder.appName("OrderProcessor").getOrCreate()
    processor = OrderProcessor(spark)

    # Replace 'path_to_csv' with the actual path to your CSV file
    path_to_csv = "path/to/orders.csv"
    orders_df = processor.read_orders_from_csv(path_to_csv)
    processor.process_orders(orders_df)