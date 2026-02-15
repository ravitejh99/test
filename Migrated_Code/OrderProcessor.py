import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum as _sum, count
from datetime import datetime

class OrderProcessor:
    class Status:
        NEW = 'NEW'
        PROCESSING = 'PROCESSING'
        DELIVERED = 'DELIVERED'
        CANCELLED = 'CANCELLED'

    def __init__(self, spark_session):
        self.spark = spark_session

    def read_orders_from_csv(self, csv_path):
        schema = [
            ('orderId', 'string'),
            ('customer', 'string'),
            ('status', 'string'),
            ('amount', 'float'),
            ('date', 'string')
        ]
        orders = self.spark.read.csv(csv_path, header=True, schema=schema)
        orders = orders.withColumn('date', col('date').cast('date'))
        return orders

    def process_orders(self, orders):
        if orders.count() == 0:
            print("No orders to process.")
            return

        delivered_orders = orders.filter(col('status') == self.Status.DELIVERED)
        total_revenue_delivered = delivered_orders.agg(_sum('amount')).collect()[0][0]
        cancelled_count = orders.filter(col('status') == self.Status.CANCELLED).count()

        revenue_by_customer = delivered_orders.groupBy('customer').agg(_sum('amount').alias('revenue'))
        top_customers = revenue_by_customer.orderBy(col('revenue').desc()).limit(3).collect()

        monthly_revenue = delivered_orders.groupBy(col('date').substr(1, 7).alias('month')).agg(_sum('amount').alias('revenue'))

        invalid_orders_count = orders.filter(
            (col('orderId').isNull()) | (col('customer').isNull()) | (col('status').isNull()) | (col('amount') < 0) | (col('date').isNull())
        ).count()

        print("=== Order Summary ===")
        print(f"Total orders: {orders.count()}")
        print(f"Delivered revenue: {total_revenue_delivered:.2f}")
        print(f"Cancelled count: {cancelled_count}")
        print(f"Invalid orders (flagged by parser): {invalid_orders_count}")

        print("\n=== Revenue by Customer (DELIVERED) ===")
        for row in revenue_by_customer.collect():
            print(f" {row['customer']} -> {row['revenue']:.2f}")

        print("\n=== Top Customers ===")
        for i, row in enumerate(top_customers):
            print(f" {i + 1}) {row['customer']}: {row['revenue']:.2f}")

        print("\n=== Monthly Revenue (DELIVERED) ===")
        for row in monthly_revenue.collect():
            print(f" {row['month']} -> {row['revenue']:.2f}")

if __name__ == "__main__":
    spark = SparkSession.builder.appName("OrderProcessorApp").getOrCreate()
    processor = OrderProcessor(spark)

    csv_path = "path/to/orders.csv"  # Replace with actual path
    orders = processor.read_orders_from_csv(csv_path)
    processor.process_orders(orders)
