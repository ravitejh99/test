import pyspark.sql.functions as F
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, DateType
from datetime import datetime

class OrderProcessor:

    def __init__(self):
        self.spark = SparkSession.builder.appName("OrderProcessor").getOrCreate()

    def process_orders(self, orders_df):
        orders_df = orders_df.withColumn("is_valid", F.expr(
            "orderId IS NOT NULL AND customer IS NOT NULL AND status IS NOT NULL AND amount >= 0 AND date IS NOT NULL"
        ))

        total_revenue_delivered = orders_df.filter(orders_df.status == "DELIVERED").groupBy().sum("amount").collect()[0][0]
        cancelled_count = orders_df.filter(orders_df.status == "CANCELLED").count()

        revenue_by_customer = orders_df.filter(orders_df.status == "DELIVERED") \
            .groupBy("customer").sum("amount").withColumnRenamed("sum(amount)", "total_revenue")

        top_customers = revenue_by_customer.orderBy(F.desc("total_revenue")).limit(3).collect()

        monthly_revenue = orders_df.filter(orders_df.status == "DELIVERED") \
            .withColumn("month", F.date_format("date", "yyyy-MM")) \
            .groupBy("month").sum("amount").withColumnRenamed("sum(amount)", "monthly_revenue")

        invalid_orders_count = orders_df.filter(~orders_df.is_valid).count()

        print("=== Order Summary ===")
        print(f"Total orders: {orders_df.count()}")
        print(f"Delivered revenue: {total_revenue_delivered}")
        print(f"Cancelled count: {cancelled_count}")
        print(f"Invalid orders (flagged by parser): {invalid_orders_count}")

        print("\n=== Revenue by Customer (DELIVERED) ===")
        revenue_by_customer.show()

        print("\n=== Top Customers ===")
        for idx, row in enumerate(top_customers):
            print(f"{idx + 1}) {row['customer']}: {row['total_revenue']}")

        print("\n=== Monthly Revenue (DELIVERED) ===")
        monthly_revenue.show()

if __name__ == "__main__":
    schema = StructType([
        StructField("orderId", StringType(), True),
        StructField("customer", StringType(), True),
        StructField("status", StringType(), True),
        StructField("amount", DoubleType(), True),
        StructField("date", DateType(), True)
    ])

    sample_data = [
        ("1001", "Alice", "DELIVERED", 120.50, datetime.strptime("2024-05-01", "%Y-%m-%d")),
        ("1002", "Bob", "CANCELLED", 75.00, datetime.strptime("2024-05-02", "%Y-%m-%d")),
        ("1003", "Alice", "DELIVERED", 49.99, datetime.strptime("2024-06-05", "%Y-%m-%d")),
        ("1004", "Charlie", "NEW", 39.90, datetime.strptime("2024-07-10", "%Y-%m-%d")),
        ("1005", "Bob", "DELIVERED", 275.00, datetime.strptime("2024-07-12", "%Y-%m-%d")),
        ("1006", "Dana", "DELIVERED", 15.00, datetime.strptime("2024-07-15", "%Y-%m-%d"))
    ]

    spark = SparkSession.builder.appName("OrderProcessorTest").getOrCreate()
    orders_df = spark.createDataFrame(sample_data, schema=schema)

    processor = OrderProcessor()
    processor.process_orders(orders_df)