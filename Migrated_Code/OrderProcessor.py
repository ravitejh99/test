import csv
from datetime import datetime
from collections import defaultdict
from pyspark.sql import SparkSession

class OrderProcessor:
    class Status:
        NEW = "NEW"
        PROCESSING = "PROCESSING"
        DELIVERED = "DELIVERED"
        CANCELLED = "CANCELLED"

    def __init__(self, spark_session):
        self.spark = spark_session

    def read_orders_from_csv(self, file_path):
        orders = []
        with open(file_path, 'r') as file:
            csv_reader = csv.reader(file)
            next(csv_reader)  # Skip header
            for row in csv_reader:
                if len(row) < 5:
                    print(f"WARN: Skipping malformed line: {row}")
                    continue
                try:
                    order_id, customer, status, amount, date = row
                    amount = float(amount)
                    date = datetime.strptime(date, '%Y-%m-%d').date()
                    orders.append({
                        'order_id': order_id,
                        'customer': customer,
                        'status': status.upper(),
                        'amount': amount,
                        'date': date
                    })
                except Exception as e:
                    print(f"WARN: Skipping malformed line: {row} ({e})")
        return orders

    def process_orders(self, orders):
        if not orders:
            print("No orders to process.")
            return

        delivered_orders = [o for o in orders if o['status'] == self.Status.DELIVERED]
        cancelled_orders = [o for o in orders if o['status'] == self.Status.CANCELLED]
        
        total_revenue_delivered = sum(o['amount'] for o in delivered_orders)
        cancelled_count = len(cancelled_orders)

        revenue_by_customer = defaultdict(float)
        for o in delivered_orders:
            revenue_by_customer[o['customer']] += o['amount']

        top_customers = sorted(revenue_by_customer.items(), key=lambda x: x[1], reverse=True)[:3]

        monthly_revenue = defaultdict(float)
        for o in delivered_orders:
            key = f"{o['date'].year}-{o['date'].month:02d}"
            monthly_revenue[key] += o['amount']

        print("=== Order Summary ===")
        print(f"Total orders: {len(orders)}")
        print(f"Delivered revenue: {total_revenue_delivered:.2f}")
        print(f"Cancelled count: {cancelled_count}")
        print()

        print("=== Revenue by Customer (DELIVERED) ===")
        for customer, revenue in sorted(revenue_by_customer.items(), key=lambda x: x[1], reverse=True):
            print(f" {customer} -> {revenue:.2f}")
        print()

        print("=== Top Customers ===")
        for i, (customer, revenue) in enumerate(top_customers, 1):
            print(f" {i}) {customer}: {revenue:.2f}")
        print()

        print("=== Monthly Revenue (DELIVERED) ===")
        for month, revenue in sorted(monthly_revenue.items()):
            print(f" {month} -> {revenue:.2f}")
        print()

if __name__ == "__main__":
    spark = SparkSession.builder.appName("OrderProcessor").getOrCreate()
    processor = OrderProcessor(spark)

    file_path = "path/to/orders.csv"  # Replace with the actual file path
    orders = processor.read_orders_from_csv(file_path)
    processor.process_orders(orders)
