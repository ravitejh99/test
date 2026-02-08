import unittest
from pyspark.sql import SparkSession
from UserMetricsJob import UserMetricsJob

class TestUserMetricsJob(unittest.TestCase):
    def setUp(self):
        self.spark = SparkSession.builder \
            .appName("TestUserMetricsJob") \
            .master("local[2]") \
            .getOrCreate()
        self.job = UserMetricsJob()

    def tearDown(self):
        self.spark.stop()

    def test_transform(self):
        events_data = [("user1", "click", 85, 100.0, "2023-10-01T12:00:00"),
                       ("user2", "purchase", 75, 200.0, "2023-10-02T12:00:00")]
        events_schema = ["user_id", "event_type", "score", "amount", "ts"]
        events_df = self.spark.createDataFrame(events_data, events_schema)

        users_data = [("user1", "US"), ("user2", "UK")]
        users_schema = ["user_id", "country"]
        users_df = self.spark.createDataFrame(users_data, users_schema)

        transformed = self.job.transform(events_df, users_df, "2023-10-01", "2023-10-03", False)

        self.assertEqual(transformed.count(), 2)
        self.assertEqual(transformed.filter(transformed.score_bucket == "high").count(), 1)
        self.assertEqual(transformed.filter(transformed.score_bucket == "low").count(), 1)

if __name__ == "__main__":
    unittest.main()