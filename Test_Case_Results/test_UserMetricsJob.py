import unittest
from pyspark.sql import SparkSession
from UserMetricsJob import load_events, load_users, transform

class TestUserMetricsJob(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.spark = SparkSession.builder \
            .appName("UserMetricsJobTest") \
            .config("spark.sql.adaptive.enabled", "true") \
            .getOrCreate()

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    def test_transform(self):
        events_data = [("user1", "click", 90, 15.0, "2023-01-01T12:00:00"),
                       ("user2", "purchase", 45, 30.0, "2023-01-02T15:00:00")]
        events_schema = "user_id STRING, event_type STRING, score INT, amount DOUBLE, ts TIMESTAMP"
        events = self.spark.createDataFrame(events_data, schema=events_schema)

        users_data = [("user1", "US"), ("user2", "UK")]
        users_schema = "user_id STRING, country STRING"
        users = self.spark.createDataFrame(users_data, schema=users_schema)

        result = transform(events, users, "2023-01-01", "2023-12-31", False)

        expected_data = [("US", "user1", 15.0, 1, "high", 1),
                         ("UK", "user2", 30.0, 1, "low", 1)]
        expected_schema = "country STRING, user_id STRING, revenue DOUBLE, event_count INT, score_bucket STRING, country_rank INT"
        expected = self.spark.createDataFrame(expected_data, schema=expected_schema)

        self.assertEqual(result.collect(), expected.collect())

if __name__ == "__main__":
    unittest.main()