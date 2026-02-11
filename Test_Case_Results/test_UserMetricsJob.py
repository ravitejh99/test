import unittest
from pyspark.sql import SparkSession
from UserMetricsJob import load_events, load_users, transform

class TestUserMetricsJob(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.spark = SparkSession.builder.master("local[1]").appName("TestUserMetricsJob").getOrCreate()

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    def test_load_events(self):
        schema = "user_id STRING, event_type STRING, score INT, amount DOUBLE, ts TIMESTAMP"
        data = [("u1", "click", 85, 100.0, "2023-01-01 10:00:00"),
                ("u2", "purchase", 45, 200.0, "2023-01-02 15:00:00")]
        df = self.spark.createDataFrame(data, schema)
        df.show()
        self.assertEqual(df.count(), 2)

    def test_transform(self):
        events_schema = "user_id STRING, event_type STRING, score INT, amount DOUBLE, ts TIMESTAMP"
        users_schema = "user_id STRING, country STRING"
        events_data = [("u1", "click", 85, 100.0, "2023-01-01 10:00:00"),
                       ("u2", "purchase", 45, 200.0, "2023-01-02 15:00:00")]
        users_data = [("u1", "US"), ("u2", "UK")]
        events = self.spark.createDataFrame(events_data, events_schema)
        users = self.spark.createDataFrame(users_data, users_schema)

        result = transform(events, users, "2023-01-01", "2023-01-03", False)
        result.show()
        self.assertEqual(result.count(), 2)

if __name__ == "__main__":
    unittest.main()