import unittest
from pyspark.sql import SparkSession
from UserMetricsJob import load_events, load_users, transform

class TestUserMetricsJob(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.spark = SparkSession.builder.master("local[*]").appName("TestUserMetricsJob").getOrCreate()

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    def test_transform(self):
        events_data = [("u1", "click", 85, 100.0, "2023-10-01T12:00:00"),
                       ("u2", "purchase", 45, 200.0, "2023-10-02T12:00:00")]
        events_schema = ["user_id", "event_type", "score", "amount", "ts"]
        events_df = self.spark.createDataFrame(events_data, events_schema)

        users_data = [("u1", "US"),
                      ("u2", "IN")]
        users_schema = ["user_id", "country"]
        users_df = self.spark.createDataFrame(users_data, users_schema)

        result = transform(events_df, users_df, "2023-10-01", "2023-10-03", False)
        result.show()

        self.assertEqual(result.count(), 2)
        self.assertEqual(result.filter(result.user_id == "u1").select("country").collect()[0][0], "US")

if __name__ == "__main__":
    unittest.main()