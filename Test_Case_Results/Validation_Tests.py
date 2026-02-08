import unittest
from pyspark.sql import SparkSession
from UserMetricsJob import UserMetricsJob

class TestUserMetricsJob(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.spark = SparkSession.builder \n            .appName('TestUserMetricsJob') \n            .config('spark.sql.adaptive.enabled', 'true') \n            .getOrCreate()
        cls.job = UserMetricsJob()

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    def test_load_events(self):
        events_path = 'sample_data/events.csv'
        events = self.job.load_events(self.spark, events_path)
        self.assertIsNotNone(events)
        self.assertGreater(events.count(), 0)

    def test_load_users(self):
        users_path = 'sample_data/users.csv'
        users = self.job.load_users(self.spark, users_path)
        self.assertIsNotNone(users)
        self.assertGreater(users.count(), 0)

    def test_transform(self):
        events_path = 'sample_data/events.csv'
        users_path = 'sample_data/users.csv'
        events = self.job.load_events(self.spark, events_path)
        users = self.job.load_users(self.spark, users_path)

        transformed = self.job.transform(events, users, '1970-01-01', '2100-01-01', False)
        self.assertIsNotNone(transformed)
        self.assertGreater(transformed.count(), 0)

if __name__ == '__main__':
    unittest.main()