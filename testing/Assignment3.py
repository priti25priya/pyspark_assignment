import unittest
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.connect.functions import datediff, current_date
from pyspark.sql.functions import to_date, col
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, TimestampType
from pyspark_assignment.src.Assignment3.util import rename_columns


class TestUserActivity(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        # Initialize Spark session for the test case
        cls.spark = SparkSession.builder.appName("TestUserActivity").getOrCreate()

        # Sample data for testing
        cls.data = [
            (1, 101, 'login', datetime.strptime('2023-09-05 08:30:00', '%Y-%m-%d %H:%M:%S')),
            (2, 102, 'click', datetime.strptime('2023-09-06 12:45:00', '%Y-%m-%d %H:%M:%S')),
            (3, 101, 'click', datetime.strptime('2023-09-07 14:15:00', '%Y-%m-%d %H:%M:%S')),
            (4, 103, 'login', datetime.strptime('2023-09-08 09:00:00', '%Y-%m-%d %H:%M:%S')),
            (5, 102, 'logout', datetime.strptime('2023-09-09 17:30:00', '%Y-%m-%d %H:%M:%S')),
            (6, 101, 'click', datetime.strptime('2023-09-10 11:20:00', '%Y-%m-%d %H:%M:%S')),
            (7, 103, 'click', datetime.strptime('2023-09-11 10:15:00', '%Y-%m-%d %H:%M:%S')),
            (8, 102, 'click', datetime.strptime('2023-09-12 13:10:00', '%Y-%m-%d %H:%M:%S'))
        ]

        cls.schema = StructType([
            StructField("log_id", IntegerType(), True),
            StructField("user_id", IntegerType(), True),
            StructField("user_activity", StringType(), True),
            StructField("time_stamp", TimestampType(), True)
        ])

        cls.user_activity_df = cls.spark.createDataFrame(cls.data, cls.schema)

    @classmethod
    def tearDownClass(cls):
        # Stop the Spark session after all tests
        cls.spark.stop()

    def test_rename_columns(self):
        # Test renaming columns
        renamed_df = rename_columns(self.user_activity_df)
        expected_columns = ["log_id", "user_id", "user_activity", "time_stamp"]
        self.assertEqual(renamed_df.columns, expected_columns)

    def test_filter_actions_in_last_7_days(self):
        # Test filtering actions performed in the last 7 days
        actions_count_df = self.user_activity_df.filter(
            datediff(current_date(), col("time_stamp")) <= 7
        ).groupBy("user_id").count().withColumnRenamed("count", "actions_performed")

        # Check the DataFrame is not empty and contains the expected columns
        self.assertGreater(actions_count_df.count(), 0)
        self.assertTrue("actions_performed" in actions_count_df.columns)

    def test_login_date_format(self):
        # Test converting time_stamp to login_date format
        user_activity_with_date_df = self.user_activity_df.withColumn(
            "login_date", to_date(col("time_stamp"))
        ).drop("time_stamp")

        # Check if the login_date column is in the correct format
        expected_date_format = '%Y-%m-%d'
        actual_date_format = user_activity_with_date_df.select("login_date").first()["login_date"].strftime(expected_date_format)
        self.assertEqual(expected_date_format, actual_date_format)

# Manually run the test suite in a Databricks notebook or Jupyter notebook
def run_tests():
    # Create a test suite and add tests
    suite = unittest.TestSuite()
    suite.addTest(TestUserActivity("test_rename_columns"))
    suite.addTest(TestUserActivity("test_filter_actions_in_last_7_days"))
    suite.addTest(TestUserActivity("test_login_date_format"))

    # Run the tests
    runner = unittest.TextTestRunner(verbosity=2)
    runner.run(suite)

# Call the function to execute the tests
run_tests()
