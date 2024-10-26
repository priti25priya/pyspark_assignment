import unittest
from pyspark.sql import SparkSession
from pyspark.sql.functions import current_date
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, ArrayType
from pyspark_assignment.src.Assignment4.util import flatten_df, filter_employee_with_id, toSnakeCase, \
    add_load_date_with_current_date, add_year_month_day


class TestSparkFunctions(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        # Initialize Spark session for the test case
        cls.spark = SparkSession.builder.appName("TestSparkFunctions").getOrCreate()

        # Sample data for testing
        cls.sample_data = [
            {"id": 1001, "properties": {"name": "Store A", "storeSize": "Large"}, "employees": [{"empId": 1, "empName": "John Doe"}]},
            {"id": 1002, "properties": {"name": "Store B", "storeSize": "Medium"}, "employees": [{"empId": 2, "empName": "Jane Doe"}]}
        ]

        cls.sample_schema = StructType([
            StructField("id", IntegerType(), True),
            StructField("properties", StructType([
                StructField("name", StringType(), True),
                StructField("storeSize", StringType(), True)
            ]), True),
            StructField("employees", ArrayType(StructType([
                StructField("empId", IntegerType(), True),
                StructField("empName", StringType(), True)
            ])), True)
        ])

        # Create DataFrame
        cls.sample_df = cls.spark.createDataFrame(cls.sample_data, cls.sample_schema)

    @classmethod
    def tearDownClass(cls):
        # Stop the Spark session after all tests
        cls.spark.stop()

    def test_flatten_df(self):
        # Test flattening the DataFrame
        flattened_df = flatten_df(self.sample_df)
        self.assertEqual(flattened_df.count(), 2, "Flattened DataFrame should have 2 records.")
        self.assertIn("empId", flattened_df.columns, "Flattened DataFrame should contain 'empId' column.")
        self.assertIn("empName", flattened_df.columns, "Flattened DataFrame should contain 'empName' column.")

    def test_filter_employee_with_id(self):
        # Test filtering employee by ID
        filtered_df = filter_employee_with_id(flatten_df(self.sample_df), 1)
        self.assertEqual(filtered_df.count(), 1, "There should be exactly one record with empId 1.")
        self.assertEqual(filtered_df.first().empId, 1, "The empId should be 1.")

    def test_toSnakeCase(self):
        # Test converting camel case to snake case
        df = flatten_df(self.sample_df)
        snake_case_df = toSnakeCase(df)
        for column in snake_case_df.columns:
            self.assertNotIn(' ', column, "Column names should not contain spaces.")
            self.assertEqual(column, column.lower(), "Column names should be lowercase.")

    def test_add_load_date_with_current_date(self):
        # Test adding load_date with current date
        df_with_load_date = add_load_date_with_current_date(flatten_df(self.sample_df))
        self.assertIn("load_date", df_with_load_date.columns, "DataFrame should have load_date column.")
        self.assertEqual(df_with_load_date.select("load_date").first()[0].date(), current_date().cast("date").collect()[0][0], "load_date should match the current date.")

    def test_add_year_month_day(self):
        # Test adding year, month, and day columns
        df_with_load_date = add_load_date_with_current_date(flatten_df(self.sample_df))
        df_with_year_month_day = add_year_month_day(df_with_load_date)
        self.assertIn("year", df_with_year_month_day.columns, "DataFrame should have year column.")
        self.assertIn("month", df_with_year_month_day.columns, "DataFrame should have month column.")
        self.assertIn("day", df_with_year_month_day.columns, "DataFrame should have day column.")

# Manually run the test suite
def run_tests():
    # Create a test suite and add tests
    suite = unittest.TestSuite()
    suite.addTest(TestSparkFunctions("test_flatten_df"))
    suite.addTest(TestSparkFunctions("test_filter_employee_with_id"))
    suite.addTest(TestSparkFunctions("test_toSnakeCase"))
    suite.addTest(TestSparkFunctions("test_add_load_date_with_current_date"))
    suite.addTest(TestSparkFunctions("test_add_year_month_day"))

    # Run the tests
    runner = unittest.TextTestRunner(verbosity=2)
    runner.run(suite)

# Call the function to execute the tests
run_tests()
