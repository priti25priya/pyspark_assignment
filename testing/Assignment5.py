import unittest
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType
from pyspark.sql.functions import col, avg, current_date


class TestSparkEmployeeData(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        # Initialize Spark session for the test case
        cls.spark = SparkSession.builder \
            .appName("TestSparkEmployeeData") \
            .getOrCreate()

        # Sample employee data for testing
        cls.employee_data = [
            (11, "james", "D101", "ny", 9000, 34),
            (12, "michel", "D101", "ny", 8900, 32),
            (13, "robert", "D102", "ca", 7900, 29),
            (14, "scott", "D103", "ca", 8000, 36),
            (15, "jen", "D102", "ny", 9500, 38),
            (16, "jeff", "D103", "uk", 9100, 35),
            (17, "maria", "D101", "ny", 7900, 40)
        ]

        cls.department_data = [
            ("D101", "sales"),
            ("D102", "finance"),
            ("D103", "marketing"),
            ("D104", "hr"),
            ("D105", "support")
        ]

        cls.country_data = [
            ("ny", "newyork"),
            ("ca", "California"),
            ("uk", "Russia")
        ]

        # Create DataFrames
        employee_schema = StructType([
            StructField("employee_id", IntegerType(), True),
            StructField("employee_name", StringType(), True),
            StructField("department", StringType(), True),
            StructField("state", StringType(), True),
            StructField("salary", IntegerType(), True),
            StructField("age", IntegerType(), True)
        ])

        department_schema = StructType([
            StructField("dept_id", StringType(), True),
            StructField("dept_name", StringType(), True)
        ])

        country_schema = StructType([
            StructField("country_code", StringType(), True),
            StructField("country_name", StringType(), True)
        ])

        cls.employee_df = cls.spark.createDataFrame(cls.employee_data, schema=employee_schema)
        cls.department_df = cls.spark.createDataFrame(cls.department_data, schema=department_schema)
        cls.country_df = cls.spark.createDataFrame(cls.country_data, schema=country_schema)

    @classmethod
    def tearDownClass(cls):
        # Stop the Spark session after all tests
        cls.spark.stop()

    def test_avg_salary_by_department(self):
        avg_salary_df = self.employee_df.groupBy("department").agg(avg("salary").alias("avg_salary"))
        avg_salary_df.show()
        self.assertEqual(avg_salary_df.count(), 3, "There should be 3 departments in the average salary DataFrame.")
        self.assertTrue(avg_salary_df.filter(col("department") == "D101").count() > 0, "Department D101 should exist.")

    def test_employee_name_starting_with_m(self):
        employee_m_df = self.employee_df.join(self.department_df,
                                              self.employee_df.department == self.department_df.dept_id) \
            .filter(col("employee_name").startswith("m")) \
            .select("employee_name", "dept_name")
        employee_m_df.show()
        self.assertEqual(employee_m_df.count(), 2, "There should be 2 employees whose names start with 'm'.")

    def test_bonus_column(self):
        employee_with_bonus_df = self.employee_df.withColumn("bonus", col("salary") * 2)
        employee_with_bonus_df.show()
        self.assertIn("bonus", employee_with_bonus_df.columns, "Bonus column should be added.")
        self.assertEqual(employee_with_bonus_df.filter(col("employee_id") == 11).select("bonus").first()[0], 18000,
                         "Bonus for employee 11 should be 18000.")

    def test_reorder_columns(self):
        reordered_df = self.employee_df.select("employee_id", "employee_name", "salary", "state", "age", "department")
        reordered_df.show()
        expected_columns = ["employee_id", "employee_name", "salary", "state", "age", "department"]
        self.assertEqual(reordered_df.columns, expected_columns, "Columns should be reordered correctly.")

    def test_joins(self):
        inner_join_df = self.employee_df.join(self.department_df,
                                              self.employee_df.department == self.department_df.dept_id, "inner")
        left_join_df = self.employee_df.join(self.department_df,
                                             self.employee_df.department == self.department_df.dept_id, "left")
        right_join_df = self.employee_df.join(self.department_df,
                                              self.employee_df.department == self.department_df.dept_id, "right")

        self.assertGreater(inner_join_df.count(), 0, "Inner join should return at least one record.")
        self.assertGreater(left_join_df.count(), 0, "Left join should return at least one record.")
        self.assertGreater(right_join_df.count(), 0, "Right join should return at least one record.")

    def test_employee_with_country(self):
        employee_with_country_df = self.employee_df.join(self.country_df,
                                                         self.employee_df.state == self.country_df.country_code, "left") \
            .drop("state") \
            .withColumnRenamed("country_name", "state")
        employee_with_country_df.show()
        self.assertIn("state", employee_with_country_df.columns,
                      "Employee DataFrame should contain the country name as state.")

    def test_lowercase_columns_and_load_date(self):
        employee_with_country_df = self.employee_df.join(self.country_df,
                                                         self.employee_df.state == self.country_df.country_code, "left") \
            .drop("state") \
            .withColumnRenamed("country_name", "state")

        employee_with_lowercase_df = employee_with_country_df.select(
            [col(c).alias(c.lower()) for c in employee_with_country_df.columns]
        ).withColumn("load_date", current_date())

        employee_with_lowercase_df.show()
        self.assertIn("load_date", employee_with_lowercase_df.columns, "Load date column should be added.")
        self.assertTrue(all(col.isLower() for col in employee_with_lowercase_df.columns),
                        "All columns should be in lowercase.")


# Manually run the test suite
def run_tests():
    suite = unittest.TestSuite()
    suite.addTest(TestSparkEmployeeData("test_avg_salary_by_department"))
    suite.addTest(TestSparkEmployeeData("test_employee_name_starting_with_m"))
    suite.addTest(TestSparkEmployeeData("test_bonus_column"))
    suite.addTest(TestSparkEmployeeData("test_reorder_columns"))
    suite.addTest(TestSparkEmployeeData("test_joins"))
    suite.addTest(TestSparkEmployeeData("test_employee_with_country"))
    suite.addTest(TestSparkEmployeeData("test_lowercase_columns_and_load_date"))

    # Run the tests
    runner = unittest.TextTestRunner(verbosity=2)
    runner.run(suite)


# Call the function to execute the tests
run_tests()
