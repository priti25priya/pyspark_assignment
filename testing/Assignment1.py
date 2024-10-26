import unittest
from pyspark.sql.connect.session import SparkSession
from pyspark.sql.types import StructType, StructField, StringType
from pyspark_assignment.src.Assignment1.util import only_product_iphone13, iphone13_to_iphone14, product_unique


# Define the test case class
class TestSparkFunctions(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        # Set up a Spark session for testing
        cls.spark = SparkSession.builder.appName("unit test").getOrCreate()

        # Sample data setup
        cls.data1 = [("1", "iphone13"),
                     ("1", "dell i5 core"),
                     ("2", "iphone13"),
                     ("2", "dell i5 core"),
                     ("3", "iphone13"),
                     ("3", "dell i5 core"),
                     ("1", "dell i3 core"),
                     ("1", "hp i5 core"),
                     ("1", "iphone14"),
                     ("3", "iphone14"),
                     ("4", "iphone13")]

        cls.schema1 = StructType([
            StructField("customer", StringType(), nullable=False),
            StructField("product_model", StringType(), nullable=False)
        ])

        cls.df1 = cls.spark.createDataFrame(cls.data1, schema=cls.schema1)

        cls.data2 = [("iphone13",),
                     ("dell i5 core",),
                     ("dell i3 core",),
                     ("hp i5 core",),
                     ("iphone14",)]

        cls.schema2 = StructType([
            StructField("product_model", StringType(), nullable=False)
        ])

        cls.df2 = cls.spark.createDataFrame(cls.data2, schema=cls.schema2)

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    def test_only_product_iphone13(self):
        # Test only_product_iphone13 function
        result_df = only_product_iphone13(self.df1)
        result = [row['customer'] for row in result_df.collect()]
        expected_result = ["4"]  # Customer 4 only purchased "iphone13"
        self.assertEqual(result, expected_result)

    def test_iphone13_to_iphone14(self):
        # Test iphone13_to_iphone14 function
        result_df = iphone13_to_iphone14(self.df1)
        result = [row['customer'] for row in result_df.collect()]
        expected_result = ["3"]  # Customer 3 purchased both "iphone13" and "iphone14"
        self.assertEqual(result, expected_result)

    def test_product_unique(self):
        # Test product_unique function
        result_df = product_unique(self.df1, self.df2)
        result = [row['customer'] for row in result_df.collect()]
        expected_result = ["1"]  # Customer 1 purchased all distinct products
        self.assertEqual(result, expected_result)

# Manually run the tests using unittest
if __name__ == "__main__":
    test_suite = unittest.TestSuite()
    test_suite.addTest(TestSparkFunctions("test_only_product_iphone13"))
    test_suite.addTest(TestSparkFunctions("test_iphone13_to_iphone14"))
    test_suite.addTest(TestSparkFunctions("test_product_unique"))

    runner = unittest.TextTestRunner()
    runner.run(test_suite)
