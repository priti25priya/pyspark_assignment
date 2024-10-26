import unittest
from pyspark.sql import SparkSession
from pyspark_assignment.src.Assignment2.util import mask_credit_card, spark, credit_card_df, mask_udf
class TestCreditCardMasking(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        # Initialize Spark session for the test case
        cls.spark = SparkSession.builder.appName("TestCreditCardMasking").getOrCreate()

        # Sample data for testing
        cls.data = [("1234567891234567",),
                    ("5678912345671234",),
                    ("9123456712345678",),
                    ("1234567812341122",),
                    ("1234567812341342",)]

        cls.credit_card_df = cls.spark.createDataFrame(cls.data, ["card_number"])

    @classmethod
    def tearDownClass(cls):
        # Stop the Spark session after all tests
        cls.spark.stop()

    def test_mask_credit_card_function(self):
        # Test the mask_credit_card function directly
        self.assertEqual(mask_credit_card("1234567891234567"), "************4567")
        self.assertEqual(mask_credit_card("5678912345671234"), "************1234")
        self.assertEqual(mask_credit_card("9123456712345678"), "************5678")
        self.assertEqual(mask_credit_card("1234"), "1234")  # Edge case: Less than 4 digits

    def test_partitioning(self):
        # Test initial partition count
        initial_partitions = self.credit_card_df.rdd.getNumPartitions()
        self.assertEqual(initial_partitions, credit_card_df.rdd.getNumPartitions())

        # Test increasing the partition size
        repartitioned_df = self.credit_card_df.repartition(5)
        self.assertEqual(repartitioned_df.rdd.getNumPartitions(), 5)

        # Test decreasing the partition size
        coalesced_df = repartitioned_df.coalesce(1)
        self.assertEqual(coalesced_df.rdd.getNumPartitions(), 1)

    def test_mask_udf(self):
        # Test the UDF on the DataFrame
        masked_df = self.credit_card_df.withColumn("masked_card_number", mask_udf(self.credit_card_df.card_number))

        # Collect the results
        results = masked_df.select("masked_card_number").collect()
        expected_results = ["************4567", "************1234", "************5678", "************1122", "************1342"]

        # Verify if the results match the expected masked values
        actual_results = [row["masked_card_number"] for row in results]
        self.assertEqual(actual_results, expected_results)

# Manually run the test suite in a Databricks notebook or Jupyter notebook
def run_tests():
    # Create a test suite and add tests
    suite = unittest.TestSuite()
    suite.addTest(TestCreditCardMasking("test_mask_credit_card_function"))
    suite.addTest(TestCreditCardMasking("test_partitioning"))
    suite.addTest(TestCreditCardMasking("test_mask_udf"))

    # Run the tests
    runner = unittest.TextTestRunner(verbosity=2)
    runner.run(suite)

# Call the function to execute the tests
run_tests()
