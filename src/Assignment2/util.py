
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType

def create_spark_session(app_name: str = "CreditCardMasking") -> SparkSession:
    """
    Creates a Spark session.
    """
    return SparkSession.builder.appName(app_name).getOrCreate()

def create_credit_card_df(spark: SparkSession, data: list) -> 'DataFrame':
    """
    Creates a DataFrame from the given list of tuples.
    """
    return spark.createDataFrame(data, ["card_number"])

def get_num_partitions(df: 'DataFrame') -> int:
    """
    Returns the number of partitions in the DataFrame.
    """
    return df.rdd.getNumPartitions()

def repartition_df(df: 'DataFrame', num_partitions: int) -> 'DataFrame':
    """
    Repartitions the DataFrame to the specified number of partitions.
    """
    return df.repartition(num_partitions)

def coalesce_df(df: 'DataFrame', num_partitions: int) -> 'DataFrame':
    """
    Coalesces the DataFrame to the specified number of partitions.
    """
    return df.coalesce(num_partitions)

def mask_credit_card(card_number: str) -> str:
    """
    Masks all but the last 4 digits of the credit card number.
    """
    return '*' * (len(card_number) - 4) + card_number[-4:]

def add_masked_column(df: 'DataFrame') -> 'DataFrame':
    """
    Adds a masked credit card column to the DataFrame.
    """
    mask_udf = udf(mask_credit_card, StringType())
    return df.withColumn("masked_card_number", mask_udf(df.card_number))


def spark():
    return None