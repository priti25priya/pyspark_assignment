# util.py

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, TimestampType
from pyspark.sql.functions import col, to_date, current_date, datediff
from datetime import datetime

def create_spark_session(app_name: str = "UserActivity") -> SparkSession:
    """
    Creates and returns a Spark session.
    """
    return SparkSession.builder.appName(app_name).getOrCreate()

def get_user_activity_data() -> list:
    """
    Returns the user activity data with datetime conversion.
    """
    return [
        (1, 101, 'login', datetime.strptime('2023-09-05 08:30:00', '%Y-%m-%d %H:%M:%S')),
        (2, 102, 'click', datetime.strptime('2023-09-06 12:45:00', '%Y-%m-%d %H:%M:%S')),
        (3, 101, 'click', datetime.strptime('2023-09-07 14:15:00', '%Y-%m-%d %H:%M:%S')),
        (4, 103, 'login', datetime.strptime('2023-09-08 09:00:00', '%Y-%m-%d %H:%M:%S')),
        (5, 102, 'logout', datetime.strptime('2023-09-09 17:30:00', '%Y-%m-%d %H:%M:%S')),
        (6, 101, 'click', datetime.strptime('2023-09-10 11:20:00', '%Y-%m-%d %H:%M:%S')),
        (7, 103, 'click', datetime.strptime('2023-09-11 10:15:00', '%Y-%m-%d %H:%M:%S')),
        (8, 102, 'click', datetime.strptime('2023-09-12 13:10:00', '%Y-%m-%d %H:%M:%S'))
    ]

def get_user_activity_schema() -> StructType:
    """
    Returns the schema for the user activity DataFrame.
    """
    return StructType([
        StructField("log_id", IntegerType(), True),
        StructField("user_id", IntegerType(), True),
        StructField("user_activity", StringType(), True),
        StructField("time_stamp", TimestampType(), True)
    ])

def rename_columns(df):
    """
    Renames columns in the DataFrame to the specified new names.
    """
    new_columns = ["log_id", "user_id", "user_activity", "time_stamp"]
    for old_name, new_name in zip(df.columns, new_columns):
        df = df.withColumnRenamed(old_name, new_name)
    return df

def calculate_actions_last_7_days(df):
    """
    Calculates the number of actions performed by each user in the last 7 days.
    """
    return df.filter(
        datediff(current_date(), col("time_stamp")) <= 7
    ).groupBy("user_id").count().withColumnRenamed("count", "actions_performed")

def convert_timestamp_to_date(df):
    """
    Converts the time_stamp column to the login_date column in YYYY-MM-DD format.
    """
    return df.withColumn("login_date", to_date(col("time_stamp"))).drop("time_stamp")

def write_dataframe_as_csv(df, path: str):
    """
    Writes the DataFrame to a CSV file.
    """
    df.write.csv(path, mode="overwrite", header=True)

def write_dataframe_as_managed_table(df, db_name: str, table_name: str):
    """
    Writes the DataFrame as a managed table in the specified database.
    """
    # Ensure the database exists
    spark = df.sparkSession
    spark.sql(f"CREATE DATABASE IF NOT EXISTS {db_name}")
    df.write.saveAsTable(f"{db_name}.{table_name}", mode="overwrite")
