from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, current_date
from pyspark.sql.types import StructType, StructField, StringType, IntegerType


def create_spark_session(app_name: str = "EmployeeData") -> SparkSession:
    """
    Creates and returns a Spark session.
    """
    return SparkSession.builder.appName(app_name).getOrCreate()


def create_employee_dataframe(spark: SparkSession):
    """
    Creates and returns the employee DataFrame with a custom schema.
    """
    employee_schema = StructType([
        StructField("employee_id", IntegerType(), True),
        StructField("employee_name", StringType(), True),
        StructField("department", StringType(), True),
        StructField("state", StringType(), True),
        StructField("salary", IntegerType(), True),
        StructField("age", IntegerType(), True)
    ])

    employee_data = [
        (11, "james", "D101", "ny", 9000, 34),
        (12, "michel", "D101", "ny", 8900, 32),
        (13, "robert", "D102", "ca", 7900, 29),
        (14, "scott", "D103", "ca", 8000, 36),
        (15, "jen", "D102", "ny", 9500, 38),
        (16, "jeff", "D103", "uk", 9100, 35),
        (17, "maria", "D101", "ny", 7900, 40)
    ]

    return spark.createDataFrame(employee_data, schema=employee_schema)


def create_department_dataframe(spark: SparkSession):
    """
    Creates and returns the department DataFrame with a custom schema.
    """
    department_schema = StructType([
        StructField("dept_id", StringType(), True),
        StructField("dept_name", StringType(), True)
    ])

    department_data = [
        ("D101", "sales"),
        ("D102", "finance"),
        ("D103", "marketing"),
        ("D104", "hr"),
        ("D105", "support")
    ]

    return spark.createDataFrame(department_data, schema=department_schema)


def create_country_dataframe(spark: SparkSession):
    """
    Creates and returns the country DataFrame with a custom schema.
    """
    country_schema = StructType([
        StructField("country_code", StringType(), True),
        StructField("country_name", StringType(), True)
    ])

    country_data = [
        ("ny", "newyork"),
        ("ca", "California"),
        ("uk", "Russia")
    ]

    return spark.createDataFrame(country_data, schema=country_schema)


def calculate_average_salary(employee_df):
    """
    Calculates the average salary for each department.
    """
    return employee_df.groupBy("department").agg(avg("salary").alias("avg_salary"))


def filter_employees_starting_with_m(employee_df, department_df):
    """
    Finds employees whose names start with 'm' and returns their names and department.
    """
    return employee_df.join(department_df, employee_df.department == department_df.dept_id) \
        .filter(col("employee_name").startswith("m")) \
        .select("employee_name", "dept_name")


def add_bonus_column(employee_df):
    """
    Adds a bonus column to the DataFrame by multiplying the salary by 2.
    """
    return employee_df.withColumn("bonus", col("salary") * 2)


def reorder_columns(employee_df):
    """
    Reorders the columns in the employee DataFrame.
    """
    return employee_df.select("employee_id", "employee_name", "salary", "state", "age", "department")


def perform_joins(employee_df, department_df, join_type: str):
    """
    Performs a join operation (inner, left, or right) between employee and department DataFrames.
    """
    return employee_df.join(department_df, employee_df.department == department_df.dept_id, join_type)


def map_country_name(employee_df, country_df):
    """
    Maps the country name instead of the state code in the employee DataFrame.
    """
    return employee_df.join(country_df, employee_df.state == country_df.country_code, "left") \
        .drop("state") \
        .withColumnRenamed("country_name", "state")


def convert_column_names_to_lowercase(df):
    """
    Converts all column names to lowercase and adds a 'load_date' column.
    """
    return df.select([col(c).alias(c.lower()) for c in df.columns]).withColumn("load_date", current_date())


def save_as_table(df, db_name: str, table_name: str, format_type: str, path: str = None):
    """
    Saves the DataFrame as a managed table in the specified database.
    """
    # Ensure the database exists
    spark = df.sparkSession
    spark.sql(f"CREATE DATABASE IF NOT EXISTS {db_name}")

    # Save the DataFrame
    writer = df.write.mode("overwrite").format(format_type)
    if path:
        writer.option("path", path)

    writer.saveAsTable(f"{db_name}.{table_name}")
