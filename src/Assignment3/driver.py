
from util import (
    create_spark_session,
    get_user_activity_data,
    get_user_activity_schema,
    rename_columns,
    calculate_actions_last_7_days,
    convert_timestamp_to_date,
    write_dataframe_as_csv,
    write_dataframe_as_managed_table
)

# Step 1: Create a Spark session
spark = create_spark_session()

# Step 2: Define the data and schema
data = get_user_activity_data()
schema = get_user_activity_schema()

# Step 3: Create DataFrame with the schema
user_activity_df = spark.createDataFrame(data, schema)

# Show the initial DataFrame
user_activity_df.show(truncate=False)

# Step 4: Rename columns dynamically
user_activity_df = rename_columns(user_activity_df)

# Step 5: Calculate the number of actions performed by each user in the last 7 days
actions_count_df = calculate_actions_last_7_days(user_activity_df)
actions_count_df.show()

# Step 6: Convert the time_stamp column to the login_date column in YYYY-MM-DD format
user_activity_df = convert_timestamp_to_date(user_activity_df)

# Show the updated DataFrame
user_activity_df.show()

# Step 7: Write the DataFrame as a CSV file
write_dataframe_as_csv(user_activity_df, "user_activity.csv")

# Step 8: Write the DataFrame as a managed table
write_dataframe_as_managed_table(user_activity_df, "user", "login_details")
