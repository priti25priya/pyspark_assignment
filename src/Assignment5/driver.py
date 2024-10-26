
from util import (
    create_spark_session,
    create_employee_dataframe,
    create_department_dataframe,
    create_country_dataframe,
    calculate_average_salary,
    filter_employees_starting_with_m,
    add_bonus_column,
    reorder_columns,
    perform_joins,
    map_country_name,
    convert_column_names_to_lowercase,
    save_as_table
)

# Step 1: Create a Spark session
spark = create_spark_session()

# Step 2: Create DataFrames
employee_df = create_employee_dataframe(spark)
department_df = create_department_dataframe(spark)
country_df = create_country_dataframe(spark)

# Show all DataFrames
employee_df.show()
department_df.show()
country_df.show()

# Step 3: Find the average salary of each department
avg_salary_df = calculate_average_salary(employee_df)
avg_salary_df.show()

# Step 4: Find the employee’s name and department name whose name starts with 'm'
employee_m_df = filter_employees_starting_with_m(employee_df, department_df)
employee_m_df.show()

# Step 5: Add a bonus column by multiplying the employee's salary by 2
employee_df = add_bonus_column(employee_df)
employee_df.show()

# Step 6: Reorder the columns of employee_df
employee_df = reorder_columns(employee_df)
employee_df.show()

# Step 7: Perform inner, left, and right joins with department_df
inner_join_df = perform_joins(employee_df, department_df, "inner")
left_join_df = perform_joins(employee_df, department_df, "left")
right_join_df = perform_joins(employee_df, department_df, "right")

inner_join_df.show()
left_join_df.show()
right_join_df.show()

# Step 8: Map the country name instead of the state
employee_with_country_df = map_country_name(employee_df, country_df)
employee_with_country_df.show()

# Step 9: Convert all column names to lowercase and add a load_date column
employee_with_country_df = convert_column_names_to_lowercase(employee_with_country_df)
employee_with_country_df.show()

# Step 10: Save the DataFrame as external tables in parquet and CSV format
save_as_table(employee_with_country_df, "employee_db", "employee_parquet", "parquet")
save_as_table(employee_with_country_df, "employee_db", "employee_csv", "csv", "dbfs:/FileStore/tables/employee_data.json")
