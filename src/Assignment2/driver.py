
from util import (
    create_spark_session,
    create_credit_card_df,
    get_num_partitions,
    repartition_df,
    coalesce_df,
    add_masked_column
)

# Step 1: Create a Spark session
spark = create_spark_session()

# Step 2: Create DataFrame using different read methods
data = [("1234567891234567",),
        ("5678912345671234",),
        ("9123456712345678",),
        ("1234567812341122",),
        ("1234567812341342",)]

credit_card_df = create_credit_card_df(spark, data)

# Step 2: Print number of partitions
original_partitions = get_num_partitions(credit_card_df)
print(f"Original number of partitions: {original_partitions}")

# Step 3: Increase the partition size to 5
credit_card_df = repartition_df(credit_card_df, 5)
increased_partitions = get_num_partitions(credit_card_df)
print(f"Number of partitions after increasing: {increased_partitions}")

# Step 4: Decrease the partition size back to its original partition size
credit_card_df = coalesce_df(credit_card_df, 1)
decreased_partitions = get_num_partitions(credit_card_df)
print(f"Number of partitions after decreasing: {decreased_partitions}")

# Step 5: Add a masked credit card number column
masked_card_df = add_masked_column(credit_card_df)

# Step 6: Show the final output with original and masked card numbers
masked_card_df.select("card_number", "masked_card_number").display()
