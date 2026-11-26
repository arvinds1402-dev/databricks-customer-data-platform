# Databricks notebook source

# Step 1: Read Bronze Delta tables

df_customers_bronze = spark.table(
    "customer_data_platform.bronze.customers"
)

df_transactions_bronze = spark.table(
    "customer_data_platform.bronze.transactions"
)

print("Customers Bronze count:", df_customers_bronze.count())
print("Transactions Bronze count:", df_transactions_bronze.count())


# COMMAND ----------


# Step 2: Identify duplicate customer IDs using Window

from pyspark.sql.window import Window
from pyspark.sql import functions as F

customer_window = Window.partitionBy("customer_id")

df_customer_duplicates = (
    df_customers_bronze
    .withColumn(
        "duplicate_count",
        F.count("*").over(customer_window)
    )
    .filter(F.col("duplicate_count") > 1)
)

display(df_customer_duplicates)


# COMMAND ----------


# Step 3: Identify customers with missing emails

df_customers_missing_email = (
    df_customers_bronze
    .filter(F.col("email").isNull())
)

display(df_customers_missing_email)


# COMMAND ----------


# Step 4: Identify valid and duplicate customers

from pyspark.sql import functions as F
from pyspark.sql.window import Window

# Create a window based on customer_id
customer_window = Window.partitionBy("customer_id")

# Calculate the number of records per customer
df_customer_checked = (
    df_customers_bronze
    .withColumn(
        "customer_id_count",
        F.count("*").over(customer_window)
    )
)

# Keep customers having unique customer IDs
df_silver_customers = (
    df_customer_checked
    .filter(F.col("customer_id_count") == 1)
    .drop("customer_id_count")
)

# Move duplicate customer IDs to exception DataFrame
df_customer_exceptions = (
    df_customer_checked
    .filter(F.col("customer_id_count") > 1)
)

# Flag customers with missing emails
df_silver_customers = (
    df_silver_customers
    .withColumn(
        "email_missing_flag",
        F.when(F.col("email").isNull(), "Y")
         .otherwise("N")
    )
)

print("Silver customer count:", df_silver_customers.count())
print("Duplicate customer exceptions:", df_customer_exceptions.count())

display(df_silver_customers)
display(df_customer_exceptions)


# COMMAND ----------


# Step 5.1: Save Silver customers

df_silver_customers.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable(
        "customer_data_platform.silver.customers"
    )

print("Silver customers table created successfully!")


# COMMAND ----------


# Step 5.2: Prepare duplicate customer exceptions

df_customer_exceptions_final = (
    df_customer_exceptions
    .withColumn(
        "exception_reason",
        F.lit("DUPLICATE_CUSTOMER_ID")
    )
    .drop("customer_id_count")
)

# Save exception records
df_customer_exceptions_final.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable(
        "customer_data_platform.silver.customer_exceptions"
    )

print("Customer exception table created successfully!")


# COMMAND ----------



# COMMAND ----------


# Step 6: Read Bronze transactions

df_transactions_bronze = spark.table(
    "customer_data_platform.bronze.transactions"
)

df_silver_customers = spark.table(
    "customer_data_platform.silver.customers"
)

print(
    "Bronze transactions:",
    df_transactions_bronze.count()
)

print(
    "Silver customers:",
    df_silver_customers.count()
)


# COMMAND ----------


# Step 7: Identify duplicate transaction IDs

from pyspark.sql import functions as F
from pyspark.sql.window import Window

transaction_window = Window.partitionBy("transaction_id")

df_transactions_checked = (
    df_transactions_bronze
    .withColumn(
        "transaction_id_count",
        F.count("*").over(transaction_window)
    )
)

# Transactions with duplicate IDs
df_duplicate_transactions = (
    df_transactions_checked
    .filter(F.col("transaction_id_count") > 1)
)

# Transactions with unique IDs
df_unique_transactions = (
    df_transactions_checked
    .filter(F.col("transaction_id_count") == 1)
    .drop("transaction_id_count")
)

print("Duplicate transaction records:",
      df_duplicate_transactions.count())

print("Unique transaction records:",
      df_unique_transactions.count())

display(df_duplicate_transactions)


# COMMAND ----------


# Step 8: Identify negative transaction amounts

df_negative_transactions = (
    df_unique_transactions
    .filter(F.col("amount") < 0)
    .withColumn(
        "exception_reason",
        F.lit("NEGATIVE_TRANSACTION_AMOUNT")
    )
)

# Keep non-negative transactions
df_non_negative_transactions = (
    df_unique_transactions
    .filter(F.col("amount") >= 0)
)

print("Negative transactions:",
      df_negative_transactions.count())

print("Non-negative transactions:",
      df_non_negative_transactions.count())

display(df_negative_transactions)


# COMMAND ----------


# Step 9: Identify transactions with unknown customers

df_orphan_transactions = (
    df_non_negative_transactions
    .join(
        df_silver_customers.select("customer_id"),
        on="customer_id",
        how="left_anti"
    )
    .withColumn(
        "exception_reason",
        F.lit("CUSTOMER_NOT_FOUND")
    )
)

# Keep transactions with valid customers
df_valid_transactions = (
    df_non_negative_transactions
    .join(
        df_silver_customers.select("customer_id"),
        on="customer_id",
        how="inner"
    )
)

print("Unknown customer transactions:",
      df_orphan_transactions.count())

print("Valid transactions:",
      df_valid_transactions.count())

display(df_orphan_transactions)
display(df_valid_transactions)


# COMMAND ----------


# Get customer IDs that have duplicate records

df_ambiguous_customer_ids = (
    df_customer_exceptions
    .select("customer_id")
    .distinct()
)

display(df_ambiguous_customer_ids)


# COMMAND ----------


# Identify transactions linked to ambiguous customer records

df_ambiguous_transactions = (
    df_non_negative_transactions
    .join(
        df_ambiguous_customer_ids,
        on="customer_id",
        how="inner"
    )
    .withColumn(
        "exception_reason",
        F.lit("AMBIGUOUS_CUSTOMER_REFERENCE")
    )
)

display(df_ambiguous_transactions)


# COMMAND ----------


# Find transactions whose customer IDs do not exist in Bronze

df_all_customer_ids = (
    df_customers_bronze
    .select("customer_id")
    .distinct()
)

df_unknown_transactions = (
    df_non_negative_transactions
    .join(
        df_all_customer_ids,
        on="customer_id",
        how="left_anti"
    )
    .withColumn(
        "exception_reason",
        F.lit("CUSTOMER_NOT_FOUND")
    )
)

display(df_unknown_transactions)


# COMMAND ----------


# Remove ambiguous customer references

df_transactions_without_ambiguous = (
    df_non_negative_transactions
    .join(
        df_ambiguous_customer_ids,
        on="customer_id",
        how="left_anti"
    )
)

# Keep only transactions with valid customer references

df_valid_transactions = (
    df_transactions_without_ambiguous
    .join(
        df_all_customer_ids,
        on="customer_id",
        how="inner"
    )
)

print("Valid transactions:", df_valid_transactions.count())
print("Ambiguous transactions:", df_ambiguous_transactions.count())
print("Unknown customer transactions:", df_unknown_transactions.count())

display(df_valid_transactions)


# COMMAND ----------


# Step 14.1: Prepare duplicate transaction exceptions

df_duplicate_transactions_final = (
    df_duplicate_transactions
    .withColumn(
        "exception_reason",
        F.lit("DUPLICATE_TRANSACTION_ID")
    )
    .drop("transaction_id_count")
)

display(df_duplicate_transactions_final)


# COMMAND ----------


# Step 14.2: Save Silver transaction tables

# 1. Valid transactions
df_valid_transactions.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable(
        "customer_data_platform.silver.transactions"
    )

# 2. Duplicate transaction exceptions
df_duplicate_transactions_final.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable(
        "customer_data_platform.silver.transaction_exceptions"
    )

# 3. Negative transaction exceptions
df_negative_transactions.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable(
        "customer_data_platform.silver.negative_transaction_exceptions"
    )

# 4. Ambiguous customer exceptions
df_ambiguous_transactions.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable(
        "customer_data_platform.silver.ambiguous_customer_exceptions"
    )

# 5. Unknown customer exceptions
df_unknown_transactions.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable(
        "customer_data_platform.silver.unknown_customer_exceptions"
    )

print("All Silver transaction tables created successfully!")
