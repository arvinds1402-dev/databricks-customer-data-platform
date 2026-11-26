# Databricks notebook source

# Step 1: Read Silver tables

df_silver_customers = spark.table(
    "customer_data_platform.silver.customers"
)

df_silver_transactions = spark.table(
    "customer_data_platform.silver.transactions"
)

print("Silver customers:", df_silver_customers.count())
print("Silver transactions:", df_silver_transactions.count())


# COMMAND ----------


# Step 2: Create Customer Dimension

df_dim_customer = (
    df_silver_customers
    .select(
        "customer_id",
        "customer_name",
        "city",
        "email",
        "email_missing_flag"
    )
)

print("Customer dimension count:", df_dim_customer.count())

display(df_dim_customer)


# COMMAND ----------


# Step 3: Save Customer Dimension

df_dim_customer.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable(
        "customer_data_platform.gold.dim_customer"
    )

print("Gold Customer Dimension created successfully!")


# COMMAND ----------


# Step 5.2: Join Transactions with Customer Dimension

df_fact_transactions = (
    df_silver_transactions
    .join(
        df_dim_customer,
        on="customer_id",
        how="inner"
    )
    .select(
        "transaction_id",
        "customer_id",
        "amount",
        "status"
    )
)

print(
    "Fact Transaction count:",
    df_fact_transactions.count()
)

display(df_fact_transactions)


# COMMAND ----------


# Step 6: Save Fact Transactions

df_fact_transactions.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable(
        "customer_data_platform.gold.fact_transactions"
    )

print("Gold Fact Transaction table created successfully!")


# COMMAND ----------


# Step 9.1: Generate Customer Surrogate Keys

from pyspark.sql import functions as F

df_dim_customer_sk = (
    df_dim_customer
    .withColumn(
        "customer_sk",
        F.xxhash64(F.col("customer_id"))
    )
    .select(
        "customer_sk",
        "customer_id",
        "customer_name",
        "city",
        "email",
        "email_missing_flag"
    )
)

display(df_dim_customer_sk)


# COMMAND ----------

# DBTITLE 1,Save Customer Dimension with Surrogate Key

# Step 9.2: Save Customer Dimension with Surrogate Key

df_dim_customer_sk.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(
        "customer_data_platform.gold.dim_customer"
    )

print("Customer dimension updated with surrogate keys!")


# COMMAND ----------


# Step 10: Add Customer Surrogate Key to Fact

df_fact_transactions_sk = (
    df_silver_transactions
    .join(
        df_dim_customer_sk.select(
            "customer_id",
            "customer_sk"
        ),
        on="customer_id",
        how="inner"
    )
    .select(
        "transaction_id",
        "customer_sk",
        "customer_id",
        "amount",
        "status"
    )
)

display(df_fact_transactions_sk)


# COMMAND ----------


# Save updated Gold Fact Table

df_fact_transactions_sk.write \
    .format("delta") \
    .mode("overwrite") \
        .option("overwriteSchema", "true") \
    .saveAsTable(
        "customer_data_platform.gold.fact_transactions"
    )

print("Fact table updated with customer surrogate keys!")


# COMMAND ----------


# Step 11: Aggregate Customer Transaction Metrics

df_customer_metrics = (
    df_fact_transactions_sk
    .groupBy("customer_sk", "customer_id")
    .agg(
        F.count("*").alias("total_transactions"),

        F.sum("amount").alias(
            "total_transaction_amount"
        ),

        F.sum(
            F.when(F.col("status") == "SUCCESS", 1)
             .otherwise(0)
        ).alias("successful_transactions"),

        F.sum(
            F.when(
                F.col("status") == "SUCCESS",
                F.col("amount")
            ).otherwise(0)
        ).alias("successful_transaction_amount"),

        F.sum(
            F.when(F.col("status") == "FAILED", 1)
             .otherwise(0)
        ).alias("failed_transactions")
    )
)

display(df_customer_metrics)


# COMMAND ----------


# Step 12: Build Customer 360

df_customer_360 = (
    df_dim_customer_sk
    .join(
        df_customer_metrics,
        on=["customer_sk", "customer_id"],
        how="left"
    )
    .fillna({
        "total_transactions": 0,
        "total_transaction_amount": 0,
        "successful_transactions": 0,
        "successful_transaction_amount": 0,
        "failed_transactions": 0
    })
)

display(df_customer_360)


# COMMAND ----------


# Step 13: Save Customer 360 Delta Table

df_customer_360.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable(
        "customer_data_platform.gold.customer_360"
    )

print("Customer 360 table created successfully!")
