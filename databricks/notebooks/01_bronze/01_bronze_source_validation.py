# Databricks notebook source

# Step 1: Verify Spark environment

print("Spark Version:", spark.version)
print("Current Catalog:", spark.sql("SELECT current_catalog()").collect()[0][0])
print("Current Schema:", spark.sql("SELECT current_schema()").collect()[0][0])


# COMMAND ----------


# Step 2: Set project catalog and Bronze schema

spark.sql("USE CATALOG customer_data_platform")
spark.sql("USE SCHEMA bronze")

print("Catalog:", spark.sql("SELECT current_catalog()").collect()[0][0])
print("Schema:", spark.sql("SELECT current_schema()").collect()[0][0])


# COMMAND ----------


# Step 3: Verify S3 source connectivity

source_path = "s3://python-etl-learning-2026-arvind-unique123/raw/"

try:
    files = dbutils.fs.ls(source_path)

    print("S3 connection successful!")
    print("Files available in raw folder:")

    for file in files:
        print(file.name, file.size, "bytes")

except Exception as e:
    print("S3 connection failed:")
    print(str(e))


# COMMAND ----------


# Step 4: Read source CSV files using PySpark

customers_path = "s3://python-etl-learning-2026-arvind-unique123/raw/customers.csv"

transactions_path = "s3://python-etl-learning-2026-arvind-unique123/raw/transactions.csv"

# Read customers
df_customers = (
    spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv(customers_path)
)

# Read transactions
df_transactions = (
    spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv(transactions_path)
)

print("Customers count:", df_customers.count())
print("Transactions count:", df_transactions.count())

print("Customers schema:")
df_customers.printSchema()

print("Transactions schema:")
df_transactions.printSchema()


# COMMAND ----------


# Step 5: Display source data

print("CUSTOMERS DATA")
display(df_customers)

print("TRANSACTIONS DATA")
display(df_transactions)


# COMMAND ----------


from pyspark.sql import functions as F

def validate_source(df, table_name):

    print(f"\n========== {table_name} ==========")

    # Total records
    total_records = df.count()
    print("Total Records:", total_records)

    # Column names
    print("Columns:", df.columns)

    # Data types
    print("\nSchema:")
    df.printSchema()

    # NULL validation
    print("\nNULL Count:")
    null_counts = df.select([
        F.count(
            F.when(F.col(c).isNull(), c)
        ).alias(c)
        for c in df.columns
    ])

    display(null_counts)

    # Duplicate validation
    duplicate_count = total_records - df.dropDuplicates().count()
    print("Duplicate Records:", duplicate_count)

    # Display records
    display(df)


# Validate both source datasets
validate_source(df_customers, "CUSTOMERS")
validate_source(df_transactions, "TRANSACTIONS")


# COMMAND ----------

from pyspark.sql import functions as F

# Find duplicate customer IDs

duplicate_customers = (
    df_customers
    .groupBy("customer_id")
    .count()
    .filter(F.col("count") > 1)
)

display(duplicate_customers)

# COMMAND ----------

duplicate_transactions = (
    df_transactions
    .groupBy("transaction_id")
    .count()
    .filter(F.col("count") > 1)
)

display(duplicate_transactions)

# COMMAND ----------

negative_transactions = (
    df_transactions
    .filter(F.col("amount") < 0)
)

display(negative_transactions)

# COMMAND ----------

orphan_transactions = (
    df_transactions
    .join(
        df_customers.select("customer_id"),
        on="customer_id",
        how="left_anti"
    )
)

display(orphan_transactions)

# COMMAND ----------


# Step 10: Prepare Bronze DataFrames

from pyspark.sql import functions as F
import uuid

# Generate one batch ID for this pipeline execution
batch_id = str(uuid.uuid4())

print("Current Batch ID:", batch_id)

# Add metadata to customers
df_customers_bronze = (
    df_customers
    .withColumn("ingestion_timestamp", F.current_timestamp())
    .withColumn("source_file_name", F.lit("customers.csv"))
    .withColumn("batch_id", F.lit(batch_id))
)

# Add metadata to transactions
df_transactions_bronze = (
    df_transactions
    .withColumn("ingestion_timestamp", F.current_timestamp())
    .withColumn("source_file_name", F.lit("transactions.csv"))
    .withColumn("batch_id", F.lit(batch_id))
)

print("Customers Bronze records:", df_customers_bronze.count())
print("Transactions Bronze records:", df_transactions_bronze.count())

display(df_customers_bronze)
display(df_transactions_bronze)


# COMMAND ----------


# Step 11.1: Write customers to Bronze Delta table

df_customers_bronze.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("customer_data_platform.bronze.customers")

print("Customers Bronze table created successfully!")


# COMMAND ----------


# Step 11.2: Write transactions to Bronze Delta table

df_transactions_bronze.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("customer_data_platform.bronze.transactions")

print("Transactions Bronze table created successfully!")


# COMMAND ----------



# COMMAND ----------

# Read customers CSV uploaded through Python Boto3
customers_df = (
    spark.read
    .option("header", True)
    .option("inferSchema", True)
    .csv("s3://python-etl-learning-2026-arvind-unique123/raw/python_upload_test/customers.csv")
)

display(customers_df)

# COMMAND ----------

# Read transactions CSV uploaded through Python Boto3
transactions_df = (
    spark.read
    .option("header", True)
    .option("inferSchema", True)
    .csv("s3://python-etl-learning-2026-arvind-unique123/raw/python_upload_test/transactions.csv")
)

display(transactions_df)

# COMMAND ----------

print("Customer records:", customers_df.count())
print("Transaction records:", transactions_df.count())

transactions_df.printSchema()

# COMMAND ----------

# Define Bronze catalog and schema
catalog_name = "customer_data_platform"
schema_name = "bronze"

# Write customers to Bronze Delta table
(
    customers_df.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(f"{catalog_name}.{schema_name}.customers")
)

# Write transactions to Bronze Delta table
(
    transactions_df.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(f"{catalog_name}.{schema_name}.transactions")
)

print("Bronze ingestion completed successfully!")

# COMMAND ----------



# COMMAND ----------

from pyspark.sql.functions import (
    current_timestamp,
    current_date,
    col,
    regexp_extract
)
customers_bronze_df = (
    customers_df
    .withColumn("ingestion_timestamp", current_timestamp())
    .withColumn("ingestion_date", current_date())
    .withColumn("source_file_path", col("_metadata.file_path"))
    .withColumn(
        "source_file_name",
        regexp_extract(col("_metadata.file_path"), r"([^/]+)$", 1)
    )
)

display(customers_bronze_df)

# COMMAND ----------

transactions_bronze_df = (
    transactions_df
    .withColumn("ingestion_timestamp", current_timestamp())
    .withColumn("ingestion_date", current_date())
    .withColumn("source_file_path", col("_metadata.file_path"))
    .withColumn(
        "source_file_name",
        regexp_extract(col("_metadata.file_path"), r"([^/]+)$", 1)
    )
)

display(transactions_bronze_df)

# COMMAND ----------

customers_bronze_df.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("customer_data_platform.bronze.customers")

transactions_bronze_df.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("customer_data_platform.bronze.transactions")

# COMMAND ----------

display(
    customers_bronze_df.select(
        "customer_id",
        "customer_name",
        "ingestion_timestamp",
        "ingestion_date",
        "source_file_path",
        "source_file_name"
    )
)

# COMMAND ----------

display(
    customers_bronze_df.select(
        "customer_id",
        "customer_name",
        "ingestion_timestamp",
        "ingestion_date",
        "source_file_path",
        "source_file_name"
    )
)

# COMMAND ----------


from pyspark.sql.functions import (
    current_timestamp,
    current_date,
    col,
    regexp_extract
)

def ingest_to_bronze(source_path, target_table):

    # Read CSV from S3
    source_df = (
        spark.read
        .option("header", True)
        .option("inferSchema", True)
        .csv(source_path)
    )

    # Add audit columns
    bronze_df = (
        source_df
        .withColumn("ingestion_timestamp", current_timestamp())
        .withColumn("ingestion_date", current_date())
        .withColumn("source_file_path", col("_metadata.file_path"))
        .withColumn(
            "source_file_name",
            regexp_extract(col("_metadata.file_path"), r"([^/]+)$", 1)
        )
    )

    # Write to Bronze Delta table
    (
        bronze_df.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(target_table)
    )

    print(f"Successfully ingested: {target_table}")
    print(f"Record count: {bronze_df.count()}")



# COMMAND ----------

ingest_to_bronze(
    "s3://python-etl-learning-2026-arvind-unique123/raw/python_upload_test/customers.csv",
    "customer_data_platform.bronze.customers"
)

ingest_to_bronze(
    "s3://python-etl-learning-2026-arvind-unique123/raw/python_upload_test/transactions.csv",
    "customer_data_platform.bronze.transactions"
)

# COMMAND ----------

# Create a sample Day 2 transaction
new_transactions = [
    (5010, 102, 2500.0, "SUCCESS"),
    (5011, 104, 1800.0, "PENDING")
]

columns = [
    "transaction_id",
    "customer_id",
    "amount",
    "status"
]

day2_df = spark.createDataFrame(new_transactions, columns)

display(day2_df)

# COMMAND ----------

from pyspark.sql.functions import (
    current_timestamp,
    current_date,
    lit
)

day2_bronze_df = (
    day2_df
    .withColumn("ingestion_timestamp", current_timestamp())
    .withColumn("ingestion_date", current_date())
    .withColumn(
        "source_file_path",
        lit("s3://python-etl-learning-2026-arvind-unique123/raw/day2/transactions.csv")
    )
    .withColumn("source_file_name", lit("transactions.csv"))
)

display(day2_bronze_df)

# COMMAND ----------

from delta.tables import DeltaTable

target_table = "customer_data_platform.bronze.transactions"

delta_table = DeltaTable.forName(spark, target_table)

(
    delta_table.alias("target")
    .merge(
        day2_bronze_df.alias("source"),
        "target.transaction_id = source.transaction_id"
    )
    .whenNotMatchedInsertAll()
    .execute()
)

print("Incremental Bronze ingestion completed!")

# COMMAND ----------

from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    TimestampType,
    LongType
)

audit_schema = StructType([
    StructField("batch_id", StringType(), False),
    StructField("source_file_name", StringType(), True),
    StructField("target_table", StringType(), True),
    StructField("start_timestamp", TimestampType(), True),
    StructField("end_timestamp", TimestampType(), True),
    StructField("records_read", LongType(), True),
    StructField("records_inserted", LongType(), True),
    StructField("status", StringType(), True),
    StructField("error_message", StringType(), True)
])

empty_audit_df = spark.createDataFrame([], audit_schema)

(
    empty_audit_df.write
    .format("delta")
    .mode("ignore")
    .saveAsTable(
        "customer_data_platform.bronze.ingestion_audit_log"
    )
)

print("Bronze ingestion audit table created successfully!")

# COMMAND ----------

from datetime import datetime
from pyspark.sql import Row

AUDIT_TABLE = "customer_data_platform.bronze.ingestion_audit_log"


def log_ingestion(
    batch_id,
    source_file_name,
    target_table,
    start_timestamp,
    end_timestamp,
    records_read,
    records_inserted,
    status,
    error_message=None
):

    audit_record = [
        Row(
            batch_id=batch_id,
            source_file_name=source_file_name,
            target_table=target_table,
            start_timestamp=start_timestamp,
            end_timestamp=end_timestamp,
            records_read=records_read,
            records_inserted=records_inserted,
            status=status,
            error_message=error_message
        )
    ]

    audit_df = spark.createDataFrame(audit_record, audit_schema)

    audit_df.write \
        .format("delta") \
        .mode("append") \
        .saveAsTable(AUDIT_TABLE)

    print(f"Audit record inserted: {batch_id}")

# COMMAND ----------

from uuid import uuid4
from datetime import datetime

test_batch_id = str(uuid4())

test_start_time = datetime.now()

test_end_time = datetime.now()

log_ingestion(
    batch_id=test_batch_id,
    source_file_name="customers.csv",
    target_table="customer_data_platform.bronze.customers",
    start_timestamp=test_start_time,
    end_timestamp=test_end_time,
    records_read=7,
    records_inserted=7,
    status="SUCCESS",
    error_message=None
)

# COMMAND ----------


from datetime import datetime
from uuid import uuid4
from pyspark.sql.functions import (
    current_timestamp,
    current_date,
    col,
    regexp_extract
)

def ingest_to_bronze_with_audit(source_path, target_table):

    batch_id = str(uuid4())
    start_timestamp = datetime.now()

    source_file_name = source_path.split("/")[-1]
    records_read = 0
    records_inserted = 0

    try:
        # Read source file
        source_df = (
            spark.read
            .option("header", True)
            .option("inferSchema", True)
            .csv(source_path)
        )

        records_read = source_df.count()

        # Add audit columns
        bronze_df = (
            source_df
            .withColumn("ingestion_timestamp", current_timestamp())
            .withColumn("ingestion_date", current_date())
            .withColumn("source_file_path", col("_metadata.file_path"))
            .withColumn(
                "source_file_name",
                regexp_extract(col("_metadata.file_path"), r"([^/]+)$", 1)
            )
        )

        # Load Bronze table
        (
            bronze_df.write
            .format("delta")
            .mode("overwrite")
            .option("overwriteSchema", "true")
            .saveAsTable(target_table)
        )

        records_inserted = records_read
        end_timestamp = datetime.now()

        # Write successful audit record
        log_ingestion(
            batch_id,
            source_file_name,
            target_table,
            start_timestamp,
            end_timestamp,
            records_read,
            records_inserted,
            "SUCCESS",
            None
        )

        print(f"Batch ID: {batch_id}")
        print(f"Successfully loaded: {target_table}")
        print(f"Records read: {records_read}")
        print(f"Records inserted: {records_inserted}")

    except Exception as error:

        end_timestamp = datetime.now()

        # Write failed audit record
        log_ingestion(
            batch_id,
            source_file_name,
            target_table,
            start_timestamp,
            end_timestamp,
            records_read,
            records_inserted,
            "FAILED",
            str(error)[:4000]
        )

        print(f"Batch failed: {batch_id}")
        raise


# COMMAND ----------

ingest_to_bronze_with_audit(
    "s3://python-etl-learning-2026-arvind-unique123/raw/python_upload_test/customers.csv",
    "customer_data_platform.bronze.customers"
)

# COMMAND ----------

try:
    ingest_to_bronze_with_audit(
        "s3://python-etl-learning-2026-arvind-unique123/raw/invalid_file.csv",
        "customer_data_platform.bronze.test_invalid"
    )
except Exception as error:
    print("Expected test failure captured.")