{
 "cells": [
  {
   "cell_type": "code",
   "execution_count": 0,
   "metadata": {
    "application/vnd.databricks.v1+cell": {
     "cellMetadata": {
      "byteLimit": 2048000,
      "rowLimit": 10000
     },
     "finishTime": 1795529421491,
     "inputWidgets": {},
     "nuid": "1c586d98-cfe4-470d-bca9-805d91219f2a",
     "showTitle": false,
     "startTime": 1795529408385,
     "submitTime": 1795529407347,
     "tableResultSettingsMap": {},
     "title": ""
    }
   },
   "outputs": [
    {
     "output_type": "stream",
     "name": "stdout",
     "output_type": "stream",
     "text": [
      "Customers Bronze count: 7\nTransactions Bronze count: 10\n"
     ]
    }
   ],
   "source": [
    "\n",
    "# Step 1: Read Bronze Delta tables\n",
    "\n",
    "df_customers_bronze = spark.table(\n",
    "    \"customer_data_platform.bronze.customers\"\n",
    ")\n",
    "\n",
    "df_transactions_bronze = spark.table(\n",
    "    \"customer_data_platform.bronze.transactions\"\n",
    ")\n",
    "\n",
    "print(\"Customers Bronze count:\", df_customers_bronze.count())\n",
    "print(\"Transactions Bronze count:\", df_transactions_bronze.count())\n"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 0,
   "metadata": {
    "application/vnd.databricks.v1+cell": {
     "cellMetadata": {
      "byteLimit": 2048000,
      "rowLimit": 10000
     },
     "finishTime": 1795529602237,
     "inputWidgets": {},
     "nuid": "e4c3335a-e7a0-4596-9f1d-4c1d3c668b67",
     "showTitle": false,
     "startTime": 1795529597338,
     "submitTime": 1795529597260,
     "tableResultSettingsMap": {},
     "title": ""
    }
   },
   "outputs": [
    {
     "output_type": "display_data",
     "data": {
      "text/html": [
       "<style scoped>\n",
       "  .table-result-container {\n",
       "    max-height: 300px;\n",
       "    overflow: auto;\n",
       "  }\n",
       "  table, th, td {\n",
       "    border: 1px solid black;\n",
       "    border-collapse: collapse;\n",
       "  }\n",
       "  th, td {\n",
       "    padding: 5px;\n",
       "  }\n",
       "  th {\n",
       "    text-align: left;\n",
       "  }\n",
       "</style><div class='table-result-container'><table class='table-result'><thead style='background-color: white'><tr><th>customer_id</th><th>customer_name</th><th>city</th><th>email</th><th>ingestion_timestamp</th><th>source_file_name</th><th>batch_id</th><th>duplicate_count</th></tr></thead><tbody><tr><td>101</td><td>Arun</td><td>Chennai</td><td>arun@gmail.com</td><td>2026-11-23T14:49:44.765Z</td><td>customers.csv</td><td>c8b21336-93bc-4791-9ccc-60108710521f</td><td>2</td></tr><tr><td>101</td><td>Arun Kumar</td><td>Chennai</td><td>arunkumar@gmail.com</td><td>2026-11-23T14:49:44.765Z</td><td>customers.csv</td><td>c8b21336-93bc-4791-9ccc-60108710521f</td><td>2</td></tr></tbody></table></div>"
      ]
     },
     "metadata": {
      "application/vnd.databricks.v1+output": {
       "addedWidgets": {},
       "aggData": [],
       "aggError": "",
       "aggOverflow": false,
       "aggSchema": [],
       "aggSeriesLimitReached": false,
       "aggType": "",
       "arguments": {},
       "columnCustomDisplayInfos": {},
       "data": [
        [
         101,
         "Arun",
         "Chennai",
         "arun@gmail.com",
         "2026-11-23T14:49:44.765Z",
         "customers.csv",
         "c8b21336-93bc-4791-9ccc-60108710521f",
         2
        ],
        [
         101,
         "Arun Kumar",
         "Chennai",
         "arunkumar@gmail.com",
         "2026-11-23T14:49:44.765Z",
         "customers.csv",
         "c8b21336-93bc-4791-9ccc-60108710521f",
         2
        ]
       ],
       "datasetInfos": [],
       "dbfsResultPath": null,
       "isJsonSchema": true,
       "metadata": {},
       "overflow": false,
       "plotOptions": {
        "customPlotOptions": {},
        "displayType": "table",
        "pivotAggregation": null,
        "pivotColumns": null,
        "xColumns": null,
        "yColumns": null
       },
       "removedWidgets": [],
       "schema": [
        {
         "metadata": "{}",
         "name": "customer_id",
         "type": "\"integer\""
        },
        {
         "metadata": "{}",
         "name": "customer_name",
         "type": "\"string\""
        },
        {
         "metadata": "{}",
         "name": "city",
         "type": "\"string\""
        },
        {
         "metadata": "{}",
         "name": "email",
         "type": "\"string\""
        },
        {
         "metadata": "{}",
         "name": "ingestion_timestamp",
         "type": "\"timestamp\""
        },
        {
         "metadata": "{}",
         "name": "source_file_name",
         "type": "\"string\""
        },
        {
         "metadata": "{}",
         "name": "batch_id",
         "type": "\"string\""
        },
        {
         "metadata": "{}",
         "name": "duplicate_count",
         "type": "\"long\""
        }
       ],
       "type": "table"
      }
     },
     "output_type": "display_data"
    }
   ],
   "source": [
    "\n",
    "# Step 2: Identify duplicate customer IDs using Window\n",
    "\n",
    "from pyspark.sql.window import Window\n",
    "from pyspark.sql import functions as F\n",
    "\n",
    "customer_window = Window.partitionBy(\"customer_id\")\n",
    "\n",
    "df_customer_duplicates = (\n",
    "    df_customers_bronze\n",
    "    .withColumn(\n",
    "        \"duplicate_count\",\n",
    "        F.count(\"*\").over(customer_window)\n",
    "    )\n",
    "    .filter(F.col(\"duplicate_count\") > 1)\n",
    ")\n",
    "\n",
    "display(df_customer_duplicates)\n"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 0,
   "metadata": {
    "application/vnd.databricks.v1+cell": {
     "cellMetadata": {
      "byteLimit": 2048000,
      "rowLimit": 10000
     },
     "finishTime": 1795529644194,
     "inputWidgets": {},
     "nuid": "26fe1c13-ae81-4172-b92e-ba4f88d711d9",
     "showTitle": false,
     "startTime": 1795529642837,
     "submitTime": 1795529642782,
     "tableResultSettingsMap": {},
     "title": ""
    }
   },
   "outputs": [
    {
     "output_type": "display_data",
     "data": {
      "text/html": [
       "<style scoped>\n",
       "  .table-result-container {\n",
       "    max-height: 300px;\n",
       "    overflow: auto;\n",
       "  }\n",
       "  table, th, td {\n",
       "    border: 1px solid black;\n",
       "    border-collapse: collapse;\n",
       "  }\n",
       "  th, td {\n",
       "    padding: 5px;\n",
       "  }\n",
       "  th {\n",
       "    text-align: left;\n",
       "  }\n",
       "</style><div class='table-result-container'><table class='table-result'><thead style='background-color: white'><tr><th>customer_id</th><th>customer_name</th><th>city</th><th>email</th><th>ingestion_timestamp</th><th>source_file_name</th><th>batch_id</th></tr></thead><tbody><tr><td>103</td><td>Karthik</td><td>Mumbai</td><td>null</td><td>2026-11-23T14:49:44.765Z</td><td>customers.csv</td><td>c8b21336-93bc-4791-9ccc-60108710521f</td></tr><tr><td>106</td><td>Ravi</td><td>Coimbatore</td><td>null</td><td>2026-11-23T14:49:44.765Z</td><td>customers.csv</td><td>c8b21336-93bc-4791-9ccc-60108710521f</td></tr></tbody></table></div>"
      ]
     },
     "metadata": {
      "application/vnd.databricks.v1+output": {
       "addedWidgets": {},
       "aggData": [],
       "aggError": "",
       "aggOverflow": false,
       "aggSchema": [],
       "aggSeriesLimitReached": false,
       "aggType": "",
       "arguments": {},
       "columnCustomDisplayInfos": {},
       "data": [
        [
         103,
         "Karthik",
         "Mumbai",
         null,
         "2026-11-23T14:49:44.765Z",
         "customers.csv",
         "c8b21336-93bc-4791-9ccc-60108710521f"
        ],
        [
         106,
         "Ravi",
         "Coimbatore",
         null,
         "2026-11-23T14:49:44.765Z",
         "customers.csv",
         "c8b21336-93bc-4791-9ccc-60108710521f"
        ]
       ],
       "datasetInfos": [],
       "dbfsResultPath": null,
       "isJsonSchema": true,
       "metadata": {},
       "overflow": false,
       "plotOptions": {
        "customPlotOptions": {},
        "displayType": "table",
        "pivotAggregation": null,
        "pivotColumns": null,
        "xColumns": null,
        "yColumns": null
       },
       "removedWidgets": [],
       "schema": [
        {
         "metadata": "{}",
         "name": "customer_id",
         "type": "\"integer\""
        },
        {
         "metadata": "{}",
         "name": "customer_name",
         "type": "\"string\""
        },
        {
         "metadata": "{}",
         "name": "city",
         "type": "\"string\""
        },
        {
         "metadata": "{}",
         "name": "email",
         "type": "\"string\""
        },
        {
         "metadata": "{}",
         "name": "ingestion_timestamp",
         "type": "\"timestamp\""
        },
        {
         "metadata": "{}",
         "name": "source_file_name",
         "type": "\"string\""
        },
        {
         "metadata": "{}",
         "name": "batch_id",
         "type": "\"string\""
        }
       ],
       "type": "table"
      }
     },
     "output_type": "display_data"
    }
   ],
   "source": [
    "\n",
    "# Step 3: Identify customers with missing emails\n",
    "\n",
    "df_customers_missing_email = (\n",
    "    df_customers_bronze\n",
    "    .filter(F.col(\"email\").isNull())\n",
    ")\n",
    "\n",
    "display(df_customers_missing_email)\n"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 0,
   "metadata": {
    "application/vnd.databricks.v1+cell": {
     "cellMetadata": {
      "byteLimit": 2048000,
      "rowLimit": 10000
     },
     "finishTime": 1795530133831,
     "inputWidgets": {},
     "nuid": "205c8d0b-ee8e-4599-907f-dce466662693",
     "showTitle": false,
     "startTime": 1795530130058,
     "submitTime": 1795530129988,
     "tableResultSettingsMap": {},
     "title": ""
    }
   },
   "outputs": [
    {
     "output_type": "stream",
     "name": "stdout",
     "output_type": "stream",
     "text": [
      "Silver customer count: 5\nDuplicate customer exceptions: 2\n"
     ]
    },
    {
     "output_type": "display_data",
     "data": {
      "text/html": [
       "<style scoped>\n",
       "  .table-result-container {\n",
       "    max-height: 300px;\n",
       "    overflow: auto;\n",
       "  }\n",
       "  table, th, td {\n",
       "    border: 1px solid black;\n",
       "    border-collapse: collapse;\n",
       "  }\n",
       "  th, td {\n",
       "    padding: 5px;\n",
       "  }\n",
       "  th {\n",
       "    text-align: left;\n",
       "  }\n",
       "</style><div class='table-result-container'><table class='table-result'><thead style='background-color: white'><tr><th>customer_id</th><th>customer_name</th><th>city</th><th>email</th><th>ingestion_timestamp</th><th>source_file_name</th><th>batch_id</th><th>email_missing_flag</th></tr></thead><tbody><tr><td>102</td><td>Bala</td><td>Bangalore</td><td>bala@gmail.com</td><td>2026-11-23T14:49:44.765Z</td><td>customers.csv</td><td>c8b21336-93bc-4791-9ccc-60108710521f</td><td>N</td></tr><tr><td>103</td><td>Karthik</td><td>Mumbai</td><td>null</td><td>2026-11-23T14:49:44.765Z</td><td>customers.csv</td><td>c8b21336-93bc-4791-9ccc-60108710521f</td><td>Y</td></tr><tr><td>104</td><td>Divya</td><td>Chennai</td><td>divya@gmail.com</td><td>2026-11-23T14:49:44.765Z</td><td>customers.csv</td><td>c8b21336-93bc-4791-9ccc-60108710521f</td><td>N</td></tr><tr><td>105</td><td>Manoj</td><td>Hyderabad</td><td>manoj@gmail.com</td><td>2026-11-23T14:49:44.765Z</td><td>customers.csv</td><td>c8b21336-93bc-4791-9ccc-60108710521f</td><td>N</td></tr><tr><td>106</td><td>Ravi</td><td>Coimbatore</td><td>null</td><td>2026-11-23T14:49:44.765Z</td><td>customers.csv</td><td>c8b21336-93bc-4791-9ccc-60108710521f</td><td>Y</td></tr></tbody></table></div>"
      ]
     },
     "metadata": {
      "application/vnd.databricks.v1+output": {
       "addedWidgets": {},
       "aggData": [],
       "aggError": "",
       "aggOverflow": false,
       "aggSchema": [],
       "aggSeriesLimitReached": false,
       "aggType": "",
       "arguments": {},
       "columnCustomDisplayInfos": {},
       "data": [
        [
         102,
         "Bala",
         "Bangalore",
         "bala@gmail.com",
         "2026-11-23T14:49:44.765Z",
         "customers.csv",
         "c8b21336-93bc-4791-9ccc-60108710521f",
         "N"
        ],
        [
         103,
         "Karthik",
         "Mumbai",
         null,
         "2026-11-23T14:49:44.765Z",
         "customers.csv",
         "c8b21336-93bc-4791-9ccc-60108710521f",
         "Y"
        ],
        [
         104,
         "Divya",
         "Chennai",
         "divya@gmail.com",
         "2026-11-23T14:49:44.765Z",
         "customers.csv",
         "c8b21336-93bc-4791-9ccc-60108710521f",
         "N"
        ],
        [
         105,
         "Manoj",
         "Hyderabad",
         "manoj@gmail.com",
         "2026-11-23T14:49:44.765Z",
         "customers.csv",
         "c8b21336-93bc-4791-9ccc-60108710521f",
         "N"
        ],
        [
         106,
         "Ravi",
         "Coimbatore",
         null,
         "2026-11-23T14:49:44.765Z",
         "customers.csv",
         "c8b21336-93bc-4791-9ccc-60108710521f",
         "Y"
        ]
       ],
       "datasetInfos": [],
       "dbfsResultPath": null,
       "isJsonSchema": true,
       "metadata": {},
       "overflow": false,
       "plotOptions": {
        "customPlotOptions": {},
        "displayType": "table",
        "pivotAggregation": null,
        "pivotColumns": null,
        "xColumns": null,
        "yColumns": null
       },
       "removedWidgets": [],
       "schema": [
        {
         "metadata": "{}",
         "name": "customer_id",
         "type": "\"integer\""
        },
        {
         "metadata": "{}",
         "name": "customer_name",
         "type": "\"string\""
        },
        {
         "metadata": "{}",
         "name": "city",
         "type": "\"string\""
        },
        {
         "metadata": "{}",
         "name": "email",
         "type": "\"string\""
        },
        {
         "metadata": "{}",
         "name": "ingestion_timestamp",
         "type": "\"timestamp\""
        },
        {
         "metadata": "{}",
         "name": "source_file_name",
         "type": "\"string\""
        },
        {
         "metadata": "{}",
         "name": "batch_id",
         "type": "\"string\""
        },
        {
         "metadata": "{}",
         "name": "email_missing_flag",
         "type": "\"string\""
        }
       ],
       "type": "table"
      }
     },
     "output_type": "display_data"
    },
    {
     "output_type": "display_data",
     "data": {
      "text/html": [
       "<style scoped>\n",
       "  .table-result-container {\n",
       "    max-height: 300px;\n",
       "    overflow: auto;\n",
       "  }\n",
       "  table, th, td {\n",
       "    border: 1px solid black;\n",
       "    border-collapse: collapse;\n",
       "  }\n",
       "  th, td {\n",
       "    padding: 5px;\n",
       "  }\n",
       "  th {\n",
       "    text-align: left;\n",
       "  }\n",
       "</style><div class='table-result-container'><table class='table-result'><thead style='background-color: white'><tr><th>customer_id</th><th>customer_name</th><th>city</th><th>email</th><th>ingestion_timestamp</th><th>source_file_name</th><th>batch_id</th><th>customer_id_count</th></tr></thead><tbody><tr><td>101</td><td>Arun</td><td>Chennai</td><td>arun@gmail.com</td><td>2026-11-23T14:49:44.765Z</td><td>customers.csv</td><td>c8b21336-93bc-4791-9ccc-60108710521f</td><td>2</td></tr><tr><td>101</td><td>Arun Kumar</td><td>Chennai</td><td>arunkumar@gmail.com</td><td>2026-11-23T14:49:44.765Z</td><td>customers.csv</td><td>c8b21336-93bc-4791-9ccc-60108710521f</td><td>2</td></tr></tbody></table></div>"
      ]
     },
     "metadata": {
      "application/vnd.databricks.v1+output": {
       "addedWidgets": {},
       "aggData": [],
       "aggError": "",
       "aggOverflow": false,
       "aggSchema": [],
       "aggSeriesLimitReached": false,
       "aggType": "",
       "arguments": {},
       "columnCustomDisplayInfos": {},
       "data": [
        [
         101,
         "Arun",
         "Chennai",
         "arun@gmail.com",
         "2026-11-23T14:49:44.765Z",
         "customers.csv",
         "c8b21336-93bc-4791-9ccc-60108710521f",
         2
        ],
        [
         101,
         "Arun Kumar",
         "Chennai",
         "arunkumar@gmail.com",
         "2026-11-23T14:49:44.765Z",
         "customers.csv",
         "c8b21336-93bc-4791-9ccc-60108710521f",
         2
        ]
       ],
       "datasetInfos": [],
       "dbfsResultPath": null,
       "isJsonSchema": true,
       "metadata": {},
       "overflow": false,
       "plotOptions": {
        "customPlotOptions": {},
        "displayType": "table",
        "pivotAggregation": null,
        "pivotColumns": null,
        "xColumns": null,
        "yColumns": null
       },
       "removedWidgets": [],
       "schema": [
        {
         "metadata": "{}",
         "name": "customer_id",
         "type": "\"integer\""
        },
        {
         "metadata": "{}",
         "name": "customer_name",
         "type": "\"string\""
        },
        {
         "metadata": "{}",
         "name": "city",
         "type": "\"string\""
        },
        {
         "metadata": "{}",
         "name": "email",
         "type": "\"string\""
        },
        {
         "metadata": "{}",
         "name": "ingestion_timestamp",
         "type": "\"timestamp\""
        },
        {
         "metadata": "{}",
         "name": "source_file_name",
         "type": "\"string\""
        },
        {
         "metadata": "{}",
         "name": "batch_id",
         "type": "\"string\""
        },
        {
         "metadata": "{}",
         "name": "customer_id_count",
         "type": "\"long\""
        }
       ],
       "type": "table"
      }
     },
     "output_type": "display_data"
    }
   ],
   "source": [
    "\n",
    "# Step 4: Identify valid and duplicate customers\n",
    "\n",
    "from pyspark.sql import functions as F\n",
    "from pyspark.sql.window import Window\n",
    "\n",
    "# Create a window based on customer_id\n",
    "customer_window = Window.partitionBy(\"customer_id\")\n",
    "\n",
    "# Calculate the number of records per customer\n",
    "df_customer_checked = (\n",
    "    df_customers_bronze\n",
    "    .withColumn(\n",
    "        \"customer_id_count\",\n",
    "        F.count(\"*\").over(customer_window)\n",
    "    )\n",
    ")\n",
    "\n",
    "# Keep customers having unique customer IDs\n",
    "df_silver_customers = (\n",
    "    df_customer_checked\n",
    "    .filter(F.col(\"customer_id_count\") == 1)\n",
    "    .drop(\"customer_id_count\")\n",
    ")\n",
    "\n",
    "# Move duplicate customer IDs to exception DataFrame\n",
    "df_customer_exceptions = (\n",
    "    df_customer_checked\n",
    "    .filter(F.col(\"customer_id_count\") > 1)\n",
    ")\n",
    "\n",
    "# Flag customers with missing emails\n",
    "df_silver_customers = (\n",
    "    df_silver_customers\n",
    "    .withColumn(\n",
    "        \"email_missing_flag\",\n",
    "        F.when(F.col(\"email\").isNull(), \"Y\")\n",
    "         .otherwise(\"N\")\n",
    "    )\n",
    ")\n",
    "\n",
    "print(\"Silver customer count:\", df_silver_customers.count())\n",
    "print(\"Duplicate customer exceptions:\", df_customer_exceptions.count())\n",
    "\n",
    "display(df_silver_customers)\n",
    "display(df_customer_exceptions)\n"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 0,
   "metadata": {
    "application/vnd.databricks.v1+cell": {
     "cellMetadata": {
      "byteLimit": 2048000,
      "rowLimit": 10000
     },
     "finishTime": 1795530286488,
     "inputWidgets": {},
     "nuid": "5fb3417c-2cc6-4378-acfd-a8837236f505",
     "showTitle": false,
     "startTime": 1795530275718,
     "submitTime": 1795530275662,
     "tableResultSettingsMap": {},
     "title": ""
    }
   },
   "outputs": [
    {
     "output_type": "stream",
     "name": "stdout",
     "output_type": "stream",
     "text": [
      "Silver customers table created successfully!\n"
     ]
    }
   ],
   "source": [
    "\n",
    "# Step 5.1: Save Silver customers\n",
    "\n",
    "df_silver_customers.write \\\n",
    "    .format(\"delta\") \\\n",
    "    .mode(\"overwrite\") \\\n",
    "    .saveAsTable(\n",
    "        \"customer_data_platform.silver.customers\"\n",
    "    )\n",
    "\n",
    "print(\"Silver customers table created successfully!\")\n"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 0,
   "metadata": {
    "application/vnd.databricks.v1+cell": {
     "cellMetadata": {
      "byteLimit": 2048000,
      "rowLimit": 10000
     },
     "finishTime": 1795530334626,
     "inputWidgets": {},
     "nuid": "cb8ef5ff-fb96-414d-a58d-6d43b6196bbe",
     "showTitle": false,
     "startTime": 1795530330556,
     "submitTime": 1795530330493,
     "tableResultSettingsMap": {},
     "title": ""
    }
   },
   "outputs": [
    {
     "output_type": "stream",
     "name": "stdout",
     "output_type": "stream",
     "text": [
      "Customer exception table created successfully!\n"
     ]
    }
   ],
   "source": [
    "\n",
    "# Step 5.2: Prepare duplicate customer exceptions\n",
    "\n",
    "df_customer_exceptions_final = (\n",
    "    df_customer_exceptions\n",
    "    .withColumn(\n",
    "        \"exception_reason\",\n",
    "        F.lit(\"DUPLICATE_CUSTOMER_ID\")\n",
    "    )\n",
    "    .drop(\"customer_id_count\")\n",
    ")\n",
    "\n",
    "# Save exception records\n",
    "df_customer_exceptions_final.write \\\n",
    "    .format(\"delta\") \\\n",
    "    .mode(\"overwrite\") \\\n",
    "    .saveAsTable(\n",
    "        \"customer_data_platform.silver.customer_exceptions\"\n",
    "    )\n",
    "\n",
    "print(\"Customer exception table created successfully!\")\n"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 0,
   "metadata": {
    "application/vnd.databricks.v1+cell": {
     "cellMetadata": {},
     "inputWidgets": {},
     "nuid": "b4f05007-b238-4380-9b51-88e67b89439c",
     "showTitle": false,
     "tableResultSettingsMap": {},
     "title": ""
    }
   },
   "outputs": [],
   "source": []
  },
  {
   "cell_type": "code",
   "execution_count": 0,
   "metadata": {
    "application/vnd.databricks.v1+cell": {
     "cellMetadata": {
      "byteLimit": 2048000,
      "rowLimit": 10000
     },
     "finishTime": 1795530459945,
     "inputWidgets": {},
     "nuid": "e2f5ddd6-d22e-45b9-902b-0c5a62dd97b0",
     "showTitle": false,
     "startTime": 1795530458825,
     "submitTime": 1795530458772,
     "tableResultSettingsMap": {},
     "title": ""
    }
   },
   "outputs": [
    {
     "output_type": "stream",
     "name": "stdout",
     "output_type": "stream",
     "text": [
      "Bronze transactions: 10\nSilver customers: 5\n"
     ]
    }
   ],
   "source": [
    "\n",
    "# Step 6: Read Bronze transactions\n",
    "\n",
    "df_transactions_bronze = spark.table(\n",
    "    \"customer_data_platform.bronze.transactions\"\n",
    ")\n",
    "\n",
    "df_silver_customers = spark.table(\n",
    "    \"customer_data_platform.silver.customers\"\n",
    ")\n",
    "\n",
    "print(\n",
    "    \"Bronze transactions:\",\n",
    "    df_transactions_bronze.count()\n",
    ")\n",
    "\n",
    "print(\n",
    "    \"Silver customers:\",\n",
    "    df_silver_customers.count()\n",
    ")\n"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 0,
   "metadata": {
    "application/vnd.databricks.v1+cell": {
     "cellMetadata": {
      "byteLimit": 2048000,
      "rowLimit": 10000
     },
     "finishTime": 1795530591158,
     "inputWidgets": {},
     "nuid": "d9c1741e-8a92-4a8b-a16b-9600ff25b8e0",
     "showTitle": false,
     "startTime": 1795530588896,
     "submitTime": 1795530588841,
     "tableResultSettingsMap": {},
     "title": ""
    }
   },
   "outputs": [
    {
     "output_type": "stream",
     "name": "stdout",
     "output_type": "stream",
     "text": [
      "Duplicate transaction records: 2\nUnique transaction records: 8\n"
     ]
    },
    {
     "output_type": "display_data",
     "data": {
      "text/html": [
       "<style scoped>\n",
       "  .table-result-container {\n",
       "    max-height: 300px;\n",
       "    overflow: auto;\n",
       "  }\n",
       "  table, th, td {\n",
       "    border: 1px solid black;\n",
       "    border-collapse: collapse;\n",
       "  }\n",
       "  th, td {\n",
       "    padding: 5px;\n",
       "  }\n",
       "  th {\n",
       "    text-align: left;\n",
       "  }\n",
       "</style><div class='table-result-container'><table class='table-result'><thead style='background-color: white'><tr><th>transaction_id</th><th>customer_id</th><th>amount</th><th>status</th><th>ingestion_timestamp</th><th>source_file_name</th><th>batch_id</th><th>transaction_id_count</th></tr></thead><tbody><tr><td>5008</td><td>101</td><td>2500</td><td>SUCCESS</td><td>2026-11-23T14:50:10.670Z</td><td>transactions.csv</td><td>c8b21336-93bc-4791-9ccc-60108710521f</td><td>2</td></tr><tr><td>5008</td><td>101</td><td>2500</td><td>SUCCESS</td><td>2026-11-23T14:50:10.670Z</td><td>transactions.csv</td><td>c8b21336-93bc-4791-9ccc-60108710521f</td><td>2</td></tr></tbody></table></div>"
      ]
     },
     "metadata": {
      "application/vnd.databricks.v1+output": {
       "addedWidgets": {},
       "aggData": [],
       "aggError": "",
       "aggOverflow": false,
       "aggSchema": [],
       "aggSeriesLimitReached": false,
       "aggType": "",
       "arguments": {},
       "columnCustomDisplayInfos": {},
       "data": [
        [
         5008,
         101,
         2500,
         "SUCCESS",
         "2026-11-23T14:50:10.670Z",
         "transactions.csv",
         "c8b21336-93bc-4791-9ccc-60108710521f",
         2
        ],
        [
         5008,
         101,
         2500,
         "SUCCESS",
         "2026-11-23T14:50:10.670Z",
         "transactions.csv",
         "c8b21336-93bc-4791-9ccc-60108710521f",
         2
        ]
       ],
       "datasetInfos": [],
       "dbfsResultPath": null,
       "isJsonSchema": true,
       "metadata": {},
       "overflow": false,
       "plotOptions": {
        "customPlotOptions": {},
        "displayType": "table",
        "pivotAggregation": null,
        "pivotColumns": null,
        "xColumns": null,
        "yColumns": null
       },
       "removedWidgets": [],
       "schema": [
        {
         "metadata": "{}",
         "name": "transaction_id",
         "type": "\"integer\""
        },
        {
         "metadata": "{}",
         "name": "customer_id",
         "type": "\"integer\""
        },
        {
         "metadata": "{}",
         "name": "amount",
         "type": "\"integer\""
        },
        {
         "metadata": "{}",
         "name": "status",
         "type": "\"string\""
        },
        {
         "metadata": "{}",
         "name": "ingestion_timestamp",
         "type": "\"timestamp\""
        },
        {
         "metadata": "{}",
         "name": "source_file_name",
         "type": "\"string\""
        },
        {
         "metadata": "{}",
         "name": "batch_id",
         "type": "\"string\""
        },
        {
         "metadata": "{}",
         "name": "transaction_id_count",
         "type": "\"long\""
        }
       ],
       "type": "table"
      }
     },
     "output_type": "display_data"
    }
   ],
   "source": [
    "\n",
    "# Step 7: Identify duplicate transaction IDs\n",
    "\n",
    "from pyspark.sql import functions as F\n",
    "from pyspark.sql.window import Window\n",
    "\n",
    "transaction_window = Window.partitionBy(\"transaction_id\")\n",
    "\n",
    "df_transactions_checked = (\n",
    "    df_transactions_bronze\n",
    "    .withColumn(\n",
    "        \"transaction_id_count\",\n",
    "        F.count(\"*\").over(transaction_window)\n",
    "    )\n",
    ")\n",
    "\n",
    "# Transactions with duplicate IDs\n",
    "df_duplicate_transactions = (\n",
    "    df_transactions_checked\n",
    "    .filter(F.col(\"transaction_id_count\") > 1)\n",
    ")\n",
    "\n",
    "# Transactions with unique IDs\n",
    "df_unique_transactions = (\n",
    "    df_transactions_checked\n",
    "    .filter(F.col(\"transaction_id_count\") == 1)\n",
    "    .drop(\"transaction_id_count\")\n",
    ")\n",
    "\n",
    "print(\"Duplicate transaction records:\",\n",
    "      df_duplicate_transactions.count())\n",
    "\n",
    "print(\"Unique transaction records:\",\n",
    "      df_unique_transactions.count())\n",
    "\n",
    "display(df_duplicate_transactions)\n"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 0,
   "metadata": {
    "application/vnd.databricks.v1+cell": {
     "cellMetadata": {
      "byteLimit": 2048000,
      "rowLimit": 10000
     },
     "finishTime": 1795530670124,
     "inputWidgets": {},
     "nuid": "98302073-2468-4e2e-b276-e6bb8f1b0df8",
     "showTitle": false,
     "startTime": 1795530668503,
     "submitTime": 1795530668453,
     "tableResultSettingsMap": {},
     "title": ""
    }
   },
   "outputs": [
    {
     "output_type": "stream",
     "name": "stdout",
     "output_type": "stream",
     "text": [
      "Negative transactions: 1\nNon-negative transactions: 7\n"
     ]
    },
    {
     "output_type": "display_data",
     "data": {
      "text/html": [
       "<style scoped>\n",
       "  .table-result-container {\n",
       "    max-height: 300px;\n",
       "    overflow: auto;\n",
       "  }\n",
       "  table, th, td {\n",
       "    border: 1px solid black;\n",
       "    border-collapse: collapse;\n",
       "  }\n",
       "  th, td {\n",
       "    padding: 5px;\n",
       "  }\n",
       "  th {\n",
       "    text-align: left;\n",
       "  }\n",
       "</style><div class='table-result-container'><table class='table-result'><thead style='background-color: white'><tr><th>transaction_id</th><th>customer_id</th><th>amount</th><th>status</th><th>ingestion_timestamp</th><th>source_file_name</th><th>batch_id</th><th>exception_reason</th></tr></thead><tbody><tr><td>5007</td><td>106</td><td>-500</td><td>SUCCESS</td><td>2026-11-23T14:50:10.670Z</td><td>transactions.csv</td><td>c8b21336-93bc-4791-9ccc-60108710521f</td><td>NEGATIVE_TRANSACTION_AMOUNT</td></tr></tbody></table></div>"
      ]
     },
     "metadata": {
      "application/vnd.databricks.v1+output": {
       "addedWidgets": {},
       "aggData": [],
       "aggError": "",
       "aggOverflow": false,
       "aggSchema": [],
       "aggSeriesLimitReached": false,
       "aggType": "",
       "arguments": {},
       "columnCustomDisplayInfos": {},
       "data": [
        [
         5007,
         106,
         -500,
         "SUCCESS",
         "2026-11-23T14:50:10.670Z",
         "transactions.csv",
         "c8b21336-93bc-4791-9ccc-60108710521f",
         "NEGATIVE_TRANSACTION_AMOUNT"
        ]
       ],
       "datasetInfos": [],
       "dbfsResultPath": null,
       "isJsonSchema": true,
       "metadata": {},
       "overflow": false,
       "plotOptions": {
        "customPlotOptions": {},
        "displayType": "table",
        "pivotAggregation": null,
        "pivotColumns": null,
        "xColumns": null,
        "yColumns": null
       },
       "removedWidgets": [],
       "schema": [
        {
         "metadata": "{}",
         "name": "transaction_id",
         "type": "\"integer\""
        },
        {
         "metadata": "{}",
         "name": "customer_id",
         "type": "\"integer\""
        },
        {
         "metadata": "{}",
         "name": "amount",
         "type": "\"integer\""
        },
        {
         "metadata": "{}",
         "name": "status",
         "type": "\"string\""
        },
        {
         "metadata": "{}",
         "name": "ingestion_timestamp",
         "type": "\"timestamp\""
        },
        {
         "metadata": "{}",
         "name": "source_file_name",
         "type": "\"string\""
        },
        {
         "metadata": "{}",
         "name": "batch_id",
         "type": "\"string\""
        },
        {
         "metadata": "{}",
         "name": "exception_reason",
         "type": "\"string\""
        }
       ],
       "type": "table"
      }
     },
     "output_type": "display_data"
    }
   ],
   "source": [
    "\n",
    "# Step 8: Identify negative transaction amounts\n",
    "\n",
    "df_negative_transactions = (\n",
    "    df_unique_transactions\n",
    "    .filter(F.col(\"amount\") < 0)\n",
    "    .withColumn(\n",
    "        \"exception_reason\",\n",
    "        F.lit(\"NEGATIVE_TRANSACTION_AMOUNT\")\n",
    "    )\n",
    ")\n",
    "\n",
    "# Keep non-negative transactions\n",
    "df_non_negative_transactions = (\n",
    "    df_unique_transactions\n",
    "    .filter(F.col(\"amount\") >= 0)\n",
    ")\n",
    "\n",
    "print(\"Negative transactions:\",\n",
    "      df_negative_transactions.count())\n",
    "\n",
    "print(\"Non-negative transactions:\",\n",
    "      df_non_negative_transactions.count())\n",
    "\n",
    "display(df_negative_transactions)\n"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 0,
   "metadata": {
    "application/vnd.databricks.v1+cell": {
     "cellMetadata": {
      "byteLimit": 2048000,
      "rowLimit": 10000
     },
     "finishTime": 1795530773633,
     "inputWidgets": {},
     "nuid": "ee71ac3c-f39d-4a85-844f-6dc917ec62ce",
     "showTitle": false,
     "startTime": 1795530770130,
     "submitTime": 1795530770078,
     "tableResultSettingsMap": {},
     "title": ""
    }
   },
   "outputs": [
    {
     "output_type": "stream",
     "name": "stdout",
     "output_type": "stream",
     "text": [
      "Unknown customer transactions: 3\nValid transactions: 4\n"
     ]
    },
    {
     "output_type": "display_data",
     "data": {
      "text/html": [
       "<style scoped>\n",
       "  .table-result-container {\n",
       "    max-height: 300px;\n",
       "    overflow: auto;\n",
       "  }\n",
       "  table, th, td {\n",
       "    border: 1px solid black;\n",
       "    border-collapse: collapse;\n",
       "  }\n",
       "  th, td {\n",
       "    padding: 5px;\n",
       "  }\n",
       "  th {\n",
       "    text-align: left;\n",
       "  }\n",
       "</style><div class='table-result-container'><table class='table-result'><thead style='background-color: white'><tr><th>customer_id</th><th>transaction_id</th><th>amount</th><th>status</th><th>ingestion_timestamp</th><th>source_file_name</th><th>batch_id</th><th>exception_reason</th></tr></thead><tbody><tr><td>101</td><td>5001</td><td>2500</td><td>SUCCESS</td><td>2026-11-23T14:50:10.670Z</td><td>transactions.csv</td><td>c8b21336-93bc-4791-9ccc-60108710521f</td><td>CUSTOMER_NOT_FOUND</td></tr><tr><td>101</td><td>5004</td><td>4500</td><td>SUCCESS</td><td>2026-11-23T14:50:10.670Z</td><td>transactions.csv</td><td>c8b21336-93bc-4791-9ccc-60108710521f</td><td>CUSTOMER_NOT_FOUND</td></tr><tr><td>999</td><td>5009</td><td>1000</td><td>SUCCESS</td><td>2026-11-23T14:50:10.670Z</td><td>transactions.csv</td><td>c8b21336-93bc-4791-9ccc-60108710521f</td><td>CUSTOMER_NOT_FOUND</td></tr></tbody></table></div>"
      ]
     },
     "metadata": {
      "application/vnd.databricks.v1+output": {
       "addedWidgets": {},
       "aggData": [],
       "aggError": "",
       "aggOverflow": false,
       "aggSchema": [],
       "aggSeriesLimitReached": false,
       "aggType": "",
       "arguments": {},
       "columnCustomDisplayInfos": {},
       "data": [
        [
         101,
         5001,
         2500,
         "SUCCESS",
         "2026-11-23T14:50:10.670Z",
         "transactions.csv",
         "c8b21336-93bc-4791-9ccc-60108710521f",
         "CUSTOMER_NOT_FOUND"
        ],
        [
         101,
         5004,
         4500,
         "SUCCESS",
         "2026-11-23T14:50:10.670Z",
         "transactions.csv",
         "c8b21336-93bc-4791-9ccc-60108710521f",
         "CUSTOMER_NOT_FOUND"
        ],
        [
         999,
         5009,
         1000,
         "SUCCESS",
         "2026-11-23T14:50:10.670Z",
         "transactions.csv",
         "c8b21336-93bc-4791-9ccc-60108710521f",
         "CUSTOMER_NOT_FOUND"
        ]
       ],
       "datasetInfos": [],
       "dbfsResultPath": null,
       "isJsonSchema": true,
       "metadata": {},
       "overflow": false,
       "plotOptions": {
        "customPlotOptions": {},
        "displayType": "table",
        "pivotAggregation": null,
        "pivotColumns": null,
        "xColumns": null,
        "yColumns": null
       },
       "removedWidgets": [],
       "schema": [
        {
         "metadata": "{}",
         "name": "customer_id",
         "type": "\"integer\""
        },
        {
         "metadata": "{}",
         "name": "transaction_id",
         "type": "\"integer\""
        },
        {
         "metadata": "{}",
         "name": "amount",
         "type": "\"integer\""
        },
        {
         "metadata": "{}",
         "name": "status",
         "type": "\"string\""
        },
        {
         "metadata": "{}",
         "name": "ingestion_timestamp",
         "type": "\"timestamp\""
        },
        {
         "metadata": "{}",
         "name": "source_file_name",
         "type": "\"string\""
        },
        {
         "metadata": "{}",
         "name": "batch_id",
         "type": "\"string\""
        },
        {
         "metadata": "{}",
         "name": "exception_reason",
         "type": "\"string\""
        }
       ],
       "type": "table"
      }
     },
     "output_type": "display_data"
    },
    {
     "output_type": "display_data",
     "data": {
      "text/html": [
       "<style scoped>\n",
       "  .table-result-container {\n",
       "    max-height: 300px;\n",
       "    overflow: auto;\n",
       "  }\n",
       "  table, th, td {\n",
       "    border: 1px solid black;\n",
       "    border-collapse: collapse;\n",
       "  }\n",
       "  th, td {\n",
       "    padding: 5px;\n",
       "  }\n",
       "  th {\n",
       "    text-align: left;\n",
       "  }\n",
       "</style><div class='table-result-container'><table class='table-result'><thead style='background-color: white'><tr><th>customer_id</th><th>transaction_id</th><th>amount</th><th>status</th><th>ingestion_timestamp</th><th>source_file_name</th><th>batch_id</th></tr></thead><tbody><tr><td>102</td><td>5002</td><td>1500</td><td>SUCCESS</td><td>2026-11-23T14:50:10.670Z</td><td>transactions.csv</td><td>c8b21336-93bc-4791-9ccc-60108710521f</td></tr><tr><td>103</td><td>5003</td><td>3000</td><td>FAILED</td><td>2026-11-23T14:50:10.670Z</td><td>transactions.csv</td><td>c8b21336-93bc-4791-9ccc-60108710521f</td></tr><tr><td>104</td><td>5005</td><td>2000</td><td>SUCCESS</td><td>2026-11-23T14:50:10.670Z</td><td>transactions.csv</td><td>c8b21336-93bc-4791-9ccc-60108710521f</td></tr><tr><td>105</td><td>5006</td><td>3500</td><td>PENDING</td><td>2026-11-23T14:50:10.670Z</td><td>transactions.csv</td><td>c8b21336-93bc-4791-9ccc-60108710521f</td></tr></tbody></table></div>"
      ]
     },
     "metadata": {
      "application/vnd.databricks.v1+output": {
       "addedWidgets": {},
       "aggData": [],
       "aggError": "",
       "aggOverflow": false,
       "aggSchema": [],
       "aggSeriesLimitReached": false,
       "aggType": "",
       "arguments": {},
       "columnCustomDisplayInfos": {},
       "data": [
        [
         102,
         5002,
         1500,
         "SUCCESS",
         "2026-11-23T14:50:10.670Z",
         "transactions.csv",
         "c8b21336-93bc-4791-9ccc-60108710521f"
        ],
        [
         103,
         5003,
         3000,
         "FAILED",
         "2026-11-23T14:50:10.670Z",
         "transactions.csv",
         "c8b21336-93bc-4791-9ccc-60108710521f"
        ],
        [
         104,
         5005,
         2000,
         "SUCCESS",
         "2026-11-23T14:50:10.670Z",
         "transactions.csv",
         "c8b21336-93bc-4791-9ccc-60108710521f"
        ],
        [
         105,
         5006,
         3500,
         "PENDING",
         "2026-11-23T14:50:10.670Z",
         "transactions.csv",
         "c8b21336-93bc-4791-9ccc-60108710521f"
        ]
       ],
       "datasetInfos": [],
       "dbfsResultPath": null,
       "isJsonSchema": true,
       "metadata": {},
       "overflow": false,
       "plotOptions": {
        "customPlotOptions": {},
        "displayType": "table",
        "pivotAggregation": null,
        "pivotColumns": null,
        "xColumns": null,
        "yColumns": null
       },
       "removedWidgets": [],
       "schema": [
        {
         "metadata": "{}",
         "name": "customer_id",
         "type": "\"integer\""
        },
        {
         "metadata": "{}",
         "name": "transaction_id",
         "type": "\"integer\""
        },
        {
         "metadata": "{}",
         "name": "amount",
         "type": "\"integer\""
        },
        {
         "metadata": "{}",
         "name": "status",
         "type": "\"string\""
        },
        {
         "metadata": "{}",
         "name": "ingestion_timestamp",
         "type": "\"timestamp\""
        },
        {
         "metadata": "{}",
         "name": "source_file_name",
         "type": "\"string\""
        },
        {
         "metadata": "{}",
         "name": "batch_id",
         "type": "\"string\""
        }
       ],
       "type": "table"
      }
     },
     "output_type": "display_data"
    }
   ],
   "source": [
    "\n",
    "# Step 9: Identify transactions with unknown customers\n",
    "\n",
    "df_orphan_transactions = (\n",
    "    df_non_negative_transactions\n",
    "    .join(\n",
    "        df_silver_customers.select(\"customer_id\"),\n",
    "        on=\"customer_id\",\n",
    "        how=\"left_anti\"\n",
    "    )\n",
    "    .withColumn(\n",
    "        \"exception_reason\",\n",
    "        F.lit(\"CUSTOMER_NOT_FOUND\")\n",
    "    )\n",
    ")\n",
    "\n",
    "# Keep transactions with valid customers\n",
    "df_valid_transactions = (\n",
    "    df_non_negative_transactions\n",
    "    .join(\n",
    "        df_silver_customers.select(\"customer_id\"),\n",
    "        on=\"customer_id\",\n",
    "        how=\"inner\"\n",
    "    )\n",
    ")\n",
    "\n",
    "print(\"Unknown customer transactions:\",\n",
    "      df_orphan_transactions.count())\n",
    "\n",
    "print(\"Valid transactions:\",\n",
    "      df_valid_transactions.count())\n",
    "\n",
    "display(df_orphan_transactions)\n",
    "display(df_valid_transactions)\n"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 0,
   "metadata": {
    "application/vnd.databricks.v1+cell": {
     "cellMetadata": {
      "byteLimit": 2048000,
      "rowLimit": 10000
     },
     "finishTime": 1795530941985,
     "inputWidgets": {},
     "nuid": "f1879ddb-68a2-4f9f-a5c7-ddce89bb5a33",
     "showTitle": false,
     "startTime": 1795530940819,
     "submitTime": 1795530940766,
     "tableResultSettingsMap": {},
     "title": ""
    }
   },
   "outputs": [
    {
     "output_type": "display_data",
     "data": {
      "text/html": [
       "<style scoped>\n",
       "  .table-result-container {\n",
       "    max-height: 300px;\n",
       "    overflow: auto;\n",
       "  }\n",
       "  table, th, td {\n",
       "    border: 1px solid black;\n",
       "    border-collapse: collapse;\n",
       "  }\n",
       "  th, td {\n",
       "    padding: 5px;\n",
       "  }\n",
       "  th {\n",
       "    text-align: left;\n",
       "  }\n",
       "</style><div class='table-result-container'><table class='table-result'><thead style='background-color: white'><tr><th>customer_id</th></tr></thead><tbody><tr><td>101</td></tr></tbody></table></div>"
      ]
     },
     "metadata": {
      "application/vnd.databricks.v1+output": {
       "addedWidgets": {},
       "aggData": [],
       "aggError": "",
       "aggOverflow": false,
       "aggSchema": [],
       "aggSeriesLimitReached": false,
       "aggType": "",
       "arguments": {},
       "columnCustomDisplayInfos": {},
       "data": [
        [
         101
        ]
       ],
       "datasetInfos": [],
       "dbfsResultPath": null,
       "isJsonSchema": true,
       "metadata": {},
       "overflow": false,
       "plotOptions": {
        "customPlotOptions": {},
        "displayType": "table",
        "pivotAggregation": null,
        "pivotColumns": null,
        "xColumns": null,
        "yColumns": null
       },
       "removedWidgets": [],
       "schema": [
        {
         "metadata": "{}",
         "name": "customer_id",
         "type": "\"integer\""
        }
       ],
       "type": "table"
      }
     },
     "output_type": "display_data"
    }
   ],
   "source": [
    "\n",
    "# Get customer IDs that have duplicate records\n",
    "\n",
    "df_ambiguous_customer_ids = (\n",
    "    df_customer_exceptions\n",
    "    .select(\"customer_id\")\n",
    "    .distinct()\n",
    ")\n",
    "\n",
    "display(df_ambiguous_customer_ids)\n"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 0,
   "metadata": {
    "application/vnd.databricks.v1+cell": {
     "cellMetadata": {
      "byteLimit": 2048000,
      "rowLimit": 10000
     },
     "finishTime": 1795530964003,
     "inputWidgets": {},
     "nuid": "edc9bca6-bc89-443f-b236-2de4841deb77",
     "showTitle": false,
     "startTime": 1795530962626,
     "submitTime": 1795530962582,
     "tableResultSettingsMap": {},
     "title": ""
    }
   },
   "outputs": [
    {
     "output_type": "display_data",
     "data": {
      "text/html": [
       "<style scoped>\n",
       "  .table-result-container {\n",
       "    max-height: 300px;\n",
       "    overflow: auto;\n",
       "  }\n",
       "  table, th, td {\n",
       "    border: 1px solid black;\n",
       "    border-collapse: collapse;\n",
       "  }\n",
       "  th, td {\n",
       "    padding: 5px;\n",
       "  }\n",
       "  th {\n",
       "    text-align: left;\n",
       "  }\n",
       "</style><div class='table-result-container'><table class='table-result'><thead style='background-color: white'><tr><th>customer_id</th><th>transaction_id</th><th>amount</th><th>status</th><th>ingestion_timestamp</th><th>source_file_name</th><th>batch_id</th><th>exception_reason</th></tr></thead><tbody><tr><td>101</td><td>5001</td><td>2500</td><td>SUCCESS</td><td>2026-11-23T14:50:10.670Z</td><td>transactions.csv</td><td>c8b21336-93bc-4791-9ccc-60108710521f</td><td>AMBIGUOUS_CUSTOMER_REFERENCE</td></tr><tr><td>101</td><td>5004</td><td>4500</td><td>SUCCESS</td><td>2026-11-23T14:50:10.670Z</td><td>transactions.csv</td><td>c8b21336-93bc-4791-9ccc-60108710521f</td><td>AMBIGUOUS_CUSTOMER_REFERENCE</td></tr></tbody></table></div>"
      ]
     },
     "metadata": {
      "application/vnd.databricks.v1+output": {
       "addedWidgets": {},
       "aggData": [],
       "aggError": "",
       "aggOverflow": false,
       "aggSchema": [],
       "aggSeriesLimitReached": false,
       "aggType": "",
       "arguments": {},
       "columnCustomDisplayInfos": {},
       "data": [
        [
         101,
         5001,
         2500,
         "SUCCESS",
         "2026-11-23T14:50:10.670Z",
         "transactions.csv",
         "c8b21336-93bc-4791-9ccc-60108710521f",
         "AMBIGUOUS_CUSTOMER_REFERENCE"
        ],
        [
         101,
         5004,
         4500,
         "SUCCESS",
         "2026-11-23T14:50:10.670Z",
         "transactions.csv",
         "c8b21336-93bc-4791-9ccc-60108710521f",
         "AMBIGUOUS_CUSTOMER_REFERENCE"
        ]
       ],
       "datasetInfos": [],
       "dbfsResultPath": null,
       "isJsonSchema": true,
       "metadata": {},
       "overflow": false,
       "plotOptions": {
        "customPlotOptions": {},
        "displayType": "table",
        "pivotAggregation": null,
        "pivotColumns": null,
        "xColumns": null,
        "yColumns": null
       },
       "removedWidgets": [],
       "schema": [
        {
         "metadata": "{}",
         "name": "customer_id",
         "type": "\"integer\""
        },
        {
         "metadata": "{}",
         "name": "transaction_id",
         "type": "\"integer\""
        },
        {
         "metadata": "{}",
         "name": "amount",
         "type": "\"integer\""
        },
        {
         "metadata": "{}",
         "name": "status",
         "type": "\"string\""
        },
        {
         "metadata": "{}",
         "name": "ingestion_timestamp",
         "type": "\"timestamp\""
        },
        {
         "metadata": "{}",
         "name": "source_file_name",
         "type": "\"string\""
        },
        {
         "metadata": "{}",
         "name": "batch_id",
         "type": "\"string\""
        },
        {
         "metadata": "{}",
         "name": "exception_reason",
         "type": "\"string\""
        }
       ],
       "type": "table"
      }
     },
     "output_type": "display_data"
    }
   ],
   "source": [
    "\n",
    "# Identify transactions linked to ambiguous customer records\n",
    "\n",
    "df_ambiguous_transactions = (\n",
    "    df_non_negative_transactions\n",
    "    .join(\n",
    "        df_ambiguous_customer_ids,\n",
    "        on=\"customer_id\",\n",
    "        how=\"inner\"\n",
    "    )\n",
    "    .withColumn(\n",
    "        \"exception_reason\",\n",
    "        F.lit(\"AMBIGUOUS_CUSTOMER_REFERENCE\")\n",
    "    )\n",
    ")\n",
    "\n",
    "display(df_ambiguous_transactions)\n"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 0,
   "metadata": {
    "application/vnd.databricks.v1+cell": {
     "cellMetadata": {
      "byteLimit": 2048000,
      "rowLimit": 10000
     },
     "finishTime": 1795530985920,
     "inputWidgets": {},
     "nuid": "063203c3-e6dd-4859-af13-b7ad2dfcdf5a",
     "showTitle": false,
     "startTime": 1795530984711,
     "submitTime": 1795530984668,
     "tableResultSettingsMap": {},
     "title": ""
    }
   },
   "outputs": [
    {
     "output_type": "display_data",
     "data": {
      "text/html": [
       "<style scoped>\n",
       "  .table-result-container {\n",
       "    max-height: 300px;\n",
       "    overflow: auto;\n",
       "  }\n",
       "  table, th, td {\n",
       "    border: 1px solid black;\n",
       "    border-collapse: collapse;\n",
       "  }\n",
       "  th, td {\n",
       "    padding: 5px;\n",
       "  }\n",
       "  th {\n",
       "    text-align: left;\n",
       "  }\n",
       "</style><div class='table-result-container'><table class='table-result'><thead style='background-color: white'><tr><th>customer_id</th><th>transaction_id</th><th>amount</th><th>status</th><th>ingestion_timestamp</th><th>source_file_name</th><th>batch_id</th><th>exception_reason</th></tr></thead><tbody><tr><td>999</td><td>5009</td><td>1000</td><td>SUCCESS</td><td>2026-11-23T14:50:10.670Z</td><td>transactions.csv</td><td>c8b21336-93bc-4791-9ccc-60108710521f</td><td>CUSTOMER_NOT_FOUND</td></tr></tbody></table></div>"
      ]
     },
     "metadata": {
      "application/vnd.databricks.v1+output": {
       "addedWidgets": {},
       "aggData": [],
       "aggError": "",
       "aggOverflow": false,
       "aggSchema": [],
       "aggSeriesLimitReached": false,
       "aggType": "",
       "arguments": {},
       "columnCustomDisplayInfos": {},
       "data": [
        [
         999,
         5009,
         1000,
         "SUCCESS",
         "2026-11-23T14:50:10.670Z",
         "transactions.csv",
         "c8b21336-93bc-4791-9ccc-60108710521f",
         "CUSTOMER_NOT_FOUND"
        ]
       ],
       "datasetInfos": [],
       "dbfsResultPath": null,
       "isJsonSchema": true,
       "metadata": {},
       "overflow": false,
       "plotOptions": {
        "customPlotOptions": {},
        "displayType": "table",
        "pivotAggregation": null,
        "pivotColumns": null,
        "xColumns": null,
        "yColumns": null
       },
       "removedWidgets": [],
       "schema": [
        {
         "metadata": "{}",
         "name": "customer_id",
         "type": "\"integer\""
        },
        {
         "metadata": "{}",
         "name": "transaction_id",
         "type": "\"integer\""
        },
        {
         "metadata": "{}",
         "name": "amount",
         "type": "\"integer\""
        },
        {
         "metadata": "{}",
         "name": "status",
         "type": "\"string\""
        },
        {
         "metadata": "{}",
         "name": "ingestion_timestamp",
         "type": "\"timestamp\""
        },
        {
         "metadata": "{}",
         "name": "source_file_name",
         "type": "\"string\""
        },
        {
         "metadata": "{}",
         "name": "batch_id",
         "type": "\"string\""
        },
        {
         "metadata": "{}",
         "name": "exception_reason",
         "type": "\"string\""
        }
       ],
       "type": "table"
      }
     },
     "output_type": "display_data"
    }
   ],
   "source": [
    "\n",
    "# Find transactions whose customer IDs do not exist in Bronze\n",
    "\n",
    "df_all_customer_ids = (\n",
    "    df_customers_bronze\n",
    "    .select(\"customer_id\")\n",
    "    .distinct()\n",
    ")\n",
    "\n",
    "df_unknown_transactions = (\n",
    "    df_non_negative_transactions\n",
    "    .join(\n",
    "        df_all_customer_ids,\n",
    "        on=\"customer_id\",\n",
    "        how=\"left_anti\"\n",
    "    )\n",
    "    .withColumn(\n",
    "        \"exception_reason\",\n",
    "        F.lit(\"CUSTOMER_NOT_FOUND\")\n",
    "    )\n",
    ")\n",
    "\n",
    "display(df_unknown_transactions)\n"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 0,
   "metadata": {
    "application/vnd.databricks.v1+cell": {
     "cellMetadata": {
      "byteLimit": 2048000,
      "rowLimit": 10000
     },
     "finishTime": 1795531030272,
     "inputWidgets": {},
     "nuid": "1160d7e1-7edd-47aa-ab8e-ef2beb3fa85e",
     "showTitle": false,
     "startTime": 1795531026388,
     "submitTime": 1795531026344,
     "tableResultSettingsMap": {},
     "title": ""
    }
   },
   "outputs": [
    {
     "output_type": "stream",
     "name": "stdout",
     "output_type": "stream",
     "text": [
      "Valid transactions: 4\nAmbiguous transactions: 2\nUnknown customer transactions: 1\n"
     ]
    },
    {
     "output_type": "display_data",
     "data": {
      "text/html": [
       "<style scoped>\n",
       "  .table-result-container {\n",
       "    max-height: 300px;\n",
       "    overflow: auto;\n",
       "  }\n",
       "  table, th, td {\n",
       "    border: 1px solid black;\n",
       "    border-collapse: collapse;\n",
       "  }\n",
       "  th, td {\n",
       "    padding: 5px;\n",
       "  }\n",
       "  th {\n",
       "    text-align: left;\n",
       "  }\n",
       "</style><div class='table-result-container'><table class='table-result'><thead style='background-color: white'><tr><th>customer_id</th><th>transaction_id</th><th>amount</th><th>status</th><th>ingestion_timestamp</th><th>source_file_name</th><th>batch_id</th></tr></thead><tbody><tr><td>102</td><td>5002</td><td>1500</td><td>SUCCESS</td><td>2026-11-23T14:50:10.670Z</td><td>transactions.csv</td><td>c8b21336-93bc-4791-9ccc-60108710521f</td></tr><tr><td>103</td><td>5003</td><td>3000</td><td>FAILED</td><td>2026-11-23T14:50:10.670Z</td><td>transactions.csv</td><td>c8b21336-93bc-4791-9ccc-60108710521f</td></tr><tr><td>104</td><td>5005</td><td>2000</td><td>SUCCESS</td><td>2026-11-23T14:50:10.670Z</td><td>transactions.csv</td><td>c8b21336-93bc-4791-9ccc-60108710521f</td></tr><tr><td>105</td><td>5006</td><td>3500</td><td>PENDING</td><td>2026-11-23T14:50:10.670Z</td><td>transactions.csv</td><td>c8b21336-93bc-4791-9ccc-60108710521f</td></tr></tbody></table></div>"
      ]
     },
     "metadata": {
      "application/vnd.databricks.v1+output": {
       "addedWidgets": {},
       "aggData": [],
       "aggError": "",
       "aggOverflow": false,
       "aggSchema": [],
       "aggSeriesLimitReached": false,
       "aggType": "",
       "arguments": {},
       "columnCustomDisplayInfos": {},
       "data": [
        [
         102,
         5002,
         1500,
         "SUCCESS",
         "2026-11-23T14:50:10.670Z",
         "transactions.csv",
         "c8b21336-93bc-4791-9ccc-60108710521f"
        ],
        [
         103,
         5003,
         3000,
         "FAILED",
         "2026-11-23T14:50:10.670Z",
         "transactions.csv",
         "c8b21336-93bc-4791-9ccc-60108710521f"
        ],
        [
         104,
         5005,
         2000,
         "SUCCESS",
         "2026-11-23T14:50:10.670Z",
         "transactions.csv",
         "c8b21336-93bc-4791-9ccc-60108710521f"
        ],
        [
         105,
         5006,
         3500,
         "PENDING",
         "2026-11-23T14:50:10.670Z",
         "transactions.csv",
         "c8b21336-93bc-4791-9ccc-60108710521f"
        ]
       ],
       "datasetInfos": [],
       "dbfsResultPath": null,
       "isJsonSchema": true,
       "metadata": {},
       "overflow": false,
       "plotOptions": {
        "customPlotOptions": {},
        "displayType": "table",
        "pivotAggregation": null,
        "pivotColumns": null,
        "xColumns": null,
        "yColumns": null
       },
       "removedWidgets": [],
       "schema": [
        {
         "metadata": "{}",
         "name": "customer_id",
         "type": "\"integer\""
        },
        {
         "metadata": "{}",
         "name": "transaction_id",
         "type": "\"integer\""
        },
        {
         "metadata": "{}",
         "name": "amount",
         "type": "\"integer\""
        },
        {
         "metadata": "{}",
         "name": "status",
         "type": "\"string\""
        },
        {
         "metadata": "{}",
         "name": "ingestion_timestamp",
         "type": "\"timestamp\""
        },
        {
         "metadata": "{}",
         "name": "source_file_name",
         "type": "\"string\""
        },
        {
         "metadata": "{}",
         "name": "batch_id",
         "type": "\"string\""
        }
       ],
       "type": "table"
      }
     },
     "output_type": "display_data"
    }
   ],
   "source": [
    "\n",
    "# Remove ambiguous customer references\n",
    "\n",
    "df_transactions_without_ambiguous = (\n",
    "    df_non_negative_transactions\n",
    "    .join(\n",
    "        df_ambiguous_customer_ids,\n",
    "        on=\"customer_id\",\n",
    "        how=\"left_anti\"\n",
    "    )\n",
    ")\n",
    "\n",
    "# Keep only transactions with valid customer references\n",
    "\n",
    "df_valid_transactions = (\n",
    "    df_transactions_without_ambiguous\n",
    "    .join(\n",
    "        df_all_customer_ids,\n",
    "        on=\"customer_id\",\n",
    "        how=\"inner\"\n",
    "    )\n",
    ")\n",
    "\n",
    "print(\"Valid transactions:\", df_valid_transactions.count())\n",
    "print(\"Ambiguous transactions:\", df_ambiguous_transactions.count())\n",
    "print(\"Unknown customer transactions:\", df_unknown_transactions.count())\n",
    "\n",
    "display(df_valid_transactions)\n"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 0,
   "metadata": {
    "application/vnd.databricks.v1+cell": {
     "cellMetadata": {
      "byteLimit": 2048000,
      "rowLimit": 10000
     },
     "finishTime": 1795531129269,
     "inputWidgets": {},
     "nuid": "32c72e6c-599b-41c3-96c2-bda852711f95",
     "showTitle": false,
     "startTime": 1795531128154,
     "submitTime": 1795531128099,
     "tableResultSettingsMap": {},
     "title": ""
    }
   },
   "outputs": [
    {
     "output_type": "display_data",
     "data": {
      "text/html": [
       "<style scoped>\n",
       "  .table-result-container {\n",
       "    max-height: 300px;\n",
       "    overflow: auto;\n",
       "  }\n",
       "  table, th, td {\n",
       "    border: 1px solid black;\n",
       "    border-collapse: collapse;\n",
       "  }\n",
       "  th, td {\n",
       "    padding: 5px;\n",
       "  }\n",
       "  th {\n",
       "    text-align: left;\n",
       "  }\n",
       "</style><div class='table-result-container'><table class='table-result'><thead style='background-color: white'><tr><th>transaction_id</th><th>customer_id</th><th>amount</th><th>status</th><th>ingestion_timestamp</th><th>source_file_name</th><th>batch_id</th><th>exception_reason</th></tr></thead><tbody><tr><td>5008</td><td>101</td><td>2500</td><td>SUCCESS</td><td>2026-11-23T14:50:10.670Z</td><td>transactions.csv</td><td>c8b21336-93bc-4791-9ccc-60108710521f</td><td>DUPLICATE_TRANSACTION_ID</td></tr><tr><td>5008</td><td>101</td><td>2500</td><td>SUCCESS</td><td>2026-11-23T14:50:10.670Z</td><td>transactions.csv</td><td>c8b21336-93bc-4791-9ccc-60108710521f</td><td>DUPLICATE_TRANSACTION_ID</td></tr></tbody></table></div>"
      ]
     },
     "metadata": {
      "application/vnd.databricks.v1+output": {
       "addedWidgets": {},
       "aggData": [],
       "aggError": "",
       "aggOverflow": false,
       "aggSchema": [],
       "aggSeriesLimitReached": false,
       "aggType": "",
       "arguments": {},
       "columnCustomDisplayInfos": {},
       "data": [
        [
         5008,
         101,
         2500,
         "SUCCESS",
         "2026-11-23T14:50:10.670Z",
         "transactions.csv",
         "c8b21336-93bc-4791-9ccc-60108710521f",
         "DUPLICATE_TRANSACTION_ID"
        ],
        [
         5008,
         101,
         2500,
         "SUCCESS",
         "2026-11-23T14:50:10.670Z",
         "transactions.csv",
         "c8b21336-93bc-4791-9ccc-60108710521f",
         "DUPLICATE_TRANSACTION_ID"
        ]
       ],
       "datasetInfos": [],
       "dbfsResultPath": null,
       "isJsonSchema": true,
       "metadata": {},
       "overflow": false,
       "plotOptions": {
        "customPlotOptions": {},
        "displayType": "table",
        "pivotAggregation": null,
        "pivotColumns": null,
        "xColumns": null,
        "yColumns": null
       },
       "removedWidgets": [],
       "schema": [
        {
         "metadata": "{}",
         "name": "transaction_id",
         "type": "\"integer\""
        },
        {
         "metadata": "{}",
         "name": "customer_id",
         "type": "\"integer\""
        },
        {
         "metadata": "{}",
         "name": "amount",
         "type": "\"integer\""
        },
        {
         "metadata": "{}",
         "name": "status",
         "type": "\"string\""
        },
        {
         "metadata": "{}",
         "name": "ingestion_timestamp",
         "type": "\"timestamp\""
        },
        {
         "metadata": "{}",
         "name": "source_file_name",
         "type": "\"string\""
        },
        {
         "metadata": "{}",
         "name": "batch_id",
         "type": "\"string\""
        },
        {
         "metadata": "{}",
         "name": "exception_reason",
         "type": "\"string\""
        }
       ],
       "type": "table"
      }
     },
     "output_type": "display_data"
    }
   ],
   "source": [
    "\n",
    "# Step 14.1: Prepare duplicate transaction exceptions\n",
    "\n",
    "df_duplicate_transactions_final = (\n",
    "    df_duplicate_transactions\n",
    "    .withColumn(\n",
    "        \"exception_reason\",\n",
    "        F.lit(\"DUPLICATE_TRANSACTION_ID\")\n",
    "    )\n",
    "    .drop(\"transaction_id_count\")\n",
    ")\n",
    "\n",
    "display(df_duplicate_transactions_final)\n"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 0,
   "metadata": {
    "application/vnd.databricks.v1+cell": {
     "cellMetadata": {
      "byteLimit": 2048000,
      "rowLimit": 10000
     },
     "finishTime": 1795531179214,
     "inputWidgets": {},
     "nuid": "0e8f5868-576c-482c-94b6-568f3f268289",
     "showTitle": false,
     "startTime": 1795531162685,
     "submitTime": 1795531162635,
     "tableResultSettingsMap": {},
     "title": ""
    }
   },
   "outputs": [
    {
     "output_type": "stream",
     "name": "stdout",
     "output_type": "stream",
     "text": [
      "All Silver transaction tables created successfully!\n"
     ]
    }
   ],
   "source": [
    "\n",
    "# Step 14.2: Save Silver transaction tables\n",
    "\n",
    "# 1. Valid transactions\n",
    "df_valid_transactions.write \\\n",
    "    .format(\"delta\") \\\n",
    "    .mode(\"overwrite\") \\\n",
    "    .saveAsTable(\n",
    "        \"customer_data_platform.silver.transactions\"\n",
    "    )\n",
    "\n",
    "# 2. Duplicate transaction exceptions\n",
    "df_duplicate_transactions_final.write \\\n",
    "    .format(\"delta\") \\\n",
    "    .mode(\"overwrite\") \\\n",
    "    .saveAsTable(\n",
    "        \"customer_data_platform.silver.transaction_exceptions\"\n",
    "    )\n",
    "\n",
    "# 3. Negative transaction exceptions\n",
    "df_negative_transactions.write \\\n",
    "    .format(\"delta\") \\\n",
    "    .mode(\"overwrite\") \\\n",
    "    .saveAsTable(\n",
    "        \"customer_data_platform.silver.negative_transaction_exceptions\"\n",
    "    )\n",
    "\n",
    "# 4. Ambiguous customer exceptions\n",
    "df_ambiguous_transactions.write \\\n",
    "    .format(\"delta\") \\\n",
    "    .mode(\"overwrite\") \\\n",
    "    .saveAsTable(\n",
    "        \"customer_data_platform.silver.ambiguous_customer_exceptions\"\n",
    "    )\n",
    "\n",
    "# 5. Unknown customer exceptions\n",
    "df_unknown_transactions.write \\\n",
    "    .format(\"delta\") \\\n",
    "    .mode(\"overwrite\") \\\n",
    "    .saveAsTable(\n",
    "        \"customer_data_platform.silver.unknown_customer_exceptions\"\n",
    "    )\n",
    "\n",
    "print(\"All Silver transaction tables created successfully!\")\n"
   ]
  }
 ],
 "metadata": {
  "application/vnd.databricks.v1+notebook": {
   "computePreferences": null,
   "dashboards": [],
   "environmentMetadata": {
    "base_environment": "",
    "environment_version": "6"
   },
   "inputWidgetPreferences": null,
   "language": "python",
   "notebookMetadata": {
    "pythonIndentUnit": 4
   },
   "notebookName": "02_silver_data_quality_transformations",
   "widgets": {}
  },
  "language_info": {
   "name": "python"
  }
 },
 "nbformat": 4,
 "nbformat_minor": 0
}