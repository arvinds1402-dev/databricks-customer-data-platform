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
     "finishTime": 1795531377612,
     "inputWidgets": {},
     "nuid": "aeab2aec-e4eb-4c34-a01e-56dbc467bbd8",
     "showTitle": false,
     "startTime": 1795531375699,
     "submitTime": 1795531374965,
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
      "Silver customers: 5\nSilver transactions: 4\n"
     ]
    }
   ],
   "source": [
    "\n",
    "# Step 1: Read Silver tables\n",
    "\n",
    "df_silver_customers = spark.table(\n",
    "    \"customer_data_platform.silver.customers\"\n",
    ")\n",
    "\n",
    "df_silver_transactions = spark.table(\n",
    "    \"customer_data_platform.silver.transactions\"\n",
    ")\n",
    "\n",
    "print(\"Silver customers:\", df_silver_customers.count())\n",
    "print(\"Silver transactions:\", df_silver_transactions.count())\n"
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
     "finishTime": 1795531423869,
     "inputWidgets": {},
     "nuid": "1c1331de-3fe1-4602-b5bb-67f49b2866cd",
     "showTitle": false,
     "startTime": 1795531421400,
     "submitTime": 1795531421321,
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
      "Customer dimension count: 5\n"
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
       "</style><div class='table-result-container'><table class='table-result'><thead style='background-color: white'><tr><th>customer_id</th><th>customer_name</th><th>city</th><th>email</th><th>email_missing_flag</th></tr></thead><tbody><tr><td>102</td><td>Bala</td><td>Bangalore</td><td>bala@gmail.com</td><td>N</td></tr><tr><td>103</td><td>Karthik</td><td>Mumbai</td><td>null</td><td>Y</td></tr><tr><td>104</td><td>Divya</td><td>Chennai</td><td>divya@gmail.com</td><td>N</td></tr><tr><td>105</td><td>Manoj</td><td>Hyderabad</td><td>manoj@gmail.com</td><td>N</td></tr><tr><td>106</td><td>Ravi</td><td>Coimbatore</td><td>null</td><td>Y</td></tr></tbody></table></div>"
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
         "N"
        ],
        [
         103,
         "Karthik",
         "Mumbai",
         null,
         "Y"
        ],
        [
         104,
         "Divya",
         "Chennai",
         "divya@gmail.com",
         "N"
        ],
        [
         105,
         "Manoj",
         "Hyderabad",
         "manoj@gmail.com",
         "N"
        ],
        [
         106,
         "Ravi",
         "Coimbatore",
         null,
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
         "name": "email_missing_flag",
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
    "# Step 2: Create Customer Dimension\n",
    "\n",
    "df_dim_customer = (\n",
    "    df_silver_customers\n",
    "    .select(\n",
    "        \"customer_id\",\n",
    "        \"customer_name\",\n",
    "        \"city\",\n",
    "        \"email\",\n",
    "        \"email_missing_flag\"\n",
    "    )\n",
    ")\n",
    "\n",
    "print(\"Customer dimension count:\", df_dim_customer.count())\n",
    "\n",
    "display(df_dim_customer)\n"
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
     "finishTime": 1795531454113,
     "inputWidgets": {},
     "nuid": "03435ba7-9986-4440-a2ab-51564acb30d0",
     "showTitle": false,
     "startTime": 1795531450778,
     "submitTime": 1795531450710,
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
      "Gold Customer Dimension created successfully!\n"
     ]
    }
   ],
   "source": [
    "\n",
    "# Step 3: Save Customer Dimension\n",
    "\n",
    "df_dim_customer.write \\\n",
    "    .format(\"delta\") \\\n",
    "    .mode(\"overwrite\") \\\n",
    "    .saveAsTable(\n",
    "        \"customer_data_platform.gold.dim_customer\"\n",
    "    )\n",
    "\n",
    "print(\"Gold Customer Dimension created successfully!\")\n"
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
     "finishTime": 1795531620192,
     "inputWidgets": {},
     "nuid": "29385003-fe11-4eba-9411-6bbc07eed4b5",
     "showTitle": false,
     "startTime": 1795531618421,
     "submitTime": 1795531618370,
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
      "Fact Transaction count: 4\n"
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
       "</style><div class='table-result-container'><table class='table-result'><thead style='background-color: white'><tr><th>transaction_id</th><th>customer_id</th><th>amount</th><th>status</th></tr></thead><tbody><tr><td>5002</td><td>102</td><td>1500</td><td>SUCCESS</td></tr><tr><td>5003</td><td>103</td><td>3000</td><td>FAILED</td></tr><tr><td>5005</td><td>104</td><td>2000</td><td>SUCCESS</td></tr><tr><td>5006</td><td>105</td><td>3500</td><td>PENDING</td></tr></tbody></table></div>"
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
         5002,
         102,
         1500,
         "SUCCESS"
        ],
        [
         5003,
         103,
         3000,
         "FAILED"
        ],
        [
         5005,
         104,
         2000,
         "SUCCESS"
        ],
        [
         5006,
         105,
         3500,
         "PENDING"
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
    "# Step 5.2: Join Transactions with Customer Dimension\n",
    "\n",
    "df_fact_transactions = (\n",
    "    df_silver_transactions\n",
    "    .join(\n",
    "        df_dim_customer,\n",
    "        on=\"customer_id\",\n",
    "        how=\"inner\"\n",
    "    )\n",
    "    .select(\n",
    "        \"transaction_id\",\n",
    "        \"customer_id\",\n",
    "        \"amount\",\n",
    "        \"status\"\n",
    "    )\n",
    ")\n",
    "\n",
    "print(\n",
    "    \"Fact Transaction count:\",\n",
    "    df_fact_transactions.count()\n",
    ")\n",
    "\n",
    "display(df_fact_transactions)\n"
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
     "finishTime": 1795531683047,
     "inputWidgets": {},
     "nuid": "cf9b3a19-831e-4639-b799-6cb194dcd7ae",
     "showTitle": false,
     "startTime": 1795531679464,
     "submitTime": 1795531679404,
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
      "Gold Fact Transaction table created successfully!\n"
     ]
    }
   ],
   "source": [
    "\n",
    "# Step 6: Save Fact Transactions\n",
    "\n",
    "df_fact_transactions.write \\\n",
    "    .format(\"delta\") \\\n",
    "    .mode(\"overwrite\") \\\n",
    "    .saveAsTable(\n",
    "        \"customer_data_platform.gold.fact_transactions\"\n",
    "    )\n",
    "\n",
    "print(\"Gold Fact Transaction table created successfully!\")\n"
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
     "finishTime": 1795531829587,
     "inputWidgets": {},
     "nuid": "14956568-9686-41c4-a7f6-925198103aa3",
     "showTitle": false,
     "startTime": 1795531828716,
     "submitTime": 1795531828658,
     "tableResultSettingsMap": {
      "0": {
       "dataGridStateBlob": "{\"version\":1,\"tableState\":{\"columnPinning\":{\"left\":[\"#row_number#\"],\"right\":[]},\"columnSizing\":{\"customer_sk\":222},\"columnVisibility\":{},\"sorting\":[]},\"settings\":{\"columns\":{}},\"syncTimestamp\":1795531837236}",
       "filterBlob": null,
       "queryPlanFiltersBlob": null,
       "tableResultIndex": 0
      }
     },
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
       "</style><div class='table-result-container'><table class='table-result'><thead style='background-color: white'><tr><th>customer_sk</th><th>customer_id</th><th>customer_name</th><th>city</th><th>email</th><th>email_missing_flag</th></tr></thead><tbody><tr><td>-357279511390273435</td><td>102</td><td>Bala</td><td>Bangalore</td><td>bala@gmail.com</td><td>N</td></tr><tr><td>-9162635464676662184</td><td>103</td><td>Karthik</td><td>Mumbai</td><td>null</td><td>Y</td></tr><tr><td>-1298446668641429307</td><td>104</td><td>Divya</td><td>Chennai</td><td>divya@gmail.com</td><td>N</td></tr><tr><td>-2296073738199878700</td><td>105</td><td>Manoj</td><td>Hyderabad</td><td>manoj@gmail.com</td><td>N</td></tr><tr><td>-6139202535883341972</td><td>106</td><td>Ravi</td><td>Coimbatore</td><td>null</td><td>Y</td></tr></tbody></table></div>"
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
         -357279511390273435,
         102,
         "Bala",
         "Bangalore",
         "bala@gmail.com",
         "N"
        ],
        [
         -9162635464676662184,
         103,
         "Karthik",
         "Mumbai",
         null,
         "Y"
        ],
        [
         -1298446668641429307,
         104,
         "Divya",
         "Chennai",
         "divya@gmail.com",
         "N"
        ],
        [
         -2296073738199878700,
         105,
         "Manoj",
         "Hyderabad",
         "manoj@gmail.com",
         "N"
        ],
        [
         -6139202535883341972,
         106,
         "Ravi",
         "Coimbatore",
         null,
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
         "name": "customer_sk",
         "type": "\"long\""
        },
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
         "name": "email_missing_flag",
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
    "# Step 9.1: Generate Customer Surrogate Keys\n",
    "\n",
    "from pyspark.sql import functions as F\n",
    "\n",
    "df_dim_customer_sk = (\n",
    "    df_dim_customer\n",
    "    .withColumn(\n",
    "        \"customer_sk\",\n",
    "        F.xxhash64(F.col(\"customer_id\"))\n",
    "    )\n",
    "    .select(\n",
    "        \"customer_sk\",\n",
    "        \"customer_id\",\n",
    "        \"customer_name\",\n",
    "        \"city\",\n",
    "        \"email\",\n",
    "        \"email_missing_flag\"\n",
    "    )\n",
    ")\n",
    "\n",
    "display(df_dim_customer_sk)\n"
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
     "finishTime": 1795532037328,
     "inputWidgets": {},
     "nuid": "cbb0ef8b-f36a-455b-af1f-f5cb9ae5ed4c",
     "showTitle": true,
     "startTime": 1795532034611,
     "submitTime": 1795532034555,
     "tableResultSettingsMap": {},
     "title": "Save Customer Dimension with Surrogate Key"
    }
   },
   "outputs": [
    {
     "output_type": "stream",
     "name": "stdout",
     "output_type": "stream",
     "text": [
      "Customer dimension updated with surrogate keys!\n"
     ]
    }
   ],
   "source": [
    "\n",
    "# Step 9.2: Save Customer Dimension with Surrogate Key\n",
    "\n",
    "df_dim_customer_sk.write \\\n",
    "    .format(\"delta\") \\\n",
    "    .mode(\"overwrite\") \\\n",
    "    .option(\"overwriteSchema\", \"true\") \\\n",
    "    .saveAsTable(\n",
    "        \"customer_data_platform.gold.dim_customer\"\n",
    "    )\n",
    "\n",
    "print(\"Customer dimension updated with surrogate keys!\")\n"
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
     "finishTime": 1795532087888,
     "inputWidgets": {},
     "nuid": "06049070-a213-4715-9c61-6d4927345c60",
     "showTitle": false,
     "startTime": 1795532086824,
     "submitTime": 1795532086783,
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
       "</style><div class='table-result-container'><table class='table-result'><thead style='background-color: white'><tr><th>transaction_id</th><th>customer_sk</th><th>customer_id</th><th>amount</th><th>status</th></tr></thead><tbody><tr><td>5002</td><td>-357279511390273435</td><td>102</td><td>1500</td><td>SUCCESS</td></tr><tr><td>5003</td><td>-9162635464676662184</td><td>103</td><td>3000</td><td>FAILED</td></tr><tr><td>5005</td><td>-1298446668641429307</td><td>104</td><td>2000</td><td>SUCCESS</td></tr><tr><td>5006</td><td>-2296073738199878700</td><td>105</td><td>3500</td><td>PENDING</td></tr></tbody></table></div>"
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
         5002,
         -357279511390273435,
         102,
         1500,
         "SUCCESS"
        ],
        [
         5003,
         -9162635464676662184,
         103,
         3000,
         "FAILED"
        ],
        [
         5005,
         -1298446668641429307,
         104,
         2000,
         "SUCCESS"
        ],
        [
         5006,
         -2296073738199878700,
         105,
         3500,
         "PENDING"
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
         "name": "customer_sk",
         "type": "\"long\""
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
    "# Step 10: Add Customer Surrogate Key to Fact\n",
    "\n",
    "df_fact_transactions_sk = (\n",
    "    df_silver_transactions\n",
    "    .join(\n",
    "        df_dim_customer_sk.select(\n",
    "            \"customer_id\",\n",
    "            \"customer_sk\"\n",
    "        ),\n",
    "        on=\"customer_id\",\n",
    "        how=\"inner\"\n",
    "    )\n",
    "    .select(\n",
    "        \"transaction_id\",\n",
    "        \"customer_sk\",\n",
    "        \"customer_id\",\n",
    "        \"amount\",\n",
    "        \"status\"\n",
    "    )\n",
    ")\n",
    "\n",
    "display(df_fact_transactions_sk)\n"
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
     "finishTime": 1795532125322,
     "inputWidgets": {},
     "nuid": "31d65c85-fe53-4bef-ae5c-324edcbaac6c",
     "showTitle": false,
     "startTime": 1795532122685,
     "submitTime": 1795532122638,
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
      "Fact table updated with customer surrogate keys!\n"
     ]
    }
   ],
   "source": [
    "\n",
    "# Save updated Gold Fact Table\n",
    "\n",
    "df_fact_transactions_sk.write \\\n",
    "    .format(\"delta\") \\\n",
    "    .mode(\"overwrite\") \\\n",
    "        .option(\"overwriteSchema\", \"true\") \\\n",
    "    .saveAsTable(\n",
    "        \"customer_data_platform.gold.fact_transactions\"\n",
    "    )\n",
    "\n",
    "print(\"Fact table updated with customer surrogate keys!\")\n"
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
     "finishTime": 1795532216512,
     "inputWidgets": {},
     "nuid": "2a6fe6a3-9ff4-443c-95a0-adf35ae5fcbd",
     "showTitle": false,
     "startTime": 1795532215384,
     "submitTime": 1795532215333,
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
       "</style><div class='table-result-container'><table class='table-result'><thead style='background-color: white'><tr><th>customer_sk</th><th>customer_id</th><th>total_transactions</th><th>total_transaction_amount</th><th>successful_transactions</th><th>successful_transaction_amount</th><th>failed_transactions</th></tr></thead><tbody><tr><td>-2296073738199878700</td><td>105</td><td>1</td><td>3500</td><td>0</td><td>0</td><td>0</td></tr><tr><td>-1298446668641429307</td><td>104</td><td>1</td><td>2000</td><td>1</td><td>2000</td><td>0</td></tr><tr><td>-9162635464676662184</td><td>103</td><td>1</td><td>3000</td><td>0</td><td>0</td><td>1</td></tr><tr><td>-357279511390273435</td><td>102</td><td>1</td><td>1500</td><td>1</td><td>1500</td><td>0</td></tr></tbody></table></div>"
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
         -2296073738199878700,
         105,
         1,
         3500,
         0,
         0,
         0
        ],
        [
         -1298446668641429307,
         104,
         1,
         2000,
         1,
         2000,
         0
        ],
        [
         -9162635464676662184,
         103,
         1,
         3000,
         0,
         0,
         1
        ],
        [
         -357279511390273435,
         102,
         1,
         1500,
         1,
         1500,
         0
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
         "name": "customer_sk",
         "type": "\"long\""
        },
        {
         "metadata": "{}",
         "name": "customer_id",
         "type": "\"integer\""
        },
        {
         "metadata": "{}",
         "name": "total_transactions",
         "type": "\"long\""
        },
        {
         "metadata": "{}",
         "name": "total_transaction_amount",
         "type": "\"long\""
        },
        {
         "metadata": "{}",
         "name": "successful_transactions",
         "type": "\"long\""
        },
        {
         "metadata": "{}",
         "name": "successful_transaction_amount",
         "type": "\"long\""
        },
        {
         "metadata": "{}",
         "name": "failed_transactions",
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
    "# Step 11: Aggregate Customer Transaction Metrics\n",
    "\n",
    "df_customer_metrics = (\n",
    "    df_fact_transactions_sk\n",
    "    .groupBy(\"customer_sk\", \"customer_id\")\n",
    "    .agg(\n",
    "        F.count(\"*\").alias(\"total_transactions\"),\n",
    "\n",
    "        F.sum(\"amount\").alias(\n",
    "            \"total_transaction_amount\"\n",
    "        ),\n",
    "\n",
    "        F.sum(\n",
    "            F.when(F.col(\"status\") == \"SUCCESS\", 1)\n",
    "             .otherwise(0)\n",
    "        ).alias(\"successful_transactions\"),\n",
    "\n",
    "        F.sum(\n",
    "            F.when(\n",
    "                F.col(\"status\") == \"SUCCESS\",\n",
    "                F.col(\"amount\")\n",
    "            ).otherwise(0)\n",
    "        ).alias(\"successful_transaction_amount\"),\n",
    "\n",
    "        F.sum(\n",
    "            F.when(F.col(\"status\") == \"FAILED\", 1)\n",
    "             .otherwise(0)\n",
    "        ).alias(\"failed_transactions\")\n",
    "    )\n",
    ")\n",
    "\n",
    "display(df_customer_metrics)\n"
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
     "finishTime": 1795532291921,
     "height": "190",
     "inputWidgets": {},
     "nuid": "124476f7-6584-4a60-85d1-8fe6f4a40774",
     "showTitle": false,
     "startTime": 1795532290467,
     "submitTime": 1795532290413,
     "tableResultSettingsMap": {},
     "title": "",
     "width": "1342"
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
       "</style><div class='table-result-container'><table class='table-result'><thead style='background-color: white'><tr><th>customer_sk</th><th>customer_id</th><th>customer_name</th><th>city</th><th>email</th><th>email_missing_flag</th><th>total_transactions</th><th>total_transaction_amount</th><th>successful_transactions</th><th>successful_transaction_amount</th><th>failed_transactions</th></tr></thead><tbody><tr><td>-357279511390273435</td><td>102</td><td>Bala</td><td>Bangalore</td><td>bala@gmail.com</td><td>N</td><td>1</td><td>1500</td><td>1</td><td>1500</td><td>0</td></tr><tr><td>-9162635464676662184</td><td>103</td><td>Karthik</td><td>Mumbai</td><td>null</td><td>Y</td><td>1</td><td>3000</td><td>0</td><td>0</td><td>1</td></tr><tr><td>-1298446668641429307</td><td>104</td><td>Divya</td><td>Chennai</td><td>divya@gmail.com</td><td>N</td><td>1</td><td>2000</td><td>1</td><td>2000</td><td>0</td></tr><tr><td>-2296073738199878700</td><td>105</td><td>Manoj</td><td>Hyderabad</td><td>manoj@gmail.com</td><td>N</td><td>1</td><td>3500</td><td>0</td><td>0</td><td>0</td></tr><tr><td>-6139202535883341972</td><td>106</td><td>Ravi</td><td>Coimbatore</td><td>null</td><td>Y</td><td>0</td><td>0</td><td>0</td><td>0</td><td>0</td></tr></tbody></table></div>"
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
         -357279511390273435,
         102,
         "Bala",
         "Bangalore",
         "bala@gmail.com",
         "N",
         1,
         1500,
         1,
         1500,
         0
        ],
        [
         -9162635464676662184,
         103,
         "Karthik",
         "Mumbai",
         null,
         "Y",
         1,
         3000,
         0,
         0,
         1
        ],
        [
         -1298446668641429307,
         104,
         "Divya",
         "Chennai",
         "divya@gmail.com",
         "N",
         1,
         2000,
         1,
         2000,
         0
        ],
        [
         -2296073738199878700,
         105,
         "Manoj",
         "Hyderabad",
         "manoj@gmail.com",
         "N",
         1,
         3500,
         0,
         0,
         0
        ],
        [
         -6139202535883341972,
         106,
         "Ravi",
         "Coimbatore",
         null,
         "Y",
         0,
         0,
         0,
         0,
         0
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
         "name": "customer_sk",
         "type": "\"long\""
        },
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
         "name": "email_missing_flag",
         "type": "\"string\""
        },
        {
         "metadata": "{}",
         "name": "total_transactions",
         "type": "\"long\""
        },
        {
         "metadata": "{}",
         "name": "total_transaction_amount",
         "type": "\"long\""
        },
        {
         "metadata": "{}",
         "name": "successful_transactions",
         "type": "\"long\""
        },
        {
         "metadata": "{}",
         "name": "successful_transaction_amount",
         "type": "\"long\""
        },
        {
         "metadata": "{}",
         "name": "failed_transactions",
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
    "# Step 12: Build Customer 360\n",
    "\n",
    "df_customer_360 = (\n",
    "    df_dim_customer_sk\n",
    "    .join(\n",
    "        df_customer_metrics,\n",
    "        on=[\"customer_sk\", \"customer_id\"],\n",
    "        how=\"left\"\n",
    "    )\n",
    "    .fillna({\n",
    "        \"total_transactions\": 0,\n",
    "        \"total_transaction_amount\": 0,\n",
    "        \"successful_transactions\": 0,\n",
    "        \"successful_transaction_amount\": 0,\n",
    "        \"failed_transactions\": 0\n",
    "    })\n",
    ")\n",
    "\n",
    "display(df_customer_360)\n"
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
     "finishTime": 1795532333573,
     "inputWidgets": {},
     "nuid": "3cc8d244-879e-427a-98e7-1815e0e6aa6c",
     "showTitle": false,
     "startTime": 1795532329444,
     "submitTime": 1795532329404,
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
      "Customer 360 table created successfully!\n"
     ]
    }
   ],
   "source": [
    "\n",
    "# Step 13: Save Customer 360 Delta Table\n",
    "\n",
    "df_customer_360.write \\\n",
    "    .format(\"delta\") \\\n",
    "    .mode(\"overwrite\") \\\n",
    "    .saveAsTable(\n",
    "        \"customer_data_platform.gold.customer_360\"\n",
    "    )\n",
    "\n",
    "print(\"Customer 360 table created successfully!\")\n"
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
   "notebookName": "03_gold_dimensional_modeling",
   "widgets": {}
  },
  "language_info": {
   "name": "python"
  }
 },
 "nbformat": 4,
 "nbformat_minor": 0
}