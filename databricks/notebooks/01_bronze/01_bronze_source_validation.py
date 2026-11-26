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
     "finishTime": 1795443014579,
     "inputWidgets": {},
     "nuid": "d2d690de-4228-4f2e-84df-33070a40d5cb",
     "showTitle": false,
     "startTime": 1795443013081,
     "submitTime": 1795443008038,
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
      "Spark Version: 4.2.0\nCurrent Catalog: workspace\nCurrent Schema: default\n"
     ]
    }
   ],
   "source": [
    "\n",
    "# Step 1: Verify Spark environment\n",
    "\n",
    "print(\"Spark Version:\", spark.version)\n",
    "print(\"Current Catalog:\", spark.sql(\"SELECT current_catalog()\").collect()[0][0])\n",
    "print(\"Current Schema:\", spark.sql(\"SELECT current_schema()\").collect()[0][0])\n"
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
     "finishTime": 1795443120375,
     "inputWidgets": {},
     "nuid": "f44ec87c-67a7-4dd8-a3e9-9699f5474ff4",
     "showTitle": false,
     "startTime": 1795443119458,
     "submitTime": 1795443119409,
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
      "Catalog: customer_data_platform\nSchema: bronze\n"
     ]
    }
   ],
   "source": [
    "\n",
    "# Step 2: Set project catalog and Bronze schema\n",
    "\n",
    "spark.sql(\"USE CATALOG customer_data_platform\")\n",
    "spark.sql(\"USE SCHEMA bronze\")\n",
    "\n",
    "print(\"Catalog:\", spark.sql(\"SELECT current_catalog()\").collect()[0][0])\n",
    "print(\"Schema:\", spark.sql(\"SELECT current_schema()\").collect()[0][0])\n"
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
     "finishTime": 1795443198084,
     "inputWidgets": {},
     "nuid": "40b6a835-3b79-4099-8fd6-50e1235474b4",
     "showTitle": false,
     "startTime": 1795443194716,
     "submitTime": 1795443194654,
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
      "S3 connection successful!\nFiles available in raw folder:\ncustomers.csv 263 bytes\ntransactions.csv 269 bytes\n"
     ]
    }
   ],
   "source": [
    "\n",
    "# Step 3: Verify S3 source connectivity\n",
    "\n",
    "source_path = \"s3://python-etl-learning-2026-arvind-unique123/raw/\"\n",
    "\n",
    "try:\n",
    "    files = dbutils.fs.ls(source_path)\n",
    "\n",
    "    print(\"S3 connection successful!\")\n",
    "    print(\"Files available in raw folder:\")\n",
    "\n",
    "    for file in files:\n",
    "        print(file.name, file.size, \"bytes\")\n",
    "\n",
    "except Exception as e:\n",
    "    print(\"S3 connection failed:\")\n",
    "    print(str(e))\n"
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
     "finishTime": 1795443371054,
     "inputWidgets": {},
     "nuid": "7f640f69-30eb-4b2c-84f8-d3183fac4f9a",
     "showTitle": false,
     "startTime": 1795443357580,
     "submitTime": 1795443357526,
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
      "Customers count: 7\nTransactions count: 10\nCustomers schema:\nroot\n |-- customer_id: integer (nullable = true)\n |-- customer_name: string (nullable = true)\n |-- city: string (nullable = true)\n |-- email: string (nullable = true)\n\nTransactions schema:\nroot\n |-- transaction_id: integer (nullable = true)\n |-- customer_id: integer (nullable = true)\n |-- amount: integer (nullable = true)\n |-- status: string (nullable = true)\n\n"
     ]
    }
   ],
   "source": [
    "\n",
    "# Step 4: Read source CSV files using PySpark\n",
    "\n",
    "customers_path = \"s3://python-etl-learning-2026-arvind-unique123/raw/customers.csv\"\n",
    "\n",
    "transactions_path = \"s3://python-etl-learning-2026-arvind-unique123/raw/transactions.csv\"\n",
    "\n",
    "# Read customers\n",
    "df_customers = (\n",
    "    spark.read\n",
    "    .option(\"header\", \"true\")\n",
    "    .option(\"inferSchema\", \"true\")\n",
    "    .csv(customers_path)\n",
    ")\n",
    "\n",
    "# Read transactions\n",
    "df_transactions = (\n",
    "    spark.read\n",
    "    .option(\"header\", \"true\")\n",
    "    .option(\"inferSchema\", \"true\")\n",
    "    .csv(transactions_path)\n",
    ")\n",
    "\n",
    "print(\"Customers count:\", df_customers.count())\n",
    "print(\"Transactions count:\", df_transactions.count())\n",
    "\n",
    "print(\"Customers schema:\")\n",
    "df_customers.printSchema()\n",
    "\n",
    "print(\"Transactions schema:\")\n",
    "df_transactions.printSchema()\n"
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
     "finishTime": 1795443414352,
     "inputWidgets": {},
     "nuid": "ef50a6c2-1f97-4278-b44a-bd3ef2beacf4",
     "showTitle": false,
     "startTime": 1795443411455,
     "submitTime": 1795443411396,
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
      "CUSTOMERS DATA\n"
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
       "</style><div class='table-result-container'><table class='table-result'><thead style='background-color: white'><tr><th>customer_id</th><th>customer_name</th><th>city</th><th>email</th></tr></thead><tbody><tr><td>101</td><td>Arun</td><td>Chennai</td><td>arun@gmail.com</td></tr><tr><td>102</td><td>Bala</td><td>Bangalore</td><td>bala@gmail.com</td></tr><tr><td>103</td><td>Karthik</td><td>Mumbai</td><td>null</td></tr><tr><td>104</td><td>Divya</td><td>Chennai</td><td>divya@gmail.com</td></tr><tr><td>105</td><td>Manoj</td><td>Hyderabad</td><td>manoj@gmail.com</td></tr><tr><td>101</td><td>Arun Kumar</td><td>Chennai</td><td>arunkumar@gmail.com</td></tr><tr><td>106</td><td>Ravi</td><td>Coimbatore</td><td>null</td></tr></tbody></table></div>"
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
         "arun@gmail.com"
        ],
        [
         102,
         "Bala",
         "Bangalore",
         "bala@gmail.com"
        ],
        [
         103,
         "Karthik",
         "Mumbai",
         null
        ],
        [
         104,
         "Divya",
         "Chennai",
         "divya@gmail.com"
        ],
        [
         105,
         "Manoj",
         "Hyderabad",
         "manoj@gmail.com"
        ],
        [
         101,
         "Arun Kumar",
         "Chennai",
         "arunkumar@gmail.com"
        ],
        [
         106,
         "Ravi",
         "Coimbatore",
         null
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
        }
       ],
       "type": "table"
      }
     },
     "output_type": "display_data"
    },
    {
     "output_type": "stream",
     "name": "stdout",
     "output_type": "stream",
     "text": [
      "TRANSACTIONS DATA\n"
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
       "</style><div class='table-result-container'><table class='table-result'><thead style='background-color: white'><tr><th>transaction_id</th><th>customer_id</th><th>amount</th><th>status</th></tr></thead><tbody><tr><td>5001</td><td>101</td><td>2500</td><td>SUCCESS</td></tr><tr><td>5002</td><td>102</td><td>1500</td><td>SUCCESS</td></tr><tr><td>5003</td><td>103</td><td>3000</td><td>FAILED</td></tr><tr><td>5004</td><td>101</td><td>4500</td><td>SUCCESS</td></tr><tr><td>5005</td><td>104</td><td>2000</td><td>SUCCESS</td></tr><tr><td>5006</td><td>105</td><td>3500</td><td>PENDING</td></tr><tr><td>5007</td><td>106</td><td>-500</td><td>SUCCESS</td></tr><tr><td>5008</td><td>101</td><td>2500</td><td>SUCCESS</td></tr><tr><td>5008</td><td>101</td><td>2500</td><td>SUCCESS</td></tr><tr><td>5009</td><td>999</td><td>1000</td><td>SUCCESS</td></tr></tbody></table></div>"
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
         5001,
         101,
         2500,
         "SUCCESS"
        ],
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
         5004,
         101,
         4500,
         "SUCCESS"
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
        ],
        [
         5007,
         106,
         -500,
         "SUCCESS"
        ],
        [
         5008,
         101,
         2500,
         "SUCCESS"
        ],
        [
         5008,
         101,
         2500,
         "SUCCESS"
        ],
        [
         5009,
         999,
         1000,
         "SUCCESS"
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
    "# Step 5: Display source data\n",
    "\n",
    "print(\"CUSTOMERS DATA\")\n",
    "display(df_customers)\n",
    "\n",
    "print(\"TRANSACTIONS DATA\")\n",
    "display(df_transactions)\n"
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
     "finishTime": 1795444539153,
     "inputWidgets": {},
     "nuid": "ef26e3fa-d161-4a8e-ac6d-17f45edda02d",
     "showTitle": false,
     "startTime": 1795444529726,
     "submitTime": 1795444529643,
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
      "\n========== CUSTOMERS ==========\nTotal Records: 7\nColumns: ['customer_id', 'customer_name', 'city', 'email']\n\nSchema:\nroot\n |-- customer_id: integer (nullable = true)\n |-- customer_name: string (nullable = true)\n |-- city: string (nullable = true)\n |-- email: string (nullable = true)\n\n\nNULL Count:\n"
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
       "</style><div class='table-result-container'><table class='table-result'><thead style='background-color: white'><tr><th>customer_id</th><th>customer_name</th><th>city</th><th>email</th></tr></thead><tbody><tr><td>0</td><td>0</td><td>0</td><td>2</td></tr></tbody></table></div>"
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
         0,
         0,
         0,
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
         "type": "\"long\""
        },
        {
         "metadata": "{}",
         "name": "customer_name",
         "type": "\"long\""
        },
        {
         "metadata": "{}",
         "name": "city",
         "type": "\"long\""
        },
        {
         "metadata": "{}",
         "name": "email",
         "type": "\"long\""
        }
       ],
       "type": "table"
      }
     },
     "output_type": "display_data"
    },
    {
     "output_type": "stream",
     "name": "stdout",
     "output_type": "stream",
     "text": [
      "Duplicate Records: 0\n"
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
       "</style><div class='table-result-container'><table class='table-result'><thead style='background-color: white'><tr><th>customer_id</th><th>customer_name</th><th>city</th><th>email</th></tr></thead><tbody><tr><td>101</td><td>Arun</td><td>Chennai</td><td>arun@gmail.com</td></tr><tr><td>102</td><td>Bala</td><td>Bangalore</td><td>bala@gmail.com</td></tr><tr><td>103</td><td>Karthik</td><td>Mumbai</td><td>null</td></tr><tr><td>104</td><td>Divya</td><td>Chennai</td><td>divya@gmail.com</td></tr><tr><td>105</td><td>Manoj</td><td>Hyderabad</td><td>manoj@gmail.com</td></tr><tr><td>101</td><td>Arun Kumar</td><td>Chennai</td><td>arunkumar@gmail.com</td></tr><tr><td>106</td><td>Ravi</td><td>Coimbatore</td><td>null</td></tr></tbody></table></div>"
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
         "arun@gmail.com"
        ],
        [
         102,
         "Bala",
         "Bangalore",
         "bala@gmail.com"
        ],
        [
         103,
         "Karthik",
         "Mumbai",
         null
        ],
        [
         104,
         "Divya",
         "Chennai",
         "divya@gmail.com"
        ],
        [
         105,
         "Manoj",
         "Hyderabad",
         "manoj@gmail.com"
        ],
        [
         101,
         "Arun Kumar",
         "Chennai",
         "arunkumar@gmail.com"
        ],
        [
         106,
         "Ravi",
         "Coimbatore",
         null
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
        }
       ],
       "type": "table"
      }
     },
     "output_type": "display_data"
    },
    {
     "output_type": "stream",
     "name": "stdout",
     "output_type": "stream",
     "text": [
      "\n========== TRANSACTIONS ==========\nTotal Records: 10\nColumns: ['transaction_id', 'customer_id', 'amount', 'status']\n\nSchema:\nroot\n |-- transaction_id: integer (nullable = true)\n |-- customer_id: integer (nullable = true)\n |-- amount: integer (nullable = true)\n |-- status: string (nullable = true)\n\n\nNULL Count:\n"
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
       "</style><div class='table-result-container'><table class='table-result'><thead style='background-color: white'><tr><th>transaction_id</th><th>customer_id</th><th>amount</th><th>status</th></tr></thead><tbody><tr><td>0</td><td>0</td><td>0</td><td>0</td></tr></tbody></table></div>"
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
         "name": "transaction_id",
         "type": "\"long\""
        },
        {
         "metadata": "{}",
         "name": "customer_id",
         "type": "\"long\""
        },
        {
         "metadata": "{}",
         "name": "amount",
         "type": "\"long\""
        },
        {
         "metadata": "{}",
         "name": "status",
         "type": "\"long\""
        }
       ],
       "type": "table"
      }
     },
     "output_type": "display_data"
    },
    {
     "output_type": "stream",
     "name": "stdout",
     "output_type": "stream",
     "text": [
      "Duplicate Records: 1\n"
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
       "</style><div class='table-result-container'><table class='table-result'><thead style='background-color: white'><tr><th>transaction_id</th><th>customer_id</th><th>amount</th><th>status</th></tr></thead><tbody><tr><td>5001</td><td>101</td><td>2500</td><td>SUCCESS</td></tr><tr><td>5002</td><td>102</td><td>1500</td><td>SUCCESS</td></tr><tr><td>5003</td><td>103</td><td>3000</td><td>FAILED</td></tr><tr><td>5004</td><td>101</td><td>4500</td><td>SUCCESS</td></tr><tr><td>5005</td><td>104</td><td>2000</td><td>SUCCESS</td></tr><tr><td>5006</td><td>105</td><td>3500</td><td>PENDING</td></tr><tr><td>5007</td><td>106</td><td>-500</td><td>SUCCESS</td></tr><tr><td>5008</td><td>101</td><td>2500</td><td>SUCCESS</td></tr><tr><td>5008</td><td>101</td><td>2500</td><td>SUCCESS</td></tr><tr><td>5009</td><td>999</td><td>1000</td><td>SUCCESS</td></tr></tbody></table></div>"
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
         5001,
         101,
         2500,
         "SUCCESS"
        ],
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
         5004,
         101,
         4500,
         "SUCCESS"
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
        ],
        [
         5007,
         106,
         -500,
         "SUCCESS"
        ],
        [
         5008,
         101,
         2500,
         "SUCCESS"
        ],
        [
         5008,
         101,
         2500,
         "SUCCESS"
        ],
        [
         5009,
         999,
         1000,
         "SUCCESS"
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
    "from pyspark.sql import functions as F\n",
    "\n",
    "def validate_source(df, table_name):\n",
    "\n",
    "    print(f\"\\n========== {table_name} ==========\")\n",
    "\n",
    "    # Total records\n",
    "    total_records = df.count()\n",
    "    print(\"Total Records:\", total_records)\n",
    "\n",
    "    # Column names\n",
    "    print(\"Columns:\", df.columns)\n",
    "\n",
    "    # Data types\n",
    "    print(\"\\nSchema:\")\n",
    "    df.printSchema()\n",
    "\n",
    "    # NULL validation\n",
    "    print(\"\\nNULL Count:\")\n",
    "    null_counts = df.select([\n",
    "        F.count(\n",
    "            F.when(F.col(c).isNull(), c)\n",
    "        ).alias(c)\n",
    "        for c in df.columns\n",
    "    ])\n",
    "\n",
    "    display(null_counts)\n",
    "\n",
    "    # Duplicate validation\n",
    "    duplicate_count = total_records - df.dropDuplicates().count()\n",
    "    print(\"Duplicate Records:\", duplicate_count)\n",
    "\n",
    "    # Display records\n",
    "    display(df)\n",
    "\n",
    "\n",
    "# Validate both source datasets\n",
    "validate_source(df_customers, \"CUSTOMERS\")\n",
    "validate_source(df_transactions, \"TRANSACTIONS\")\n"
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
     "finishTime": 1795444926702,
     "inputWidgets": {},
     "nuid": "3737138b-0aba-43bd-99f4-4934167f9a7b",
     "showTitle": false,
     "startTime": 1795444924711,
     "submitTime": 1795444924645,
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
       "</style><div class='table-result-container'><table class='table-result'><thead style='background-color: white'><tr><th>customer_id</th><th>count</th></tr></thead><tbody><tr><td>101</td><td>2</td></tr></tbody></table></div>"
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
         "name": "count",
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
    "from pyspark.sql import functions as F\n",
    "\n",
    "# Find duplicate customer IDs\n",
    "\n",
    "duplicate_customers = (\n",
    "    df_customers\n",
    "    .groupBy(\"customer_id\")\n",
    "    .count()\n",
    "    .filter(F.col(\"count\") > 1)\n",
    ")\n",
    "\n",
    "display(duplicate_customers)"
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
     "finishTime": 1795444942017,
     "inputWidgets": {},
     "nuid": "334f3ccd-3cda-412a-bcfd-c6d8e7dfd993",
     "showTitle": false,
     "startTime": 1795444940307,
     "submitTime": 1795444940270,
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
       "</style><div class='table-result-container'><table class='table-result'><thead style='background-color: white'><tr><th>transaction_id</th><th>count</th></tr></thead><tbody><tr><td>5008</td><td>2</td></tr></tbody></table></div>"
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
         "name": "count",
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
    "duplicate_transactions = (\n",
    "    df_transactions\n",
    "    .groupBy(\"transaction_id\")\n",
    "    .count()\n",
    "    .filter(F.col(\"count\") > 1)\n",
    ")\n",
    "\n",
    "display(duplicate_transactions)"
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
     "finishTime": 1795444969271,
     "inputWidgets": {},
     "nuid": "e7f4dc80-5ec9-4da0-b268-67da365a39b9",
     "showTitle": false,
     "startTime": 1795444967736,
     "submitTime": 1795444967689,
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
       "</style><div class='table-result-container'><table class='table-result'><thead style='background-color: white'><tr><th>transaction_id</th><th>customer_id</th><th>amount</th><th>status</th></tr></thead><tbody><tr><td>5007</td><td>106</td><td>-500</td><td>SUCCESS</td></tr></tbody></table></div>"
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
         "SUCCESS"
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
    "negative_transactions = (\n",
    "    df_transactions\n",
    "    .filter(F.col(\"amount\") < 0)\n",
    ")\n",
    "\n",
    "display(negative_transactions)"
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
     "finishTime": 1795444991475,
     "inputWidgets": {},
     "nuid": "2086f2ec-bad7-4327-b027-8bc2ddf08a8d",
     "showTitle": false,
     "startTime": 1795444989538,
     "submitTime": 1795444989494,
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
       "</style><div class='table-result-container'><table class='table-result'><thead style='background-color: white'><tr><th>customer_id</th><th>transaction_id</th><th>amount</th><th>status</th></tr></thead><tbody><tr><td>999</td><td>5009</td><td>1000</td><td>SUCCESS</td></tr></tbody></table></div>"
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
         "SUCCESS"
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
        }
       ],
       "type": "table"
      }
     },
     "output_type": "display_data"
    }
   ],
   "source": [
    "orphan_transactions = (\n",
    "    df_transactions\n",
    "    .join(\n",
    "        df_customers.select(\"customer_id\"),\n",
    "        on=\"customer_id\",\n",
    "        how=\"left_anti\"\n",
    "    )\n",
    ")\n",
    "\n",
    "display(orphan_transactions)"
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
     "finishTime": 1795445222011,
     "inputWidgets": {},
     "nuid": "3b8110d5-4394-4463-9e7d-5d734050d42e",
     "showTitle": false,
     "startTime": 1795445217289,
     "submitTime": 1795445217234,
     "tableResultSettingsMap": {
      "0": {
       "dataGridStateBlob": "{\"version\":1,\"tableState\":{\"columnPinning\":{\"left\":[\"#row_number#\"],\"right\":[]},\"columnSizing\":{\"batch_id\":299},\"columnVisibility\":{},\"sorting\":[]},\"settings\":{\"columns\":{}},\"syncTimestamp\":1795445250121}",
       "filterBlob": null,
       "queryPlanFiltersBlob": null,
       "tableResultIndex": 0
      },
      "1": {
       "dataGridStateBlob": "{\"version\":1,\"tableState\":{\"columnPinning\":{\"left\":[\"#row_number#\"],\"right\":[]},\"columnSizing\":{\"batch_id\":379},\"columnVisibility\":{},\"sorting\":[]},\"settings\":{\"columns\":{}},\"syncTimestamp\":1795445246296}",
       "filterBlob": null,
       "queryPlanFiltersBlob": null,
       "tableResultIndex": 1
      }
     },
     "title": ""
    }
   },
   "outputs": [
    {
     "output_type": "stream",
     "name": "stdout",
     "output_type": "stream",
     "text": [
      "Current Batch ID: c8b21336-93bc-4791-9ccc-60108710521f\nCustomers Bronze records: 7\nTransactions Bronze records: 10\n"
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
       "</style><div class='table-result-container'><table class='table-result'><thead style='background-color: white'><tr><th>customer_id</th><th>customer_name</th><th>city</th><th>email</th><th>ingestion_timestamp</th><th>source_file_name</th><th>batch_id</th></tr></thead><tbody><tr><td>101</td><td>Arun</td><td>Chennai</td><td>arun@gmail.com</td><td>2026-11-23T14:46:59.718Z</td><td>customers.csv</td><td>c8b21336-93bc-4791-9ccc-60108710521f</td></tr><tr><td>102</td><td>Bala</td><td>Bangalore</td><td>bala@gmail.com</td><td>2026-11-23T14:46:59.718Z</td><td>customers.csv</td><td>c8b21336-93bc-4791-9ccc-60108710521f</td></tr><tr><td>103</td><td>Karthik</td><td>Mumbai</td><td>null</td><td>2026-11-23T14:46:59.718Z</td><td>customers.csv</td><td>c8b21336-93bc-4791-9ccc-60108710521f</td></tr><tr><td>104</td><td>Divya</td><td>Chennai</td><td>divya@gmail.com</td><td>2026-11-23T14:46:59.718Z</td><td>customers.csv</td><td>c8b21336-93bc-4791-9ccc-60108710521f</td></tr><tr><td>105</td><td>Manoj</td><td>Hyderabad</td><td>manoj@gmail.com</td><td>2026-11-23T14:46:59.718Z</td><td>customers.csv</td><td>c8b21336-93bc-4791-9ccc-60108710521f</td></tr><tr><td>101</td><td>Arun Kumar</td><td>Chennai</td><td>arunkumar@gmail.com</td><td>2026-11-23T14:46:59.718Z</td><td>customers.csv</td><td>c8b21336-93bc-4791-9ccc-60108710521f</td></tr><tr><td>106</td><td>Ravi</td><td>Coimbatore</td><td>null</td><td>2026-11-23T14:46:59.718Z</td><td>customers.csv</td><td>c8b21336-93bc-4791-9ccc-60108710521f</td></tr></tbody></table></div>"
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
         "2026-11-23T14:46:59.718Z",
         "customers.csv",
         "c8b21336-93bc-4791-9ccc-60108710521f"
        ],
        [
         102,
         "Bala",
         "Bangalore",
         "bala@gmail.com",
         "2026-11-23T14:46:59.718Z",
         "customers.csv",
         "c8b21336-93bc-4791-9ccc-60108710521f"
        ],
        [
         103,
         "Karthik",
         "Mumbai",
         null,
         "2026-11-23T14:46:59.718Z",
         "customers.csv",
         "c8b21336-93bc-4791-9ccc-60108710521f"
        ],
        [
         104,
         "Divya",
         "Chennai",
         "divya@gmail.com",
         "2026-11-23T14:46:59.718Z",
         "customers.csv",
         "c8b21336-93bc-4791-9ccc-60108710521f"
        ],
        [
         105,
         "Manoj",
         "Hyderabad",
         "manoj@gmail.com",
         "2026-11-23T14:46:59.718Z",
         "customers.csv",
         "c8b21336-93bc-4791-9ccc-60108710521f"
        ],
        [
         101,
         "Arun Kumar",
         "Chennai",
         "arunkumar@gmail.com",
         "2026-11-23T14:46:59.718Z",
         "customers.csv",
         "c8b21336-93bc-4791-9ccc-60108710521f"
        ],
        [
         106,
         "Ravi",
         "Coimbatore",
         null,
         "2026-11-23T14:46:59.718Z",
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
       "</style><div class='table-result-container'><table class='table-result'><thead style='background-color: white'><tr><th>transaction_id</th><th>customer_id</th><th>amount</th><th>status</th><th>ingestion_timestamp</th><th>source_file_name</th><th>batch_id</th></tr></thead><tbody><tr><td>5001</td><td>101</td><td>2500</td><td>SUCCESS</td><td>2026-11-23T14:47:00.871Z</td><td>transactions.csv</td><td>c8b21336-93bc-4791-9ccc-60108710521f</td></tr><tr><td>5002</td><td>102</td><td>1500</td><td>SUCCESS</td><td>2026-11-23T14:47:00.871Z</td><td>transactions.csv</td><td>c8b21336-93bc-4791-9ccc-60108710521f</td></tr><tr><td>5003</td><td>103</td><td>3000</td><td>FAILED</td><td>2026-11-23T14:47:00.871Z</td><td>transactions.csv</td><td>c8b21336-93bc-4791-9ccc-60108710521f</td></tr><tr><td>5004</td><td>101</td><td>4500</td><td>SUCCESS</td><td>2026-11-23T14:47:00.871Z</td><td>transactions.csv</td><td>c8b21336-93bc-4791-9ccc-60108710521f</td></tr><tr><td>5005</td><td>104</td><td>2000</td><td>SUCCESS</td><td>2026-11-23T14:47:00.871Z</td><td>transactions.csv</td><td>c8b21336-93bc-4791-9ccc-60108710521f</td></tr><tr><td>5006</td><td>105</td><td>3500</td><td>PENDING</td><td>2026-11-23T14:47:00.871Z</td><td>transactions.csv</td><td>c8b21336-93bc-4791-9ccc-60108710521f</td></tr><tr><td>5007</td><td>106</td><td>-500</td><td>SUCCESS</td><td>2026-11-23T14:47:00.871Z</td><td>transactions.csv</td><td>c8b21336-93bc-4791-9ccc-60108710521f</td></tr><tr><td>5008</td><td>101</td><td>2500</td><td>SUCCESS</td><td>2026-11-23T14:47:00.871Z</td><td>transactions.csv</td><td>c8b21336-93bc-4791-9ccc-60108710521f</td></tr><tr><td>5008</td><td>101</td><td>2500</td><td>SUCCESS</td><td>2026-11-23T14:47:00.871Z</td><td>transactions.csv</td><td>c8b21336-93bc-4791-9ccc-60108710521f</td></tr><tr><td>5009</td><td>999</td><td>1000</td><td>SUCCESS</td><td>2026-11-23T14:47:00.871Z</td><td>transactions.csv</td><td>c8b21336-93bc-4791-9ccc-60108710521f</td></tr></tbody></table></div>"
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
         5001,
         101,
         2500,
         "SUCCESS",
         "2026-11-23T14:47:00.871Z",
         "transactions.csv",
         "c8b21336-93bc-4791-9ccc-60108710521f"
        ],
        [
         5002,
         102,
         1500,
         "SUCCESS",
         "2026-11-23T14:47:00.871Z",
         "transactions.csv",
         "c8b21336-93bc-4791-9ccc-60108710521f"
        ],
        [
         5003,
         103,
         3000,
         "FAILED",
         "2026-11-23T14:47:00.871Z",
         "transactions.csv",
         "c8b21336-93bc-4791-9ccc-60108710521f"
        ],
        [
         5004,
         101,
         4500,
         "SUCCESS",
         "2026-11-23T14:47:00.871Z",
         "transactions.csv",
         "c8b21336-93bc-4791-9ccc-60108710521f"
        ],
        [
         5005,
         104,
         2000,
         "SUCCESS",
         "2026-11-23T14:47:00.871Z",
         "transactions.csv",
         "c8b21336-93bc-4791-9ccc-60108710521f"
        ],
        [
         5006,
         105,
         3500,
         "PENDING",
         "2026-11-23T14:47:00.871Z",
         "transactions.csv",
         "c8b21336-93bc-4791-9ccc-60108710521f"
        ],
        [
         5007,
         106,
         -500,
         "SUCCESS",
         "2026-11-23T14:47:00.871Z",
         "transactions.csv",
         "c8b21336-93bc-4791-9ccc-60108710521f"
        ],
        [
         5008,
         101,
         2500,
         "SUCCESS",
         "2026-11-23T14:47:00.871Z",
         "transactions.csv",
         "c8b21336-93bc-4791-9ccc-60108710521f"
        ],
        [
         5008,
         101,
         2500,
         "SUCCESS",
         "2026-11-23T14:47:00.871Z",
         "transactions.csv",
         "c8b21336-93bc-4791-9ccc-60108710521f"
        ],
        [
         5009,
         999,
         1000,
         "SUCCESS",
         "2026-11-23T14:47:00.871Z",
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
    "# Step 10: Prepare Bronze DataFrames\n",
    "\n",
    "from pyspark.sql import functions as F\n",
    "import uuid\n",
    "\n",
    "# Generate one batch ID for this pipeline execution\n",
    "batch_id = str(uuid.uuid4())\n",
    "\n",
    "print(\"Current Batch ID:\", batch_id)\n",
    "\n",
    "# Add metadata to customers\n",
    "df_customers_bronze = (\n",
    "    df_customers\n",
    "    .withColumn(\"ingestion_timestamp\", F.current_timestamp())\n",
    "    .withColumn(\"source_file_name\", F.lit(\"customers.csv\"))\n",
    "    .withColumn(\"batch_id\", F.lit(batch_id))\n",
    ")\n",
    "\n",
    "# Add metadata to transactions\n",
    "df_transactions_bronze = (\n",
    "    df_transactions\n",
    "    .withColumn(\"ingestion_timestamp\", F.current_timestamp())\n",
    "    .withColumn(\"source_file_name\", F.lit(\"transactions.csv\"))\n",
    "    .withColumn(\"batch_id\", F.lit(batch_id))\n",
    ")\n",
    "\n",
    "print(\"Customers Bronze records:\", df_customers_bronze.count())\n",
    "print(\"Transactions Bronze records:\", df_transactions_bronze.count())\n",
    "\n",
    "display(df_customers_bronze)\n",
    "display(df_transactions_bronze)\n"
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
     "finishTime": 1795445392398,
     "inputWidgets": {},
     "nuid": "8688e830-5500-4f5d-9ae1-dab788959bcc",
     "showTitle": false,
     "startTime": 1795445380854,
     "submitTime": 1795445380802,
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
      "Customers Bronze table created successfully!\n"
     ]
    }
   ],
   "source": [
    "\n",
    "# Step 11.1: Write customers to Bronze Delta table\n",
    "\n",
    "df_customers_bronze.write \\\n",
    "    .format(\"delta\") \\\n",
    "    .mode(\"overwrite\") \\\n",
    "    .saveAsTable(\"customer_data_platform.bronze.customers\")\n",
    "\n",
    "print(\"Customers Bronze table created successfully!\")\n"
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
     "finishTime": 1795445412818,
     "inputWidgets": {},
     "nuid": "25c0eeda-4b3d-4fd7-b369-eda28542def6",
     "showTitle": false,
     "startTime": 1795445408937,
     "submitTime": 1795445408886,
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
      "Transactions Bronze table created successfully!\n"
     ]
    }
   ],
   "source": [
    "\n",
    "# Step 11.2: Write transactions to Bronze Delta table\n",
    "\n",
    "df_transactions_bronze.write \\\n",
    "    .format(\"delta\") \\\n",
    "    .mode(\"overwrite\") \\\n",
    "    .saveAsTable(\"customer_data_platform.bronze.transactions\")\n",
    "\n",
    "print(\"Transactions Bronze table created successfully!\")\n"
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
   "notebookName": "01_bronze_source_validation",
   "widgets": {}
  },
  "language_info": {
   "name": "python"
  }
 },
 "nbformat": 4,
 "nbformat_minor": 0
}