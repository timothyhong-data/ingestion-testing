# Databricks notebook source
dbutils.library.restartPython()

# COMMAND ----------

from src.deployment.schema import (bronze_turbines_schema, 
                                   silver_turbines_schema,
                                   error_log_schema,
                                   gold_turbines_schema
                                   )
from delta.tables import DeltaTable
from typing import Dict

# COMMAND ----------

# MAGIC %sql
# MAGIC USE CATALOG `ingestion-testing`;

# COMMAND ----------

# List of schemas (Database)
catalog_schemas = ['raw', 'bronze', 'silver', 'gold']

# COMMAND ----------

# Dictionary mapping table names to their corresponding schemas
table_schema_pairs = {'bronze.bronze_turbines': bronze_turbines_schema,
                      'silver.silver_turbines': silver_turbines_schema,
                      'silver.error_log': error_log_schema,
                      'gold.gold_turbines': gold_turbines_schema
                      }

# COMMAND ----------

# List of volumes
volumes = ['landing', 'processed']

# COMMAND ----------

# Creating schemas (database)
for schema in catalog_schemas: 
    # spark.sql(f'DROP SCHEMA IF EXISTS {schema}')
    location = f'abfss://ingestiontesting03@ingestiontesting03.dfs.core.windows.net/{schema}'
    spark.sql(f'CREATE SCHEMA IF NOT EXISTS {schema} MANAGED LOCATION "{location}"')

# COMMAND ----------

# Creating volume for data storage
for volume in volumes: 
    # spark.sql(f'DROP VOLUME IF EXISTS bronze.{volume}')
    location = f"abfss://ingestiontesting03@ingestiontesting03.dfs.core.windows.net/raw/{volume}"
    spark.sql(f'CREATE EXTERNAL VOLUME IF NOT EXISTS raw.{volume} LOCATION "{location}"')

# COMMAND ----------

# Creating tables with their correspoinding schemas
for table_name, schema in table_schema_pairs.items():
    try:
        # Check if the table exists using Spark catalog
        if spark.catalog._jcatalog.tableExists(table_name):
            print(f"Table {table_name} already exists.")
        else:
            print(f"Creating table {table_name}.")
            spark.catalog.createTable(table_name, schema=schema)
            print(f"Table {table_name} created.")
    except Exception as e:
        # Handle unexpected exceptions
        print(f"Error processing table {table_name}: {e}")

# COMMAND ----------

df = spark.read.table("bronze.bronze_turbines")
df.write.mode("overwrite").option("overwriteSchema", "true").clusterBy("turbine_id", "timestamp").saveAsTable("bronze.bronze_turbines")

# COMMAND ----------

df = spark.read.table("silver.silver_turbines")
df.write.mode("overwrite").option("overwriteSchema", "true").clusterBy("turbine_id", "timestamp").saveAsTable("silver.silver_turbines")

# COMMAND ----------

df = spark.read.table("gold.gold_turbines")
df.write.mode("overwrite").option("overwriteSchema", "true").clusterBy("date", "turbine_id").saveAsTable("gold.gold_turbines")

# COMMAND ----------

# MAGIC %sql
# MAGIC OPTIMIZE bronze.bronze_turbines FULL;
# MAGIC OPTIMIZE silver.silver_turbines FULL;
# MAGIC OPTIMIZE gold.gold_turbines FULL;
