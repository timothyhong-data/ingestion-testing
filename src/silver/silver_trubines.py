# Databricks notebook source
dbutils.library.restartPython()

# COMMAND ----------

from pyspark.sql import DataFrame
from pyspark.sql.functions import col
from src.deployment.schema import silver_turbines_schema
from src.utils.transformation_helper import (filter_null_columns,
                                             filter_invalid,
                                             cast_dataframe_to_schema)
from src.utils.ingestion_helper import merge_update_insert

# COMMAND ----------

# MAGIC %sql
# MAGIC USE CATALOG `ingestion-testing`;
# MAGIC USE SCHEMA silver

# COMMAND ----------

ingested_df = spark.sql("""
    SELECT
        load_datetime,
        timestamp,
        turbine_id,
        wind_speed,
        wind_direction,
        power_output
    FROM bronze.bronze_turbines AS bt
    WHERE bt.load_datetime > (
        SELECT 
            COALESCE(MAX(st.load_datetime), '1900-01-01 00:00:00')
        FROM silver_turbines AS st
    )
""")

# COMMAND ----------

# Define variables
# Define columns for null validation
null_validation_columns = ['wind_speed', 'wind_direction', 'power_output']
# Define conditions for filter_invalid function
conditions = (
    (col('wind_speed').between(3, 25)) &
    (col('wind_direction').between(0, 360)) &
    (col('power_output') > 0)
)
# List of merge columns to join on
columns_to_merge = ["turbine_id", "timestamp"]  


# COMMAND ----------

# Filter out rows with null values in specified columns.
df, null_df = filter_null_columns(ingested_df, null_validation_columns)
# Validate the remaining rows against specific conditions.
valid_df, invalid_df = filter_invalid(df, conditions)

# COMMAND ----------

# Combine all invalid rows
error_log_df = null_df.unionByName(invalid_df)
# Cast the valid DataFrame to match the schema of the target Silver table.
ingested_silver_df = cast_dataframe_to_schema(valid_df, silver_turbines_schema)

# COMMAND ----------

# Write to the `error_log` table.
error_log_df.write \
    .mode('append') \
    .saveAsTable("error_log")

# COMMAND ----------

# Perform a merge operation between the ingested data and the existing silver table.
try:
    merge_update_insert(ingested_silver_df,
                        "silver_turbines",
                        columns_to_merge)
except Exception as e:
    print(f"An error occurred during the process: {e}")
