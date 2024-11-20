# Databricks notebook source
dbutils.library.restartPython()

# COMMAND ----------

import os
from datetime import datetime
from pyspark.sql import DataFrame
from pyspark.sql.functions import lit
from delta.tables import DeltaTable
from typing import List
from src.utils.ingestion_helper import (load_file_to_df, 
                                        add_load_datetime)

# COMMAND ----------

# MAGIC %sql
# MAGIC USE CATALOG `ingestion-testing`;
# MAGIC USE SCHEMA bronze;

# COMMAND ----------

def merge_bronze_table(source_df: DataFrame, 
                       target_table: str, 
                       columns: List):
    # Build the condition string dynamically based on the column list
    condition = " AND ".join([f"target.{col} = source.{col}" for col in columns])
    
    # Reference the target table
    bronze_table = DeltaTable.forName(spark, target_table)
    
    # Perform the merge operation
    bronze_table.alias("target").merge(
        source_df.alias("source"),
        condition
    ).whenNotMatchedInsertAll().execute()

# COMMAND ----------

# Defind variables
load_datetime = datetime.now()
load_datetime_folder = datetime.now().strftime('%Y%m%d_%H%M%S')
landing_path = '/Volumes/ingestion-testing/raw/landing/'
processed_path = f'/Volumes/ingestion-testing/raw/processed/{load_datetime_folder}'
# List of merge columns to join on
columns_to_merge = ["turbine_id", "timestamp"]  

# COMMAND ----------

# Loading data from landing
landing_df = load_file_to_df(file_path=landing_path, 
                            file_format="csv",
                            has_header=True)

# Adding load date time
df_with_timestamp = add_load_datetime(landing_df, load_datetime)

# COMMAND ----------

try: 
    merge_bronze_table(df_with_timestamp, 
                    'bronze_turbines', 
                    columns_to_merge)
    if dbutils.fs.ls(landing_path):
        # List all files in the source directory and move them to the target directory
        for file_info in dbutils.fs.ls(landing_path):
            file_name = os.path.basename(file_info.path)
            source_file = os.path.join(landing_path, file_name)
            target_file = os.path.join(processed_path, file_name)
            dbutils.fs.mv(source_file, target_file)
            print(f"Files moved from {source_file} to {target_file} successfully.")
    else:
        print(f"No files found in {landing_path} to move.")
except Exception as e:
    print(f"An error occurred during the process: {e}")
