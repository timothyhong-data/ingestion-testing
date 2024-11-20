# Databricks notebook source
dbutils.library.restartPython()

# COMMAND ----------

from pyspark.sql.functions import (to_date, avg, min, max, stddev, 
                                   when, col, lit, round)
from pyspark.sql import DataFrame
from pyspark.sql.types import StructType
from src.deployment.schema import gold_turbines_schema
from delta.tables import DeltaTable
from src.utils.transformation_helper import (get_ingested_data,
                                             cast_dataframe_to_schema)
from src.utils.ingestion_helper import merge_update_insert

# COMMAND ----------

# MAGIC %sql
# MAGIC USE CATALOG `ingestion-testing`;
# MAGIC USE SCHEMA gold

# COMMAND ----------

def aggregate_power_data(df: DataFrame) -> DataFrame:
    aggregated_df = (
        unprocessed_silver_df
        .groupBy("turbine_id", "date")
        .agg(
            round(avg("power_output"), 2).alias("avg_power_output"),
            min("power_output").alias("min_power_output"),
            max("power_output").alias("max_power_output"),
            stddev("power_output").alias("stddev_power_output")
        )
        .withColumn(
            "is_anomaly",
            when(
                (col("max_power_output") > col("avg_power_output") + 2 * col("stddev_power_output")) |
                (col("min_power_output") < col("avg_power_output") - 2 * col("stddev_power_output")),
                lit(True)
            ).otherwise(lit(False))
        )
        .drop(col('stddev_power_output'))
    )
    return aggregated_df


# COMMAND ----------

# Degine variables
# List of columns to get ingested data
join_columns = [('turbine_id', 'turbine_id'), ('date', 'date')]
# List of merge columns to join on
columns_to_merge = ["turbine_id", "date"]  

# COMMAND ----------

silver_df = spark.read.table('silver.silver_turbines').withColumn("date", to_date("timestamp"))
gold_df = spark.read.table('gold_turbines')

# COMMAND ----------

unprocessed_silver_df = get_ingested_data(silver_df, gold_df, join_columns, 'left_anti')
aggregated_gold_df = aggregate_power_data(unprocessed_silver_df)
ingested_gold_df = cast_dataframe_to_schema(aggregated_gold_df, gold_turbines_schema)

# COMMAND ----------

try:
    merge_update_insert(ingested_gold_df,
                        "gold_turbines",
                        columns_to_merge)
except Exception as e:
    print(f"An error occurred during the process: {e}")
