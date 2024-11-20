from pyspark.sql import DataFrame
from pyspark.sql import SparkSession
from pyspark.sql.functions import lit
from delta.tables import DeltaTable
from typing import List

spark = SparkSession.builder.getOrCreate()

def load_file_to_df(
        file_path: str, 
        file_format: str = "csv", 
        has_header: bool = True) -> DataFrame:

    return (
        spark.read.format(file_format)
        .option("header", str(has_header).lower())
        .load(file_path)
    )

def add_load_datetime(df: DataFrame, 
                      load_datetime: str) -> DataFrame:

    df = df.withColumn('load_datetime', lit(load_datetime))

    return df

def merge_update_insert(source_df: DataFrame, 
                        target_table: str, 
                        columns: List):
    # Build the condition string dynamically based on the column list
    condition = " AND ".join([f"target.{col} = source.{col}" for col in columns])
    # Dynamically create a dictionary to update all columns
    set_updates = {f"target.{col}": f"source.{col}" for col in columns}

    target_table = DeltaTable.forName(spark, target_table)

    target_table.alias("target").merge(
        source_df.alias("source"),
        condition
    ).whenMatchedUpdate(
        set=set_updates 
    ).whenNotMatchedInsertAll(
    ).execute()