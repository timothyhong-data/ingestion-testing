from pyspark.sql import DataFrame
from pyspark.sql import SparkSession
from typing import List
from pyspark.sql.types import StructType
from pyspark.sql.functions import lit, col

spark = SparkSession.builder.getOrCreate()

def filter_null_columns(df: DataFrame, 
                        columns: List) -> (DataFrame, DataFrame):
    # Start with a condition that is False (it won't filter out anything)
    condition = lit(False) 
    
    # Create a condition for each column to check if it's not null
    for column in columns:
        condition |= df[column].isNotNull()  # Combine with `isNotNull` condition for each column using OR (|)
    df = df.filter(condition)
    null_df = df.filter(~condition)
    # Apply the condition to filter the DataFrame
    return df, null_df
  
def filter_invalid(df: DataFrame, 
                   conditions) -> (DataFrame, DataFrame):
    
    # Filter valid rows and check for nulls
    df = df.filter(conditions)

    # Invalid rows (rows that don't meet the condition)
    invalid_df = df.filter(~conditions)

    return df, invalid_df
  
def cast_dataframe_to_schema(df: DataFrame, 
                             schema: StructType) -> DataFrame:
    for field in schema.fields:
        column_name = field.name
        target_type = field.dataType
        # Check if column exists in the DataFrame
        if column_name in df.columns:
            # Cast the column to the target type
            df = df.withColumn(column_name, df[column_name].cast(target_type))
    return df
  
def get_ingested_data(source_df: DataFrame, 
                    target_df: DataFrame, 
                    join_columns: list, 
                    join_type: str = "left_anti") -> DataFrame:
  # Build the join condition
  join_condition = None
  for silver_col, gold_col in join_columns:
      if join_condition is None:
          join_condition = (source_df[silver_col] == target_df[gold_col])
      else:
          join_condition &= (source_df[silver_col] == target_df[gold_col])
  # Perform the join and return unprocessed rows
  unprocessed_source_df = source_df.join(target_df, join_condition, join_type)
  
  return unprocessed_source_df
