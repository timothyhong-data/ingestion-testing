from pyspark.sql.types import *

bronze_turbines_schema = StructType(
    [
        StructField("load_datetime", TimestampType(), False),
        StructField("timestamp", StringType(), False),
        StructField("turbine_id", StringType(), False),
        StructField("wind_speed", StringType(), True),
        StructField("wind_direction", StringType(), True),
        StructField("power_output", StringType(), True)
    ]
)

error_log_schema = StructType(
    [
        StructField("load_datetime", TimestampType(), False),
        StructField("timestamp", StringType(), False),
        StructField("turbine_id", StringType(), False),
        StructField("wind_speed", StringType(), True),
        StructField("wind_direction", StringType(), True),
        StructField("power_output", StringType(), True)
    ]
)

silver_turbines_schema = StructType(
    [
        StructField("load_datetime", TimestampType(), False),
        StructField("timestamp", TimestampType(), False),
        StructField("turbine_id", IntegerType(), False),
        StructField("wind_speed", DoubleType(), True),
        StructField("wind_direction", IntegerType(), True),
        StructField("power_output", DoubleType(), True)
    ]
)

gold_turbines_schema = StructType(
    [
        StructField("turbine_id", IntegerType(), False),
        StructField("date", DateType(), False),
        StructField("avg_power_output", DoubleType(), False),
        StructField("min_power_output", DoubleType(), False),
        StructField("max_power_output", DoubleType(), False),
        StructField("is_anomaly", BooleanType(), False)
    ]
)