from pyspark.sql.functions import explode, posexplode, col, explode_outer, lit, year, month, dayofmonth
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, ArrayType
from datetime import datetime


def read_json_with_schema(spark, file_path):
    custom_schema = StructType([
        StructField("id", IntegerType(), True),
        StructField("properties", StructType([
            StructField("name", StringType(), True),
            StructField("storeSize", StringType(), True),
        ]), True),
        StructField("employees", ArrayType(StructType([
            StructField("empId", IntegerType(), True),
            StructField("empName", StringType(), True),
        ])), True),
    ])

    return spark.read.json(file_path, schema=custom_schema)


def read_json_without_schema(spark, file_path):
    return spark.read.json(file_path)
# Find record count with and without schema
def flatten_dataframe(df):
    return df.select(col("id"),col("properties.name").alias("company_name"),col("properties.storeSize").alias("store_size"),explode("employees").alias("employee"))

def explode_data(df):
    return df.select("id", explode("data").alias("exploded_data"))
def explode_data(df):
    return df.select("id", explode("employees").alias("exploded_employee")).\
           df.select("id", explode_outer("employees").alias("exploded_outer_employee")).\
           df.select("id", posexplode("employees").alias("pos_employee"))
def convert_to_snake_case(df):
    return df.toDF(*(col_name.lower() for col_name in df.columns))
def load_date_column(df):
    return df.withColumn("load_date", lit(datetime.now().date()))
def date_parts_columns(df):
    return (df.withColumn("year", year("load_date"))
            .withColumn("month", month("load_date"))
            .withColumn("day", dayofmonth("load_date")))
    def filter_data_by_id(df, target_id):
        return df.filter(f"id == '{target_id}'")
