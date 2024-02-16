from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType
from utils import *

spark = SparkSession.builder.appName("assignment_1").getOrCreate()

# Define schema for purchase_data_df
purchase_data_schema = StructType([
    StructField("customer", IntegerType(), True),
    StructField("product_model", StringType(), True)
])

# Define schema for product_data_df
product_data_schema = StructType([
    StructField("product_model", StringType(), True)
])

# Define data for purchase_data_df
purchase_data = [
    (1, "A"),
    (1, "B"),
    (2, "A"),
    (2, "B"),
    (3, "A"),
    (3, "B"),
    (1, "C"),
    (1, "D"),
    (1, "E"),
    (3, "E"),
    (4, "A")
]

# Define data for product_data_df
product_data = [
    ("A",),
    ("B",),
    ("C",),
    ("D",),
    ("E",)
]

# Create DataFrames
purchase_data_df = spark.createDataFrame(purchase_data, schema=purchase_data_schema)
product_data_df = spark.createDataFrame(product_data, schema=product_data_schema)

# Show the DataFrames
purchase_data_df.show()
product_data_df.show()

#Find the customers who have bought only product A
bought_only_A(purchase_data_df).show()

#Find customers who upgraded from product B to product E
upgraded_B_to_E(purchase_data_df).show()

#Find customers who have bought all models in the new Product Data
bought_all_models(purchase_data_df, product_data_df).show()


