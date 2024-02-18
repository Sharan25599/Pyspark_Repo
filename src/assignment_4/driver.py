from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, explode_outer, posexplode, lit
from src.assignment_4.util import *

spark = SparkSession.builder.appName("assignment_4").getOrCreate()
df_with_schema = read_json_with_schema(spark, r"C:\Users\SharanKumar\Desktop\JSON files\Nested_json_file.json")

record_count_with_schema = df_with_schema.count()

df_without_schema = read_json_without_schema(spark, r"C:\Users\SharanKumar\Desktop\JSON files\Nested_json_file.json")
record_count_without_schema = df_without_schema.count()

# Flatten the DataFrame with custom schema
df_flattened = flatten_dataframe(df_with_schema)

# Find record count with and without flattening
record_count_flattened = df_flattened.count()
record_count_not_flattened = df_with_schema.count()

# explode, explode_outer, and posexplode
df_explode = df_with_schema.select("id", explode("employees").alias("exploded_employee"))
df_explode_outer = df_with_schema.select("id", explode_outer("employees").alias("exploded_outer_employee"))
df_posexplode = df_with_schema.select("id", posexplode("employees").alias("pos", "employee"))

# Convert column names from camel case to snake case
df_snake_case = convert_to_snake_case(df_flattened)

# Add a new column named load_date with the current date
df_with_load_date = load_date_column(df_snake_case)

# Create year, month, and day columns from load_date
df_with_date_parts = date_parts_columns(df_with_load_date)

# Write DataFrame to a table
df_with_date_parts.write.mode("overwrite").format("json").partitionBy("year", "month", "day").saveAsTable("employee.employee_details")

# Read the json file
json_df = read_json_without_schema(spark, r"C:\Users\SharanKumar\Desktop\JSON files\Nested_json_file.json")

# Explode nested data
nested_column = "employees"
exploded_json_df = explode_data(json_df, nested_column)

# Filter the DataFrame based on a condition
filter_column_name = "exploded_column.id"
condition_value = "0001"
filtered_json_df = json_df.filter(exploded_json_df, filter_column_name, condition_value)
filtered_json_df.show()
