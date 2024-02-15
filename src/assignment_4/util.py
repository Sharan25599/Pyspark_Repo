from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, count, col, current_date, year, month, day, explode_outer, posexplode
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, ArrayType
spark = SparkSession.builder.appName("assignment_4").getOrCreate()



custom_schema=StructType([
    StructField("id", IntegerType(), True),
    StructField("properties", StructType([
        StructField("name", StringType(), True),
        StructField("storeSize", StringType(), True), ]), True),
    StructField("employees", ArrayType(
        StructType([
            StructField("empId", IntegerType(), True),
            StructField("empName", StringType(), True), ])), True)
])
# df_json=spark.read.json(r"C:\Users\SharanKumar\Desktop\JSON files\Nested_json_file.json",schema=custom_schema,multiLine=True)
# # df_json.show(truncate=False)

# Read Json file provided in the attachment using dynamic function
def read_json(file_path, custom_schema, multiline=True):
    df_json = spark.read.json(file_path, schema=custom_schema, multiLine=multiline)
    return df_json

custom_schema =custom_schema
file_path = r"C:\Users\SharanKumar\Desktop\JSON files\Nested_json_file.json"
df_json = read_json(file_path, custom_schema)
df_json.show(truncate=False)

# create custom schema and find the record count when schema defined and when schema not defined.
count_schema=df_json.count()

count_without_schema=spark.read.json(r"C:\Users\SharanKumar\Desktop\JSON files\Nested_json_file.json").count()

#flatten the data frame which is custom schema
df_flatten = df_json.withColumn("employee_flatten", explode("employees"))


#  find out the record count when flattened and when its not flattened(find out the difference why you are getting more count)
df_count_flatten=df_flatten.count()

df_json.groupBy('employees').agg(count('*').alias('CountofEmp'))

# Differentiate the difference using explode, explode outer, posexplode functions
exploded_df = df_json.withColumn("explode_employee", explode("employees"))
exploded_outer_df = df_json.withColumn("explode_outer_employee", explode_outer("employees"))
posexploded_df = df_json.withColumn("pos_explode_employee", posexplode("employees"))

# convert the column names from camel case to snake case
convert_df=df_json.toDF(*(col_name.lower() for col_name in df_json.columns))


#Add a new column named load_date with current date
load_date_df = df_json.withColumn("load_date", current_date())

#create 3 new columns as year , month and day from load_date column
date_parts_df = load_date_df.withColumn("year", year(load_date_df.load_date)) \
                            .withColumn("month",month(load_date_df.load_date)) \
                            .withColumn("day", day(load_date_df.load_date))


#write dataframe to a table with Database name as employee and table name as employee_details with overwrite mode, format as json and partition based on (year, month, day)
df_json.write.format("json").mode("overwrite").partitionBy("year", "month", "day").saveAsTable("employee.employee_details")

#  JSON question 2
json_df = spark.read.json(r"C:\Users\SharanKumar\Desktop\JSON files\Nested_json_file.json",multiLine=True)

# Explode the JSON data
exploded_json_df = df_json.withColumn("exploded_column", explode("employees"))

# Filter the id equal to 0001
filter_json_df = exploded_json_df.filter(col("exploded_column.id") == "0001")