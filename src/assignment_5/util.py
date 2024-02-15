from pyspark.sql import SparkSession
from pyspark.sql.functions import avg, col, current_date
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

spark = SparkSession.builder.appName("assignment_5").getOrCreate()

#create all 3 dataframes as employee_df, department_df, country_df with custom schema defined in dynamic way
emp_data = [
    (11, "james", "D101", "ny", 9000, 34),
    (12, "michel", "D101", "ny", 8900, 32),
    (13, "robert", "D102", "ca", 7900, 29),
    (14, "scott", "D103", "ca", 8000, 36),
    (15, "jen", "D102", "ny", 9500, 38),
    (16, "jeff", "D103", "uk", 9100, 35),
    (17, "maria", "D101", "ny", 7900, 40)
]
emp_schema = StructType([
    StructField("employee_id", IntegerType(), True),
    StructField("employee_name", StringType(), True),
    StructField("department", StringType(), True),
    StructField("State", StringType(), True),
    StructField("salary", IntegerType(), True),
    StructField("Age", IntegerType(), True)
])
employee_df = spark.createDataFrame(emp_data,emp_schema)

dept_data = [
    ("D101", "sales"),
    ("D102", "finance"),
    ("D103", "marketing"),
    ("D104", "hr"),
    ("D105", "support")
]
dept_schema = StructType([
    StructField("dept_id", StringType(), True),
    StructField("dept_name", StringType(), True)
])
department_df = spark.createDataFrame(emp_data,dept_schema)

country_data = [
    ("ny", "newyork"),
    ("ca", "California"),
    ("uk", "Russia")
]
country_schema = StructType([
    StructField("country_code", StringType(), True),
    StructField("country_name", StringType(), True)
])
country_df = spark.createDataFrame(country_data, country_schema)


# Find average salary each department
avg_salary_df = employee_df.groupBy("department").agg(avg("salary").alias("avg_salary"))


#Find the employee name and department name whose name starts with ‘m’
join_df=employee_df.join(department_df,on='dept_id',how='inner')
starts_with_m = join_df.filter(employee_df["employee_name"].like("m%"),department_df["dept_name"].like("m%"))


# create another new column in  employee_df as bonus by multiplying employee salary *2
employee_df = employee_df.withColumn("bonus", employee_df["salary"] * 2)

# Reorder the column names of employee_df columns  as (employee_id,employee_name,salary,State,Age,department)
change_column_df = employee_df.select( ["employee_id", "employee_name", "salary", "State", "Age", "department"])


#Give the result of inner join, left join, right join when joining employee_df with department_df in dynamic way
join_types = ["inner", "left", "right"]
for join_type in join_types:
    join_df = employee_df.join(department_df, on="department", how=join_type)


# # derive a new dataframe with country_name instead of State in employee_df
new_df = employee_df.join(country_df, employee_df.State == country_df.country_code, "left").drop("State").withColumnRenamed("country_name", "State")

# convert all the column names into lower case from the result of question 7 in dynamic way, add load_date column with current date
def add_load_date(new_employee_df):
    lowercase_columns = [col(column).alias(column.lower()) for column in new_employee_df.columns]
    return new_employee_df.select(lowercase_columns).withColumn("load_date", current_date())



#write the parquet and csv as a table
employee_df.df.write.format("parquet").mode("overwrite").saveAsTable("parquet_table")
employee_df.write.format("csv").mode("overwrite").option("header", "true").saveAsTable("csv_table")













