from pyspark.sql import SparkSession
from src.assignment_5.util import *

spark = SparkSession.builder.appName("assignment_5").getOrCreate()

# Define schemas dynamically
employee_schema = employee_schema()
department_schema = department_schema()
country_schema = country_schema()

#DataFrames using the defined schemas
employee_df, department_df, country_df = create_dataframes(spark, employee_schema, department_schema, country_schema)
show_dataframes(employee_df, department_df, country_df)

#Find the average salary of each department
avg_salary_department = avg_salary(employee_df)
avg_salary_department.show()

#Find the employee name and department name whose name starts with 'm'
employees_start_with_m = employees_start_with_m(employee_df)
employees_start_with_m.show()

#Create another new column 'bonus' by multiplying employee salary * 2
employee_df = add_bonus_column(employee_df)
employee_df.show()

#Reorder the column names of employee_df
employee_df = reorder_columns(employee_df)
employee_df.show()

#Perform Inner, Left, and Right joins dynamically
inner_join, left_join, right_join = perform_joins(employee_df, department_df)
inner_join.show()
left_join.show()
right_join.show()

#Derive a new DataFrame with 'country_name' instead of 'State' in employee_df
new_employee_df = new_dataframe(employee_df, country_df)
new_employee_df.show()

#Convert all column names into lowercase and add a 'load_date' column with the current date
final_df = convert_columns(new_employee_df)
final_df.show()

#Create external tables with Parquet and CSV formats
external_tables(table_df)
