from pyspark.sql import SparkSession
from src.assignment_3.utils import *


spark = SparkSession.builder.appName("assignment_3").getOrCreate()

# Create Data Frame with custom schema creation without using Struct Type and Struct Field
data = [
    (1, 101, 'login', '2023-09-05 08:30:00'),
    (2, 102, 'click', '2023-09-06 12:45:00'),
    (3, 101, 'click', '2023-09-07 14:15:00'),
    (4, 103, 'login', '2023-09-08 09:00:00'),
    (5, 102, 'logout', '2023-09-09 17:30:00'),
    (6, 101, 'click', '2023-09-10 11:20:00'),
    (7, 103, 'click', '2023-09-11 10:15:00'),
    (8, 102, 'click', '2023-09-12 13:10:00')]
columns = ["log_id", "user_id", "user_activity", "time_stamp"]

def user_login_dataframe(spark, data, columns):
    return user_login_df

user_login_df = user_login_dataframe(spark, data, columns)

# Write a query to calculate the number of actions performed by each user in the last 7 days
result_df = actions_last_7_days(user_login_df)

# Convert the time stamp column to login_date column with yyyy-MM-dd format with date type as its data type
converted_df = timestamp(user_login_df)

# Write the data frame as csv file with different write options except (merge condition)
write_to_csv(user_login_df, r"C:\Users\SharanKumar\Desktop\csv_assign", mode='overwrite')
write_to_csv(user_login_df, r"C:\Users\SharanKumar\Desktop\csv_assign", mode='append')
user_login_df.write.format("csv").mode("overwrite").partitionBy("month")  # Note: This line was corrected

# Write it as a managed table with Database name as user and table name as login_details with overwrite mode.
write_as_managed_table(user_login_df, "user", "login_details", mode='overwrite')
