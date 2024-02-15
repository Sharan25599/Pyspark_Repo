from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, date_format, current_date, expr
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, TimestampType


spark = SparkSession.builder.appName("UserLoginDetails").getOrCreate()

# create Data Frame with custom schema creation without using Struct Type and Struct Field
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

# Column names should be log_id, user_id, user_activity, time_stamp using dynamic function
user_login_df=spark.createDataFrame(data,columns)

# Write a query to calculate the number of actions performed by each user in the last 7 days
df_filtered = user_login_df.filter(col("time_stamp") >= ('2023-09-15' - expr("INTERVAL 7 DAYS")))
result_df = df_filtered.groupBy("user_id").agg(count("log_id").alias("actions_last_7_days"))

# Convert the time stamp column to login_date column with yyyy-MM-dd format with date type as its data type
convert_df = user_login_df.withColumn("login_date", date_format("time_stamp", "yyyy-MM-dd"))

# Write the data frame as csv file with different write options expect(merge condition)
user_login_df.write.csv(r"C:\Users\SharanKumar\Desktop\csv assign",mode='overwrite')
user_login_df.write.csv(r"C:\Users\SharanKumar\Desktop\csv assign",mode='append')
user_login_df.write.format("csv").mode("overwrite").partitionBy("month")

# Write it as managed table with Database name as user and table name as login_details with overwrite mode.
user_login_df.write.mode("overwrite").saveAsTable("user.login_details")
