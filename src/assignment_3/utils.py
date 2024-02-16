from pyspark.sql.functions import *

def actions_last_7_days(spark, df):
    df1 = df.withColumn('timestamp', col('timestamp').cast('timestamp'))
    result = df1.filter((col('timestamp') >= '2023-09-05') & (col('timestamp') <= '2023-09-12')) \
        .groupBy('user_id') \
        .count() \
        .withColumnRenamed('count', 'actions_last_7_days')
    result.show()
    return result

def timestamp(spark, df):
    df2 = df.withColumn('timestamp', col('timestamp').cast('timestamp'))
    df2a = df2.withColumn('login_date', to_date(col('timestamp')).cast('date'))
    df2a.show()
    df2a.write.csv(r"C:\Users\Admin\OneDrive\Desktop\Pyspark Assignment\Output_files\Q3", header=True, mode="overwrite")
    return df2a

def write_as_managed_table(spark, df, table_name):
    spark.sql("CREATE DATABASE IF NOT EXISTS user")
    spark.catalog.setCurrentDatabase("user")
    df.write.mode("overwrite").saveAsTable("login_details")

def query_managed_table(spark, table_name):
    query = "SELECT * FROM login_details "
    spark.sql(query).show()

def process_data(spark, df):
    # Calculate actions performed by each user in the last 7 days
    result = actions_last_7_days(spark, df)

    #Convert timestamp column to login_date column with yyyy-MM-dd format and save as CSV
    transformed_df = timestamp(spark, df)

    #Write DataFrame as a managed table
    write_as_managed_table(spark, df, "user.login_details")

    # Query the managed table in the 'user' database
    query_managed_table(spark, "user.login_details")