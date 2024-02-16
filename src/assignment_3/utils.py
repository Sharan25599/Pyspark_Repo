from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, date_format, expr, user


def create_spark_session(app_name="assignment_3"):
    return SparkSession.builder.appName(app_name).getOrCreate()

def user_login_dataframe(spark, data, columns):
    return spark.createDataFrame(data, columns)

def actions_last_7_days(df):
    df_filtered = df.filter(col("time_stamp") >= ('2023-09-15' - expr("INTERVAL 7 DAYS")))
    result_df = df_filtered.groupBy("user_id").agg(count("log_id").alias("actions_last_7_days"))
    return result_df

def timestamp(df):
    return df.withColumn("login_date", date_format("time_stamp", "yyyy-MM-dd"))

def write_to_csv(df, path, mode='overwrite'):
    df.write.csv(path, mode=mode)

def write_as_managed_table(df, mode='overwrite'):
    df.write.mode(mode).saveAsTable(user.login_details)
