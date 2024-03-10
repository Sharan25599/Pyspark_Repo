from pyspark.sql.functions import *
from src.assignment_2.driver import spark


def credit_card_dataframe(spark, data, schema):
    return spark.createDataFrame(data, schema)

def increase_partitions(df, new_partitions):
    return df.repartition(new_partitions)

def back_to_original_partitions(df, original_partitions):
    return df.coalesce(original_partitions)

def save_and_read_parquet(df, path):
    df.write.parquet(path, mode="overwrite")
    return spark.read.parquet(path)

def mask_card_number_udf():
    def mask_card_number(card_number):
        if len(card_number) >= 4:
            return "*" * (len(card_number) - 4) + card_number[-4:]
        else:
            return card_number

    return udf(mask_card_number, StringType())

def masked_credit_card(df, mask_udf):
    df = df.withColumn("masked_card_number", mask_udf("card_number"))
    return df
