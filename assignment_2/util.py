from pyspark.sql import SparkSession
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType

spark=SparkSession.builder.appName('assignment_2').getOrCreate()

# question 1
data=[("1234567891234567",),
 ("5678912345671234",),
 ("9123456712345678",),
 ("1234567812341122",),
 ("1234567812341342",)]
column=['card_number']
credit_card_df=spark.createDataFrame(data,column)
# credit_card_df.show()

# question 2
# print(credit_card_df.rdd.getNumPartitions())


# question 3
# credit_card_df.repartition(5)

# question 4
original_partition_size=credit_card_df.coalesce(credit_card_df.rdd.getNumPartitions())
# print(original_partition_size.rdd.getNumPartitions())

# question 5

# save to disk
original_partition_size.write.mode("overwrite").parquet(r"C:\Users\SharanKumar\Desktop\Parquet")

# read from disk
read_df=spark.read.parquet(r"C:\Users\SharanKumar\Desktop\Parquet",header=True)


# Udf to print only last 4 digits marking remaining digits as *
def mask_credit_card(card_number):
 masked_number = "*" * (len(card_number) - 4) + card_number[-4:]
 return masked_number

mask_credit_card_udf = udf(mask_credit_card, StringType())
credit_card_df = credit_card_df.withColumn("masked_credit_card", mask_credit_card_udf("credit_card_number"))
credit_card_df.show(truncate=False)









