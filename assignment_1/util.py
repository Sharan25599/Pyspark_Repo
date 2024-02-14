from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, size
from pyspark.sql.functions import row_number, rank, dense_rank, col, collect_list, collect_set, count, expr
from pyspark.sql.window import Window


spark=SparkSession.builder.appName("assignment_1").getOrCreate()

# question 1

data=[(1, "A"),
 (1, "B"),
 (2, "A"),
 (2, "B"),
 (3, "A"),
 (3, "B"),
 (1, "C"),
 (1, "D"),
 (1, "E"),
 (3, "E"),
 (4, "A") ]
schema=StructType([StructField('customer',IntegerType(),True),
                   StructField('product_model', StringType(), True)])
purchase_data_df=spark.createDataFrame(data,schema)
purchase_data_df.show()

data=[("A",),
 ("B",),
 ("C",),
 ("D",),
 ("E",)]
schema=StructType([StructField('customer',StringType(),True)])
product_data_df=spark.createDataFrame(data,schema)
product_data_df.show()

# question 2

only_product_df = purchase_data_df.groupBy("customer").agg(collect_set('product_model')).filter(expr("collect_set(product_model) = array('A')"))
only_product_df.show()

# question 3
# purchase_data_df.where((purchase_data_df.product_model == 'B').alias('p1')  & (purchase_data_df.product_model == 'E')).alias('p2').show()
# purchase_data_df.where((purchase_data_df.product_model == 'E')).show()

upgrade_customers = purchase_data_df.alias("p1").join(purchase_data_df.alias("p2"), ['customer']).filter("(p1.product_model = 'B') and (p2.product_model = 'E')")
upgrade_customers.select("p1.customer").show()

# question 4

all_models_df = purchase_data_df.groupBy("customer").agg(collect_set('product_model').alias('SetProduct'))
# all_models_df.filter(col(size(col('SetProduct')) == '5')).show()
all_models_df.withColumn('lenProduct',size(col('SetProduct'))).show()







