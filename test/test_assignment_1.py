import unittest
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType
from src.assignment_1.util import *

class MyTestCase(unittest.TestCase):
    def setUp(self):
        self.spark = SparkSession.builder.appName("TestPyspark").getOrCreate()
        self.purchase_data_schema = StructType([
            StructField("customer", IntegerType(), True),
            StructField("product_model", StringType(), True)
        ])

        self.product_data_schema = StructType([
            StructField("product_model", StringType(), True)
        ])

        purchase_data = [
            (1, "A"), (1, "B"), (2, "A"), (2, "B"), (3, "A"), (3, "B"),
            (1, "C"), (1, "D"), (1, "E"), (3, "E"), (4, "A")
        ]

        product_data = [("A",), ("B",), ("C",), ("D",), ("E",)]

        self.purchase_data_df = self.spark.createDataFrame(purchase_data, schema=self.purchase_data_schema)
        self.product_data_df = self.spark.createDataFrame(product_data, schema=self.product_data_schema)

    def test_customers_bought_only_A(self):
        result = bought_only_A(self.purchase_data_df)
        self.assertEqual(result.count(), 4)  # Expected count for customers who bought only product A

    def test_customers_upgraded_B_to_E(self):
        result = upgraded_B_to_E(self.purchase_data_df)
        self.assertEqual(result.count(), 2)  # Expected count for customers who upgraded from product B to product E

    def test_customers_bought_all_models(self):
        result = bought_all_models(self.purchase_data_df, self.product_data_df)
        self.assertEqual(result.count(), 1)  # Expected count for customers who bought all models in the new Product Data

    def tearDown(self):
        self.spark.stop()

if __name__ == '__main__':
    unittest.main()
