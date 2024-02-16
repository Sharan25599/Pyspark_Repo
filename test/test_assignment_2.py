import unittest
from pyspark.sql import SparkSession
from src.assignment_2.utils import *


class TestUtils(unittest.TestCase):

    def setUp(self):
        self.spark = SparkSession.builder.appName("UnitTest").getOrCreate()

    def test_create_credit_card_dataframe(self):
        data = [("1234567891234567",), ("5678912345671234",)]
        schema = ["card_number"]
        df = credit_card_dataframe(self.spark, data, schema)
        self.assertEqual(df.count(), 2)

    def test_increase_partitions(self):
        data = [("1234567891234567",), ("5678912345671234",)]
        schema = ["card_number"]
        df = credit_card_dataframe(self.spark, data, schema)
        increased_df = increase_partitions(df, 5)
        self.assertEqual(increased_df.rdd.getNumPartitions(), 5)



if __name__ == '__main__':
    unittest.main()