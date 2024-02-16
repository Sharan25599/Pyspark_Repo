import unittest
from pyspark.sql import SparkSession
from src.assignment_3.utils import *

class TestAssertions(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.spark = SparkSession.builder.appName("Test").getOrCreate()
        cls.data = [
            (1, 101, 'login', '2023-09-05 08:30:00'),
            (2, 102, 'click', '2023-09-06 12:45:00'),
            (3, 101, 'click', '2023-09-07 14:15:00'),
            (4, 103, 'login', '2023-09-08 09:00:00'),
            (5, 102, 'logout', '2023-09-09 17:30:00'),
            (6, 101, 'click', '2023-09-10 11:20:00'),
            (7, 103, 'click', '2023-09-11 10:15:00'),
            (8, 102, 'click', '2023-09-12 13:10:00')
        ]
        cls.columns = ['log_id', 'user_id', 'action', 'timestamp']
        cls.df = cls.spark.createDataFrame(cls.data, cls.columns)

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    def test_actions_last_7_days(self):
        result = actions_last_7_days(self.spark, self.df)
        expected_result = self.spark.createDataFrame([(101, 3), (102, 2), (103, 2)], ['user_id', 'actions_last_7_days'])
        self.assertTrue(result.collect() == expected_result.collect())

    def test_timestamp(self):
        result_df = timestamp(self.spark, self.df)
        expected_columns = ['log_id', 'user_id', 'action', 'timestamp', 'login_date']
        self.assertEqual(result_df.columns, expected_columns)

    def test_write(self):
        write_as_managed_table(self.spark, self.df, "user.login_details")
        table_exists = self.spark.catalog.tableExists("user.login_details")
        self.assertTrue(table_exists)

if __name__ == '__main__':
    unittest.main()
