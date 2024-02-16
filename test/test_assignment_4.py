import unittest
from pyspark.sql import SparkSession
from src.assignment_4.util import *


class TestEmployeeDetails(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.spark = SparkSession.builder.appName("assignment_4").getOrCreate()
        cls.json_file_path = r"C:\Users\SharanKumar\Desktop\JSON files\Nested_json_file.json"
        cls.json_data_file_path = r"C:\Users\SharanKumar\Desktop\JSON files\Nested_json_file.json"
        cls.id_value = "0001"

    def test_read_json_with_schema(self):
        df = read_json_with_schema(self.spark, self.json_file_path)
        self.assertEqual(df.count(), 10)

    def test_read_json_without_schema(self):
        df = read_json_without_schema(self.spark, self.json_file_path)
        self.assertEqual(df.count(), 10)

    def test_flatten_dataframe(self):
        df = read_json_with_schema(self.spark, self.json_file_path)
        df_flattened = flatten_dataframe(df)
        self.assertEqual(df_flattened.count(), 20)

    def test_explode_data(self):
        json_df = read_json_without_schema(self.spark, self.json_data_file_path)
        exploded_json_df = explode_data(json_df)
        self.assertEqual(exploded_json_df.count(), 5)

    def test_filter_data_by_id(self):
        json_df = read_json_without_schema(self.spark, self.json_data_file_path)
        filtered_json_df = filter_data_by_id(json_df, self.id_value)
        self.assertEqual(filtered_json_df.count(), 1)

    def test_convert_to_snake_case(self):
        df = read_json_with_schema(self.spark, self.json_file_path)
        df_snake_case = convert_to_snake_case(df)
        self.assertEqual(df_snake_case.count(), 10)

    def test_add_load_date_column(self):
        df = read_json_with_schema(self.spark, self.json_file_path)
        df_with_load_date = load_date_column(df)
        self.assertEqual(df_with_load_date.count(), 10)

    def test_add_date_columns(self):
        df = read_json_with_schema(self.spark, self.json_file_path)
        df_with_date_parts = date_parts_columns(df)
        self.assertEqual(df_with_date_parts.count(), 10)



if __name__ == '__main__':
    unittest.main()