import unittest
from pyspark.sql import SparkSession
from src.assignment_5.util import *


class TestUtils(unittest.TestCase):
    def setUp(self):
        self.spark = SparkSession.builder.appName("assignment_5").getOrCreate()
        self.employee_schema = employee_schema()
        self.department_schema =department_schema()
        self.country_schema = country_schema()

        self.employee_df, self.department_df, self.country_df = create_dataframes(
            self.spark, self.employee_schema, self.department_schema, self.country_schema
        )
        self.employee_df = add_bonus_column(self.employee_df)

    def tearDown(self):
        self.spark.stop()

    def test_avg_salary(self):
        avg_salary_department = avg_salary(self.employee_df)
        self.assertIsNotNone(avg_salary_department)

    def test_employees_start_with_m(self):
        result_df = employees_start_with_m(self.employee_df)
        self.assertIsNotNone(result_df)

    def test_add_bonus_column(self):
        self.assertIn('bonus', self.employee_df.columns)

    def test_reorder_columns(self):
        reordered_employee_df = reorder_columns(self.employee_df)
        self.assertIsNotNone(reordered_employee_df)
        self.assertIn('bonus', reordered_employee_df.columns)  # Ensure 'bonus' column is in the reordered DataFrame

    def test_perform_joins(self):
        inner_join, left_join, right_join = perform_joins(self.employee_df, self.department_df)
        self.assertIsNotNone(inner_join)
        self.assertIsNotNone(left_join)
        self.assertIsNotNone(right_join)

    def test_new_dataframe(self):
        new_employee_df = new_dataframe(self.employee_df, self.country_df)
        self.assertIsNotNone(new_employee_df)

    def test_convert_columns(self):
        final_df = convert_columns(self.employee_df)
        self.assertIsNotNone(final_df)

if __name__ == "__main__":
    unittest.main()