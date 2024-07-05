import unittest

import pandas as pd
from airflow import DAG
from airflow.utils.dates import days_ago
from plugins.operators.pandas_operator import PandasOperator


class TestPandasOperator(unittest.TestCase):
    def setUp(self) -> None:
        self.dag = DAG("test_dag", start_date=days_ago(1))

    def test_success(self):
        sample_data = {
            "name": ["Kim", "Lee", "Park", "Kim"],
            "year": [2013, 2013, 2015, 2017],
            "points": [1.5, 1.3, 3.7, 2.5],
        }

        task = PandasOperator(
            task_id="test_success",
            dag=self.dag,
            input_callable=pd.DataFrame,
            input_callable_kwargs={
                "data": sample_data,
            },
            transform_callable=pd.DataFrame.rename,
            transform_callable_kwargs={
                "columns": {"name": "last_name"},
            },
            output_callable=print,
        )

        result = task.execute({})

        expected_result = [
            {"last_name": "Kim", "year": 2013, "points": 1.5},
            {"last_name": "Lee", "year": 2013, "points": 1.3},
            {"last_name": "Park", "year": 2015, "points": 3.7},
            {"last_name": "Kim", "year": 2017, "points": 2.5},
        ]

        self.assertEqual(result, expected_result)


if __name__ == "__main__":
    unittest.main()
