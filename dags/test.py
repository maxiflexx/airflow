import datetime as dt
import time

import airflow.utils.dates
from airflow import DAG
from airflow.operators.bash import BashOperator

dag = DAG(
    dag_id="jenkins_test",
    schedule_interval=dt.timedelta(minutes=1),
    start_date=airflow.utils.dates.days_ago(0, hour=5, minute=19),
    catchup=False,
    render_template_as_native_obj=True,
)

test = BashOperator(
    task_id="bash_task",
    bash_command="""
    echo "good!"
  """,
    dag=dag,
)

test
