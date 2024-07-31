import datetime as dt
import time

import airflow.utils.dates
import pandas as pd
from airflow import DAG
from airflow.operators.python import PythonOperator
from libs.coins import transform_raw_data
from libs.data_io import get_markets, upsert_coins
from libs.datalake import PROCESSED, RAWS, write_to_minio
from libs.upbit import get_market_candles
from operators.pandas_operator import PandasOperator

dag = DAG(
    dag_id="coins_etl_dag",
    description="ETL DAG for fetching coin data from the Upbit API, transforming it, and storing it in Opensearch via the data-io server.",
    schedule_interval=dt.timedelta(minutes=1),
    start_date=airflow.utils.dates.days_ago(0, hour=5, minute=19),
    catchup=False,
    render_template_as_native_obj=True,
)


def _get_enabled_markets():
    markets = get_markets()

    enabled_markets = list(filter(lambda market: market["isEnabled"] == True, markets))
    return enabled_markets  # xcom push


get_enabled_markets = PythonOperator(
    task_id="get_enabled_markets",
    python_callable=_get_enabled_markets,
    dag=dag,
)


# ts: ISO string
# ds: YYYYMMDD
def _extract_coins(ts, ds, ti, **_):
    enabled_markets = ti.xcom_pull(task_ids="get_enabled_markets")

    coins = []
    for market in enabled_markets:
        market_code = market["code"]
        raws = get_market_candles(market=market_code, to=ts, count=1)

        write_to_minio(
            market_name=str(market_code),
            date_str=ds,
            data=raws,
            data_type=RAWS,
        )

        time.sleep(0.2)  # 0.2초
        coins = [*coins, *raws]

    return coins  # xcom push


extract_coins = PythonOperator(
    task_id="extract_coins",
    python_callable=_extract_coins,
    dag=dag,
)


def _write_processed(df: pd.DataFrame, ds: str):
    markets = df["market"].unique()

    for market in markets:
        data = df[df["market"] == market].to_dict(orient="records")
        write_to_minio(
            market_name=str(market),
            date_str=ds,
            data=data,
            data_type=PROCESSED,
        )


transform_coins = PandasOperator(
    task_id="transform_coins",
    input_callable=pd.DataFrame,
    input_callable_kwargs={
        "data": "{{ ti.xcom_pull(task_ids='extract_coins') }}",
    },
    transform_callable=transform_raw_data,
    output_callable=_write_processed,
    output_callable_kwargs={
        "ds": "{{ ds }}",
    },
    dag=dag,
)


load_coins = PythonOperator(
    task_id="load_coins",
    python_callable=upsert_coins,
    op_kwargs={"data": "{{ ti.xcom_pull(task_ids='transform_coins') }}"},
    dag=dag,
)


get_enabled_markets >> extract_coins >> transform_coins >> load_coins
