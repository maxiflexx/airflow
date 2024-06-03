import datetime as dt
import time

import airflow.utils.dates
import pandas as pd
import requests
from airflow import DAG
from airflow.hooks.base import BaseHook
from airflow.operators.python import PythonOperator
from libs.datalake import write_minio_object
from operators.pandas_operator import PandasOperator

dag = DAG(
    dag_id="coins_etl_dag",
    description="ETL DAG for fetching coin data from the Upbit API, transforming it, and storing it in Opensearch via the data-io server.",
    schedule_interval=dt.timedelta(minutes=1),
    start_date=airflow.utils.dates.days_ago(0, hour=5, minute=19),
    catchup=False,
    render_template_as_native_obj=True,
)


def _get_markets():
    data_io_conn = BaseHook.get_connection(conn_id="data-io")
    url = f"{data_io_conn.schema}://{data_io_conn.host}:{data_io_conn.port}/markets"

    res = requests.get(url=url)

    data = res.json()

    enabled_markets = list(filter(lambda market: market["isEnabled"] == True, data))
    return enabled_markets  # xcom push


get_markets = PythonOperator(
    task_id="get_markets",
    python_callable=_get_markets,
    dag=dag,
)


# ts: ISO string
# ds: YYYYMMDD
def _extract_coins(ts, ds, ti, **_):
    upbit_conn = BaseHook.get_connection(conn_id="upbit")
    url = f"{upbit_conn.host}/v1/candles/minutes/1"

    enabled_markets = ti.xcom_pull(task_ids="get_markets")

    coins = []
    for market in enabled_markets:
        market_code = market["code"]

        response = requests.get(
            url=url,
            params={
                "market": market_code,
                "to": ts,
                "count": 1,
            },
            headers={"accept": "application/json"},
        )

        data = response.json()

        write_minio_object(
            market_name=str(market_code),
            date_str=ds,
            data=data,
            data_type="raws",
        )

        time.sleep(0.1)  # 0.1초
        coins = [*coins, *data]

    return coins  # xcom push


extract_coins = PythonOperator(
    task_id="extract_coins",
    python_callable=_extract_coins,
    dag=dag,
)


def _transform_raw_data(df: pd.DataFrame):
    return df.rename(
        columns={
            "candle_date_time_utc": "candleDateTimeUtc",
            "candle_date_time_kst": "candleDateTimeKst",
            "opening_price": "openingPrice",
            "high_price": "highPrice",
            "low_price": "lowPrice",
            "trade_price": "tradePrice",
            "candle_acc_trade_price": "candleAccTradePrice",
            "candle_acc_trade_volume": "candleAccTradeVolume",
        }
    )


def _write_processed(df: pd.DataFrame, ds: str):
    markets = df["market"].unique()

    for market in markets:
        data = df[df["market"] == market].to_dict(orient="records")
        write_minio_object(
            market_name=str(market),
            date_str=ds,
            data=data,
            data_type="processed",
        )


transform_coins = PandasOperator(
    task_id="transform_coins",
    input_callable=pd.DataFrame,
    input_callable_kwargs={
        "data": "{{ ti.xcom_pull(task_ids='extract_coins') }}",
    },
    transform_callable=_transform_raw_data,
    output_callable=_write_processed,
    output_callable_kwargs={
        "ds": "{{ ds }}",
    },
    dag=dag,
)


def _load_coins(ti):
    data_io_conn = BaseHook.get_connection(conn_id="data-io")

    url = f"{data_io_conn.schema}://{data_io_conn.host}:{data_io_conn.port}/coins"
    data = ti.xcom_pull(task_ids="transform_coins")

    res = requests.post(url=url, json=data)

    res.raise_for_status()


load_coins = PythonOperator(
    task_id="load_coins",
    python_callable=_load_coins,
    dag=dag,
)


get_markets >> extract_coins >> transform_coins >> load_coins
