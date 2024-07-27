import datetime as dt
import time

import airflow.utils.dates
import pandas as pd
from airflow import DAG
from airflow.hooks.base import BaseHook
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import BranchPythonOperator, PythonOperator
from libs.data_io import get_coins, get_markets
from libs.datalake import generate_object_name, get_minio_object, write_minio_object
from libs.date import generate_end_dates
from libs.upbit import get_market_candles
from operators.pandas_operator import PandasOperator

dag = DAG(
    dag_id="initial_data_collection_dag",
    description="This DAG is responsible for collecting cryptocurrency market data when a user first enables a market.",
    schedule_interval=dt.timedelta(minutes=5),
    start_date=airflow.utils.dates.days_ago(0, hour=7, minute=10),
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


def _get_latest_coins(ti, **_):
    enabled_markets = ti.xcom_pull(task_ids="get_enabled_markets")

    recent_coin_exists = []
    no_recent_coin = []

    for market in enabled_markets:
        market_code = market["code"]
        res = get_coins(market=market_code, limit=1)
        latest_coins = res["data"]

        if len(latest_coins) == 0:
            no_recent_coin.append(market_code)
        else:
            recent_coin_exists.append(market_code)
    return recent_coin_exists, no_recent_coin


get_latest_coins = PythonOperator(
    task_id="get_latest_coins",
    python_callable=_get_latest_coins,
    dag=dag,
)


def _choose_next_task(ti, **_):
    recent_market_exists, no_recent_market = ti.xcom_pull(task_ids="get_latest_coins")

    if len(no_recent_market) == 0:
        return "end_task"
    else:
        return "start_crawl"


choose_next_task = BranchPythonOperator(
    task_id="choose_next_task", python_callable=_choose_next_task, dag=dag
)

start_crawl = EmptyOperator(task_id="start_crawl", dag=dag)


def _crawl_recent_coin(ti, **_):
    recent_market_exists, no_recent_market = ti.xcom_pull(task_ids="get_latest_coins")

    files = []
    for market in no_recent_market:
        coins = get_coins_one_day(market)
        split_data = split_by_day(coins, "candle_date_time_utc")

        for date in split_data.keys():
            write_minio_object(
                market_name=str(market),
                date_str=date,
                data=split_data[date],
                data_type="raws",
            )

            filename = generate_object_name("raws", market, date)
            files.append(filename)
    return files


def get_coins_one_day(market):
    upbit_conn = BaseHook.get_connection(conn_id="upbit")
    upbit_url = f"{upbit_conn.host}/v1/candles/minutes/1"

    end_dates = generate_end_dates()
    count = 200

    answer = []
    for idx, end_date in enumerate(end_dates):
        print(f"{market} request... {idx}")
        try:
            candles = get_market_candles(
                market=market,
                to=end_date,
                count=count,
            )

            answer += candles
            print(f"success")
        except:
            print(f"failed")

        time.sleep(0.1)  # 0.5초 대기
    return answer


def split_by_day(data, date_column):
    df = pd.DataFrame(data)
    df[date_column] = pd.to_datetime(df[date_column])
    unique_dates = df[date_column].dt.date.unique()
    split_data = {}

    for date in unique_dates:
        daily_data = df[df[date_column].dt.date == date]
        daily_data[date_column] = daily_data[date_column].astype(str)

        formatted = "".join(date.isoformat().split("-"))
        split_data[formatted] = daily_data.to_dict(orient="records")

    return split_data


crawl_recent_coin = PythonOperator(
    task_id="crawl_recent_coin", python_callable=_crawl_recent_coin, dag=dag
)


def _transform_raw_data(df: pd.DataFrame):
    answer = df.rename(
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
    return answer


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


def get_data_from_minio(files):
    answer = get_minio_object(files)
    return pd.DataFrame(answer)


transform_coins = PandasOperator(
    task_id="transform_coins",
    input_callable=get_data_from_minio,
    input_callable_kwargs={
        "files": "{{ ti.xcom_pull(task_ids='crawl_recent_coin') }}",
    },
    transform_callable=_transform_raw_data,
    output_callable=_write_processed,
    output_callable_kwargs={
        "ds": "{{ ds }}",
    },
    dag=dag,
)

end_task = EmptyOperator(task_id="end_task", dag=dag)


get_enabled_markets >> get_latest_coins >> choose_next_task
choose_next_task >> [end_task, start_crawl]
start_crawl >> crawl_recent_coin >> transform_coins >> end_task

"""
코드 정리 필요
덮어쓰기 로직 필요
중복되는 태스크 존재 -> group 살펴볼것
"""
