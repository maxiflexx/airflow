import datetime as dt
import time

import airflow.utils.dates
import pandas as pd
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import BranchPythonOperator, PythonOperator
from libs.coins import transform_raw_data
from libs.data_io import get_coins, get_markets, upsert_coins
from libs.datalake import (
    PROCESSED,
    RAWS,
    get_minio_object,
    write_data_by_date,
    write_to_minio,
)
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
        files = write_data_by_date(
            market,
            "candle_date_time_utc",
            coins,
            RAWS,
        )
    return files


def get_coins_one_day(market):
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

        time.sleep(0.2)
    return answer


crawl_recent_coin = PythonOperator(
    task_id="crawl_recent_coin", python_callable=_crawl_recent_coin, dag=dag
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
    input_callable=lambda **kwargs: pd.DataFrame(get_minio_object(kwargs["files"])),
    input_callable_kwargs={
        "files": "{{ ti.xcom_pull(task_ids='crawl_recent_coin') }}",
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

end_task = EmptyOperator(task_id="end_task", dag=dag)


get_enabled_markets >> get_latest_coins >> choose_next_task
choose_next_task >> [end_task, start_crawl]
start_crawl >> crawl_recent_coin >> transform_coins >> load_coins >> end_task
