from typing import Final, Optional

import requests
from airflow.hooks.base import BaseHook

UPBIT: Final[str] = "upbit"
ENDPOINT: Final[str] = "v1/candles/minutes/1"


def get_data_io_root():
    upbit_conn = BaseHook.get_connection(conn_id=UPBIT)
    root = f"{upbit_conn.host}"
    return root


def get_market_candles(
    market: str,
    to: Optional[str] = None,  # endDate
    count: Optional[str] = None,
):
    root = get_data_io_root()

    res = requests.get(
        url=f"{root}/{ENDPOINT}",
        params={
            "market": market,
            "to": to,
            "count": count,
        },
        headers={"accept": "application/json"},
    )

    res.raise_for_status()

    return res.json()
