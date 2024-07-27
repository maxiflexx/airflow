from typing import Dict, Final, List, Optional

import requests
from airflow.hooks.base import BaseHook

DATA_IO: Final[str] = "data-io"
MARKETS: Final[str] = "markets"
COINS: Final[str] = "coins"


def get_data_io_root():
    data_io_conn = BaseHook.get_connection(conn_id=DATA_IO)
    root = f"{data_io_conn.schema}://{data_io_conn.host}:{data_io_conn.port}"
    return root


def get_markets():
    root = get_data_io_root()

    res = requests.get(url=f"{root}/{MARKETS}")

    res.raise_for_status()

    return res.json()


def get_coins(
    market: str,
    startDate: Optional[str] = None,
    endDate: Optional[str] = None,
    limit: Optional[int] = None,
    offset: Optional[str] = None,
    sortingField: Optional[str] = None,
    sortingDirection: Optional[str] = None,
):
    root = get_data_io_root()

    res = requests.get(
        url=f"{root}/{COINS}",
        params={
            "market": market,
            "startDate": startDate,
            "endDate": endDate,
            "limit": limit,
            "offset": offset,
            "sortingField": sortingField,
            "sortingDirection": sortingDirection,
        },
    )

    res.raise_for_status()

    return res.json()


def upsert_coins(data: List[Dict]):
    root = get_data_io_root()

    res = requests.post(
        url=f"{root}/{COINS}",
        json=data,
    )

    res.raise_for_status()

    return res.json()
