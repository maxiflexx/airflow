import io
import json
from copy import deepcopy

from airflow.hooks.base import BaseHook
from airflow.models.connection import Connection
from minio import Minio
from minio.error import MinioException


def get_minio_client(minio_conn: Connection):
    return Minio(
        endpoint=minio_conn.host,
        access_key=minio_conn.login,
        secret_key=minio_conn.password,
        secure=False,
    )


def generate_object_name(data_type: str, market_name: str, date_str: str):
    return f"{data_type}/{market_name}_{date_str}.json"


def write_minio_object(
    market_name: str,
    date_str: str,
    data: list[dict],
    data_type: str,
):
    minio_conn = BaseHook.get_connection(conn_id="minio")
    client = get_minio_client(minio_conn)

    bucket_name = json.loads(minio_conn._extra)["bucket_name"]
    object_name = generate_object_name(data_type, market_name, date_str)

    raws = deepcopy(data)
    try:
        res = client.get_object(bucket_name, object_name)
        raws.extend(json.loads(res.data))
    except MinioException as e:
        pass

    contents = json.dumps(raws).encode("utf8")

    client.put_object(
        bucket_name=bucket_name,
        object_name=object_name,
        content_type="application/json",
        data=io.BytesIO(contents),
        length=-1,
        part_size=5 * 1024 * 1024,
    )
