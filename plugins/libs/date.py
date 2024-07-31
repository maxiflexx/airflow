from datetime import datetime, timedelta

import pandas as pd


# crawl을 위한 함수
# upbit api가 최대 200개(200분)씩 데이터를 가져올 수 있기 때문
def generate_end_dates():
    now = datetime.utcnow()
    yesterday = now - timedelta(days=1)

    current = now
    end_dates = []

    while current > yesterday:
        end_dates.append(current.isoformat() + "Z")
        current = current - timedelta(minutes=200)

    return end_dates


# data를 YYYYmmdd 기준으로 구분
def split_by_day(data, date_column):
    df = pd.DataFrame(data)

    df = df.drop_duplicates([date_column, "market"])
    df[date_column] = pd.to_datetime(df[date_column])

    unique_dates = df[date_column].dt.date.unique()
    split_data = {}

    for date in unique_dates:
        daily_data = df[df[date_column].dt.date == date]
        daily_data[date_column] = daily_data[date_column].astype(str)

        formatted = "".join(date.isoformat().split("-"))
        split_data[formatted] = daily_data.to_dict(orient="records")

    return split_data
