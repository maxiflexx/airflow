from datetime import datetime, timedelta


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
