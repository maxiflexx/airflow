airflow connections add 'upbit' \
    --conn-type 'http' \
    --conn-description 'Connection to retrieve data from upbit API.' \
    --conn-host 'https://api.upbit.com' \
    --conn-schema 'https'

airflow connections add 'minio' \
    --conn-type 'http' \
    --conn-description 'Connection to MinIO for storing data.' \
    --conn-host 'minio:9000' \
    --conn-schema 'http' \
    --conn-login 'admin' \
    --conn-password 'password' \
    --conn-extra '{ "bucket_name": "datalake" }'

airflow connections add 'data-io' \
    --conn-type 'http' \
    --conn-description 'Connection to DataIO for cryptocurrency data.' \
    --conn-host 'host.docker.internal' \
    --conn-schema 'http' \
    --conn-port '3001'
