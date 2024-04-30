import logging
from datetime import datetime, timedelta

import psycopg2
from airflow import DAG
# from airflow.operators.python_operator import PythonOperator
import requests
import json

from airflow.operators.python import PythonOperator
from psycopg2 import sql


import logging


def fetch_data(symbol='BTCUSDT', limit=9999, interval='1m'):
    logging.info(f'Fetching {limit} candles for {symbol} interval: {interval}')
    url = f'https://api.binance.com/api/v3/klines?symbol={symbol}&interval={interval}&limit={limit}'
    try:
        response = requests.get(url)
        response.raise_for_status()
        data = json.loads(response.text)
        return data
    except requests.exceptions.HTTPError as err:
        logging.error(f"HTTP Error: {err}")
        raise  # Re-raise the exception

    except requests.exceptions.ConnectionError as err:
        logging.error(f"Connection Error: {err}")
        raise  # Re-raise the exception

    except requests.exceptions.Timeout as err:
        logging.error(f"Timeout Error: {err}")
        raise  # Re-raise the exception

    except requests.exceptions.RequestException as err:
        logging.error(f"Request Exception: {err}")
        raise  # Re-raise the exception


def insert_data(**kwargs):
    ti = kwargs['ti']
    data = ti.xcom_pull(task_ids='fetch_data')

    conn = psycopg2.connect(
        dbname='raw_data',
        user='raw_data_user',
        password='raw_data_password',
        host='postgres-raw-data'
    )
    cursor = conn.cursor()
    upsert_sql = sql.SQL("""
        INSERT INTO market_data (time, open, high, low, close, volume)
        VALUES (%s, %s, %s, %s, %s, %s)
        ON CONFLICT (time) DO UPDATE SET
        open = EXCLUDED.open,
        high = EXCLUDED.high,
        low = EXCLUDED.low,
        close = EXCLUDED.close,
        volume = EXCLUDED.volume
    """)
    for candle in data:
        cursor.execute(upsert_sql, (
            candle[0], candle[1], candle[2], candle[3], candle[4], candle[5]))
    conn.commit()
    cursor.close()
    conn.close()

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email': ['your_email@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'fetch_crypto_prices',
    default_args=default_args,
    description='A simple DAG to fetch and insert crypto prices',
    schedule_interval=timedelta(minutes=5),
    start_date=datetime(2024, 4, 24),
    catchup=False,
) as dag:

    fetch_data_task = PythonOperator(
        task_id='fetch_data',
        python_callable=fetch_data,
        op_kwargs={'symbol': 'BTCUSDT', 'limit': 100, 'interval': '1m'}
    )

    insert_data_task = PythonOperator(
        task_id='insert_data',
        python_callable=insert_data,
        provide_context=True
    )

    fetch_data_task >> insert_data_task

# Define the task

# Set the task sequence

# testing the fetch_prices function
# docker compose exec airflow-scheduler airflow tasks test fetch_crypto_prices fetch_data 2024-04-24

