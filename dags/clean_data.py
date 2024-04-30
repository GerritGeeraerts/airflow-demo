import logging
from decimal import Decimal

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime, timedelta

from airflow.sensors.external_task import ExternalTaskSensor


def extract_data():
    logging.info("Extracting data")
    src_hook = PostgresHook(postgres_conn_id='raw_data')
    conn = src_hook.get_conn()
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM market_data;")
    data = cursor.fetchall()
    cursor.close()
    conn.close()
    logging.info(f"Extracted {len(data)} rows")
    return data


def clean_data(ti):
    logging.info("Cleaning data")
    data = ti.xcom_pull(task_ids='extract_data')
    logging.debug(f"Extracted data: {len(data)} rows")
    cleaned_data = []
    for row in data:
        cleaned_timestamp = datetime.fromtimestamp(row[0] / 1000.0)  # Convert to datetime
        cleaned_row = row[:1] + [cleaned_timestamp] + row[1:]
        cleaned_data.append(cleaned_row)
    logging.info(f"Cleaned {len(cleaned_data)} rows")
    return cleaned_data


def load_data(ti):
    logging.info("Loading data")
    cleaned_data = ti.xcom_pull(task_ids='clean_data')
    dest_hook = PostgresHook(postgres_conn_id='clean_data')
    conn = dest_hook.get_conn()
    cursor = conn.cursor()

    # Upsert query to handle both insert and update operations
    upsert_query = """
    INSERT INTO market_data (id, time, open, high, low, close, volume)
    VALUES (%s, %s, %s, %s, %s, %s, %s)
    ON CONFLICT (id) DO UPDATE SET
    time = EXCLUDED.time,
    open = EXCLUDED.open,
    high = EXCLUDED.high,
    low = EXCLUDED.low,
    close = EXCLUDED.close,
    volume = EXCLUDED.volume
    """

    cursor.executemany(upsert_query, cleaned_data)
    conn.commit()
    cursor.close()
    conn.close()
    logging.info("Data loaded successfully with UPSERT operation")

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG('data_processing_dag',
         default_args=default_args,
         description='A simple DAG to process data',
         schedule_interval=timedelta(minutes=5),
         start_date=datetime(2023, 1, 1),
         catchup=False) as dag:

    # wait_for_dag1 = ExternalTaskSensor(
    #     task_id='wait_for_fetch_crypto_prices',
    #     external_dag_id='fetch_crypto_prices',
    #     external_task_id=None,  # Set to None to wait for the whole DAG to complete
    #     execution_delta=timedelta(minutes=2),
    #     timeout=500,  # Timeout in seconds after which the task will fail
    #     poke_interval=10,  # Check every minute
    #     mode='reschedule',  # Reschedule itself until external task succeeds
    # )

    extract = PythonOperator(
        task_id='extract_data',
        python_callable=extract_data,
    )

    clean = PythonOperator(
        task_id='clean_data',
        python_callable=clean_data,
    )

    load = PythonOperator(
        task_id='load_data',
        python_callable=load_data,
    )

    # wait_for_dag1 >> extract >> clean >> load
    extract >> clean >> load
