from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago

import psycopg2

def init_database():
    # Connect to the default 'postgres' database
    conn = psycopg2.connect(
        dbname='postgres',
        user='clean_data_user',
        password='clean_data_password',
        host='postgres-clean-data'
    )
    conn.autocommit = True
    cur = conn.cursor()

    # Check if 'clean_data' database exists and create it if not
    cur.execute("SELECT 1 FROM pg_database WHERE datname='clean_data'")
    exists = cur.fetchone()
    if not exists:
        cur.execute("CREATE DATABASE clean_data")
        print("Database created.")
    else:
        print("Database already exists.")

    # Close cursor and connection to the 'postgres' database
    cur.close()
    conn.close()

    # Reconnect to the newly created 'clean_data' database
    conn = psycopg2.connect(
        dbname='clean_data',
        user='clean_data_user',
        password='clean_data_password',
        host='postgres-clean-data'
    )
    conn.autocommit = True
    cur = conn.cursor()

    # Create the table if it doesn't exist
    cur.execute("""
        CREATE TABLE IF NOT EXISTS market_data (
            id BIGINT PRIMARY KEY,
            time TIMESTAMP,
            open NUMERIC(14,8),
            high NUMERIC(14,8),
            low NUMERIC(14,8),
            close NUMERIC(14,8),
            volume NUMERIC(12,8)
        );
    """)

    # Confirm table creation and close resources
    print("Table created or already exists.")
    cur.close()
    conn.close()


default_args = {
    'owner': 'airflow',
    'start_date': days_ago(1),
}

dag = DAG(
    'initialize_database',
    default_args=default_args,
    schedule_interval='@once',  # Run once or as required
    catchup=False
)

init_db_task = PythonOperator(
    task_id='init_db',
    python_callable=init_database,
    dag=dag,
)

init_db_task
