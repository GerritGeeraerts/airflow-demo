import logging
from datetime import timedelta, datetime

import pandas as pd
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook


from sklearn.linear_model import LinearRegression
from sklearn.model_selection import train_test_split
from sklearn.metrics import mean_squared_error


def extract_data():
    logging.info("Extracting data")
    src_hook = PostgresHook(postgres_conn_id='clean_data')
    conn = src_hook.get_conn()
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM market_data;")
    data = cursor.fetchall()
    df = pd.DataFrame(data, columns=[desc[0] for desc in cursor.description])
    cursor.close()
    conn.close()
    logging.info(f"Extracted {len(df)} rows")
    return df


def train_model(ti):
    df = ti.xcom_pull(task_ids='extract_data')
    logging.info(f"Extracted data: {len(df)} rows")

    X = df[['id']]
    y = df['close']

    # Split the data into training and testing sets
    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=0)
    logging.info(f"Training set: {len(X_train)} rows")
    logging.info(f"Testing set: {len(X_test)} rows")

    # Create a linear regression model
    model = LinearRegression()
    logging.info("Training the model")
    model.fit(X_train, y_train)  # Train the model

    # Predict on the testing set
    logging.info('Predicting the model')
    y_pred = model.predict(X_test)

    # Calculate the Mean Squared Error
    mse = mean_squared_error(y_test, y_pred)

    return {}


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG('train_model',
         default_args=default_args,
         description='A simple DAG to process data',
         schedule_interval=timedelta(minutes=5),
         start_date=datetime(2023, 1, 1),
         catchup=False) as dag:
    extract = PythonOperator(
        task_id='extract_data',
        python_callable=extract_data,
    )

    train = PythonOperator(
        task_id='train_model',
        python_callable=train_model,
    )

    extract >> train
