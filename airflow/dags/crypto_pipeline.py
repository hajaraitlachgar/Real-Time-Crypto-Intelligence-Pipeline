from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import requests

def fetch_crypto_price():
    url = "https://api.coingecko.com/api/v3/simple/price?ids=bitcoin&vs_currencies=usd"
    response = requests.get(url)
    data = response.json()
    print("Bitcoin Price:", data["bitcoin"]["usd"])

with DAG(
    dag_id="crypto_price_pipeline",
    start_date=datetime(2024, 1, 1),
    schedule="@daily",  
    catchup=False,
) as dag:

    fetch_task = PythonOperator(
        task_id="fetch_price",
        python_callable=fetch_crypto_price
    )

    fetch_task