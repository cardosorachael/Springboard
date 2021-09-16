from airflow.models import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from datetime import date, datetime, timedelta

import yfinance as yf
import pandas as pd

default_args = {
            "owner": "airflow",
            "start_date": datetime(2021, 9, 16),
            "retries": 2, # retry twice
            "retry_delay": timedelta(minutes=5) # five minutes interval
        }

today = date.today()

def download_stock(symbol):
    start_date = today
    end_date = start_date + timedelta(days=1)
    df = yf.download(symbol, start=start_date, end=end_date, interval='1m')
    df.to_csv(symbol + '_data.csv', header=True)


def query_stock():
    apple_df = pd.read_csv("/tmp/data/" + str(today) + "/AAPL_data.csv", header=0)
    tesla_df = pd.read_csv("/tmp/data/" + str(today) + "/TSLA_data.csv", header=0)
    query = [(apple_df['High'][0] + apple_df['Low'][0]) / 2, (tesla_df['High'][0] + tesla_df['Low'][0]) / 2]
    return query

today_str = (datetime.now()).strftime('%Y-%m-%d')

#run the workflow on 6 PM every work day
dag = DAG(
    dag_id='marketvol',
    default_args=default_args,
    description='A simple DAG',
    schedule_interval= timedelta(days=1),
)

t0 = BashOperator(
    task_id='init_dir',
    bash_command="mkdir -p /tmp/data/" + str(today),
    dag=dag
)

# extract data for symbol AAPL
t1 = PythonOperator(
    task_id='extract_stock1',
    dag=dag,
    python_callable=download_stock,
    op_kwargs={'symbol': 'AAPL'}
)

# extract data for symbol TSLA
t2 = PythonOperator(
    task_id='extract_stock2',
    dag=dag,
    python_callable=download_stock,
    op_kwargs={'symbol': 'TSLA'}
)
# move data download into a same place
t3 = BashOperator(
    task_id='move_data1',
    dag=dag,
    bash_command='mv /opt/airflow/TSLA_data.csv /tmp/data/' + str(today) + "/"
)

t4 = BashOperator(
    task_id='move_data2',
    dag=dag,
    bash_command='mv /opt/airflow/AAPL_data.csv /tmp/data/' + str(today) + "/")

t5 = PythonOperator(
        task_id="t5",
        python_callable=query_stock,
        dag=dag
    )

t0 >> [t1, t2]
t1 >> t3
t2 >> t4
[t3, t4] >> t5