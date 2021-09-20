
from datetime import date, timedelta, datetime
from pathlib import Path
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
import os

# Initialize log folder path (Recall "docker exec -it airflow sh" to log into docker-airflow.)
LOG_PATH = "/opt/airflow/logs/marketvol"


# Create Python method to parse each file
def analyze_file(log_dir, symbol):


    # Iterate through log_path directory to get all log files w/ ".log" extension as list.
    log_file_list = Path(log_dir).rglob("*.log")
    # Initialize empty error message list and create list of log files object
    errors = []
    logfile_list = list(log_file_list)

    # Iterate through each log file
    for file in logfile_list:
        file_str = str(file)
        # Filter out and open only files that have "marketvol" or stock "symbol" in file string
        if file_str.find("marketvol") != -1 and file_str.find(symbol) != -1:
            log_file = open(file_str, "r")
            # Iterate through each line in log_file to see if "ERROR" msg exists (if so, append)
            for line in log_file:
                if "ERROR" in line:
                    errors.append(line)

    num_errors = len(errors)


    return(num_errors, errors)

# Define default_args
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2021, 9, 16)
}


# Initialize DAG
with DAG(dag_id='log_analyzer',
         default_args=default_args,
         description="Simple DAG on Stock Market volume data",
         schedule_interval="* * * * *"  # Formerly used "@daily".
         ) as dag:

    t1 = PythonOperator(
        task_id="TSLA_log_errors",
        python_callable=analyze_file,
        provide_context=True,
        op_kwargs={'symbol': 'TSLA', 'log_dir': LOG_PATH}
    )


    t2 = PythonOperator(
        task_id="AAPL_log_errors",
        python_callable=analyze_file,
        provide_context=True,
        op_kwargs={'symbol': 'AAPL', 'log_dir': LOG_PATH}
    )


t1 >> t2
