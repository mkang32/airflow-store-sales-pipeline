from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.mysql_operator import MySqlOperator
from datetime import datetime, timedelta

from data_cleaner import data_cleaner

# define dag 
default_args = {
    "owner": "Airflow",
    "start_date": datetime(2021, 4, 1),
    "retries": 1, 
    "retry_delay": timedelta(seconds=5)
}



dag = DAG("store_dag", 
    default_args=default_args, 
    schedule_interval='@daily',
    template_searchpath=['/usr/local/airflow/sql_files'],
    catchup=False)

# tasks 
# Read & clean input file 
#   -> Save cleansed data into MySQL table 
#       -> Location wise profit 
#       -> Store wise profit 
#           -> Save results into CSV 
#               -> Send email 

# task1: check if the source file exists in the input directory 
# note that the file in airflow container. 
t1 = BashOperator(
    task_id="check_file_exists",
    bash_command="shasum ~/store_files_airflow/raw_store_transactions.csv", 
    retries=1,
    retry_delay=timedelta(seconds=15),
    dag=dag)

# task 2: clean data
t2 = PythonOperator(
    task_id="clean_raw_csv",
    python_callable=data_cleaner,
    dag=dag)

# task 3: create table 
t3 = MySqlOperator(
    task_id="create_mysql_table",
    mysql_conn_id="mysql_conn",
    sql="create_table.sql",
    dag=dag)

t1 >> t2 >> t3

