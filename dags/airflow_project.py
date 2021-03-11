from datetime import timedelta, date

# The DAG object; we'll need this to instantiate a DAG
from airflow import DAG

# Operators; we need this to operate!
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from glob import glob
import yfinance as yf
import pandas as pd
# These args will get passed on to each operator
# You can override them on a per-task basis during operator initialization

default_args = {
    'owner': 'VAddanki',
    'depends_on_past': False,
    'email': ['airflow@vaddanki.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    # 'catchup':False,
    # 'queue': 'bash_queue',
    # 'pool': 'backfill',
    # 'priority_weight': 10,
    # 'end_date': datetime(2021, 3, 1),
    # 'wait_for_downstream': False,
    # 'dag': dag,
    # 'sla': timedelta(hours=2),
    # 'execution_timeout': timedelta(seconds=300),
    # 'on_failure_callback': some_function,
    # 'on_success_callback': some_other_function,
    # 'on_retry_callback': another_function,
    # 'sla_miss_callback': yet_another_function,
    # 'trigger_rule': 'all_success'
}
with DAG(
    'MarketVol',
    default_args=default_args,
    description='Airflow Mini-Project',
    schedule_interval='0 18 * * 1-5',
    # schedule_interval='0 18 * * *',
    start_date=days_ago(1),
    tags=['MarketVol'],
) as dag:

    def get_quotes(stock_symbol):
        start_date = date.today()
        end_date = start_date + timedelta(days=1)
        stock_df = yf.download(stock_symbol, start=start_date, end=end_date, interval='1m')
        file_name = '/c/GoogleDrive/Springboard/airflow-mini-project/data_'+ stock_symbol.lower() +'.csv'
        stock_df.to_csv(file_name, header=False)

    def query_data():
        today =  date.today()
        location = '/c/GoogleDrive/Springboard/airflow-mini-project/data/' + "{:%Y-%m-%d}".format(today) + '/*.csv'
        filenames = glob(location)
        # stocks_df = [pd.read_csv(f) for f in filenames]
        stocks_df = pd.concat([pd.read_csv(f) for f in glob(location)], ignore_index=True)
        print(stocks_df.shape)



    t0 = BashOperator(
        task_id='create_folder',
        bash_command='mkdir -p $AIRFLOW_HOME/data/"$(date +"%Y-%m-%d")"',
        dag=dag,
    )

    t1 = PythonOperator(
        task_id='get_aapl_data',
        python_callable=get_quotes,
        op_kwargs={"stock_symbol": 'AAPL'},
    )

    t2 = PythonOperator(
        task_id='get_tsla_data',
        python_callable=get_quotes,
        op_kwargs={"stock_symbol": 'TSLA'},
    )

    t3 = BashOperator(
        task_id='move_aapl_file',
        bash_command='mv /c/GoogleDrive/Springboard/airflow-mini-project/data_aapl.csv $AIRFLOW_HOME/data/"$(date +"%Y-%m-%d")"',
        dag=dag,
    )

    t4 = BashOperator(
        task_id='move_tsla_file',
        bash_command='mv /c/GoogleDrive/Springboard/airflow-mini-project/data_tsla.csv $AIRFLOW_HOME/data/"$(date +"%Y-%m-%d")"',
        dag=dag,
    )


    t5 = PythonOperator(
        task_id='query_data',
        python_callable=query_data,
    )
    dag.doc_md = __doc__

    t1.doc_md = """\
    #### Task Documentation
    You can document your task using the attributes `doc_md` (markdown),
    `doc` (plain text), `doc_rst`, `doc_json`, `doc_yaml` which gets
    rendered in the UI's Task Instance Details page.
    ![img](http://montcs.bloomu.edu/~bobmon/Semesters/2012-01/491/import%20soul.png)
    """
    templated_command = """
    {% for i in range(5) %}
        echo "{{ ds }}"
        echo "{{ macros.ds_add(ds, 7)}}"
        echo "{{ params.my_param }}"
    {% endfor %}
    """



    t0 >> t1 >> t3 >> t5
    t0 >> t2 >> t4 >> t5
