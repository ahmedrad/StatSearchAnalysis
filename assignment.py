from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2017, 9, 10),
    # 'schedule_interval': '@daily',
    'email': ['ahmedrad@gmail.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

dag = DAG('assignment', default_args=default_args, schedule_interval=timedelta(1))
dag.catchup = False

# t1, t2 and t3 are examples of tasks created by instantiating operators
t1 = BashOperator(
    task_id='extract__crawler_report_csv',
    bash_command='ipython ~/airflow/statSearch/ETL/extract__crawler_report_csv.py',
    dag=dag)

t2 = BashOperator(
    task_id='compute__under10RankingCount_perUrl_forAllTimePeriod',
    bash_command='ipython ~/airflow/statSearch/ETL/compute__under10RankingCount_perUrl_forAllTimePeriod.py',
    dag=dag)

t2.set_upstream(t1)

t3 = BashOperator(
    task_id='load__under10RankingCount_perUrl_forAllTimePeriod',
    bash_command='ipython ~/airflow/statSearch/ETL/load__under10RankingCount_perUrl_forAllTimePeriod.py',
    dag=dag)

t3.set_upstream(t2)

t4 = BashOperator(
    task_id='compute__topUrlChangesCount_perKeywordInfo_forAllTimePeriod',
    bash_command='ipython ~/airflow/statSearch/ETL/compute__topUrlChangesCount_perKeywordInfo_forAllTimePeriod.py',
    dag=dag)

t4.set_upstream(t1)

t5 = BashOperator(
    task_id='load__topUrlChangesCount_perKeywordInfo_forAllTimePeriod',
    bash_command='ipython ~/airflow/statSearch/ETL/load__topUrlChangesCount_perKeywordInfo_forAllTimePeriod.py',
    dag=dag)

t5.set_upstream(t4)

t6 = BashOperator(
    task_id='compute__deviceRankingDifference_perDay',
    bash_command='ipython ~/airflow/statSearch/ETL/compute__deviceRankingDifference_perDay.py',
    dag=dag)

t6.set_upstream(t1)

t7 = BashOperator(
    task_id='load__deviceRankingDifference_perDay',
    bash_command='ipython ~/airflow/statSearch/ETL/load__deviceRankingDifference_perDay.py',
    dag=dag)

t7.set_upstream(t6)
