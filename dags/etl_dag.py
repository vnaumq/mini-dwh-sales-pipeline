from airflow.decorators import dag, task
from datetime import datetime, timedelta

def  main():
    print('Hello World')

    return 0

args = {
    'email': [],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 0,
    'params': {
        'priority': 'P0'
    }
}
@dag(dag_id='test_dag',
     schedule= None,
     start_date=datetime(year=2025, month=8, day=26),
     catchup=False,
     dagrun_timeout=timedelta(minutes=59),
     default_args=args,
     tags=['TEST'])
def my_dag():
    @task
    def process():
        main()
    process()
my_dag()