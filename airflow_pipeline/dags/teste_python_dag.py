from airflow.models import DAG
from airflow.utils.dates import days_ago
from airflow.operators.empty import EmptyOperator
from airflow.decorators import task

with DAG(
  'teste_python',
  start_date=days_ago(1),
  schedule_interval='@daily',
) as dag:
    
    tarefa_1 = EmptyOperator(task_id='tarefa_1')
    
    @task(task_id = 'task_cumprimentos')
    def cumprimentos(x):
        data_interval_end = '{{data_interval_end.strftime("%Y-%m-%d")}}'
        print(f'Boas-vindas ao Airflow!')
        print(f'{data_interval_end}')

    tarefa_2 = cumprimentos(5)
    
    tarefa_1 >> tarefa_2

