from airflow import DAG 
from airflow.operators.bash import BashOperator
from airflow.decorators import task
from airflow.macros import ds_add
import pendulum
import os
from os.path import join
import pandas as pd
from dotenv import load_dotenv

with DAG(
    "dados_climaticos",
    start_date=pendulum.datetime(2023, 9, 22, tz="UTC"),
    schedule_interval='0 0 * * 1', # executar toda segunda feira
) as dag:

    tarefa_1 = BashOperator(
        task_id = 'cria_pasta',
        bash_command = 'mkdir -p "/Users/leandrosouza/Library/CloudStorage/GoogleDrive-leandro.souza.159@gmail.com/My Drive/Projetos Python/data_pipeline/semana={{ ds }}"'
    )

    @task(task_id = 'extrai_dados')
    def extrai_dados():
        load_dotenv()
        city = 'Boston'
        key = os.getenv("WEATHER_API_KEY")
        begin_date = '{{ ds }}'
        end_date = ds_add('{{ ds }}', 7)

        #URL = join('https://weather.visualcrossing.com/VisualCrossingWebServices/rest/services/timeline/',
        #    f'{city}/{data_interval_end}/{ds_add(data_interval_end, 7)}?unitGroup=metric&include=days&key={key}&contentType=csv')

        #dados = pd.read_csv(URL)

        #file_path = f'/Users/leandrosouza/Library/CloudStorage/GoogleDrive-leandro.souza.159@gmail.com/My Drive/Projetos Python/data_pipeline/semana={data_interval_end}/'

        #dados.to_csv(file_path + 'dados_brutos.csv')
        #dados[['datetime','tempmin', 'temp', 'tempmax']].to_csv(file_path + 'temperaturas.csv')
        #dados[['datetime', 'description', 'icon']].to_csv(file_path + 'condicoes.csv')

    extrai_dados_task = extrai_dados()
    tarefa_1 >> extrai_dados_task