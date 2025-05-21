from datetime import datetime
import os
import requests
import pandas as pd

from airflow import DAG
from airflow.operators.python import PythonOperator


def extrair_dados_cnae():
    url = "https://servicodados.ibge.gov.br/api/v2/cnae/classes"
    response = requests.get(url)
    response.raise_for_status()
    
    dados = response.json()
    df = pd.json_normalize(dados)[['id', 'descricao']]

    os.makedirs('/tmp/airflow_data', exist_ok=True)
    df.to_csv('/mnt/c/Users/User/Desktop/CODE/autoglass/cnae_classes.csv', index=False)
    print("Arquivo salvo com sucesso em /mnt/c/Users/User/Desktop/CODE/autoglass/cnae_classes.csv")

with DAG(
    dag_id='extrair_cnae_ibge',
    start_date=datetime(2025, 5, 21),
    schedule_interval=None,
    catchup=False,
    tags=['ibge', 'cnae', 'api']
) as dag:

    tarefa_extrair = PythonOperator(
        task_id='extrair_api_ibge',
        python_callable=extrair_dados_cnae
    )
