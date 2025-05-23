from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import pandas as pd
import requests
import boto3
import io

def extrair_e_salvar_em_s3():
    # Extrair dados da API
    url = "https://servicodados.ibge.gov.br/api/v2/cnae/classes"
    response = requests.get(url)
    response.raise_for_status()
    dados = response.json()

    df = pd.json_normalize(dados)[['id', 'descricao']]

    # Converter para Parquet em memória
    buffer = io.BytesIO()
    df.to_parquet(buffer, index=False)
    buffer.seek(0)

    # Fazer upload para o S3
    s3 = boto3.client('s3')
    s3.upload_fileobj(buffer, "inbox-autoglass", "cnae_classes.parquet")

    print("Upload realizado com sucesso no S3: inbox-autoglass/cnae_classes.parquet")

# DAG
default_args = {
    'owner': 'beathriz',
    'start_date': datetime(2025, 5, 21),
    'retries': 1,
}

with DAG(
    dag_id="extrair_cnae_ibge_para_s3",
    default_args=default_args,
    description="Extrai dados da API IBGE e salva no S3 como Parquet",
    schedule_interval=None,
    catchup=False,
    tags=["ibge", "s3"]
) as dag:

    extrair_salvar_task = PythonOperator(
        task_id="extrair_e_enviar_para_s3",
        python_callable=extrair_e_salvar_em_s3
    )

    extrair_salvar_task
