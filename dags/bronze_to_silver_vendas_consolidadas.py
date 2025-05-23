from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import boto3
import os
import glob
import io
import pandas as pd

def enviar_vendas_para_s3():
    diretorio_parquet = "/tmp/vendas_estado_categoria_temp"
    bucket_destino = "silver-autoglass"
    nome_arquivo_s3 = "vendas_estado_categoria.parquet"

    # Encontrar o arquivo part-*.parquet
    lista_arquivos = glob.glob(os.path.join(diretorio_parquet, "part-*.parquet"))
    if not lista_arquivos:
        raise FileNotFoundError("Arquivo Parquet não encontrado.")

    arquivo_parquet = lista_arquivos[0]

    # Ler com Pandas
    df = pd.read_parquet(arquivo_parquet)

    # Enviar para o S3
    buffer = io.BytesIO()
    df.to_parquet(buffer, index=False)
    buffer.seek(0)

    s3 = boto3.client('s3')
    s3.upload_fileobj(buffer, bucket_destino, nome_arquivo_s3)

    print(f"Arquivo enviado para s3://{bucket_destino}/{nome_arquivo_s3}")

# Configuração da DAG
default_args = {
    'owner': 'beathriz',
    'start_date': datetime(2025, 5, 23),
    'retries': 1,
}

with DAG(
    dag_id='consolidar_vendas_por_estado_categoria',
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    tags=['s3', 'silver', 'vendas']
) as dag:

    enviar_task = PythonOperator(
        task_id='enviar_vendas_s3',
        python_callable=enviar_vendas_para_s3
    )

    enviar_task
