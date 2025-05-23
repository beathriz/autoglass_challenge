from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import boto3
import pandas as pd
import io
import os

def converter_inbox_para_bronze():
    s3 = boto3.client('s3')
    bucket_origem = 'inbox-autoglass'
    bucket_destino = 'bronze-autoglass'

    resposta = s3.list_objects_v2(Bucket=bucket_origem)
    arquivos = resposta.get("Contents", [])

    for obj in arquivos:
        nome_arquivo = obj["Key"]

        if not (nome_arquivo.endswith(".csv") or nome_arquivo.endswith(".parquet")):
            continue

        # Baixar o arquivo
        buffer = io.BytesIO()
        s3.download_fileobj(bucket_origem, nome_arquivo, buffer)
        buffer.seek(0)

        # Nome base sem extensão
        nome_base = os.path.splitext(os.path.basename(nome_arquivo))[0]
        novo_caminho = f"{nome_base}.parquet"

        try:
            if nome_arquivo.endswith(".csv"):
                df = pd.read_csv(buffer)
            else:
                df = pd.read_parquet(buffer)

            # Reescreve como Parquet
            novo_buffer = io.BytesIO()
            df.to_parquet(novo_buffer, index=False)
            novo_buffer.seek(0)

            s3.upload_fileobj(novo_buffer, bucket_destino, novo_caminho)
            print(f"Arquivo convertido e movido: {nome_arquivo} -> {novo_caminho}")
        except Exception as e:
            print(f"Erro ao processar {nome_arquivo}: {e}")

# DAG
default_args = {
    'owner': 'beathriz',
    'start_date': datetime(2025, 5, 22),
    'retries': 1,
}

with DAG(
    dag_id='converter_inbox_para_bronze',
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    tags=['s3', 'bronze']
) as dag:

    mover_converter_task = PythonOperator(
        task_id='mover_e_converter_arquivos_s3',
        python_callable=converter_inbox_para_bronze
    )

    mover_converter_task
