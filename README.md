# autoglass_challenge
**Desafio Prático de Engenharia de Dados:** desenvolver um pipeline de dados para consolidar, transformar e disponibilizar informações estratégicas sobre vendas, produtos e categorias econômicas, utilizando dados do Kaggle e da API CNAE do IBGE, com armazenamento em formato Delta para análise em ambiente Snowflake.

# Resumo da Arquitetura de Dados

Este pipeline organiza o fluxo de dados da API do IBGE em quatro camadas: Inbox, Bronze, Silver e Gold. A orquestração é feita com Airflow e as transformações com PySpark em notebooks Jupyter e os dados são armazenados em Buckets S3. 



## 1. Inbox – Extração da API IBGE

- **Airflow extrai dados da API do IBGE**
- **Armazena no bucket:** `inbox-autoglass`
- **Script:** `extrair_cnae_ibge_para_s3.py`


## 2. Bronze – Padronização dos Arquivos

- **Airflow move e padroniza os arquivos da Inbox**
- **Salva no bucket:** `bronze-autoglass`
- **DAG:** `inbox-to-bronze.py`


## 3. Silver – Transformações

- **Notebook:** `transform_bronze_to_silver.ipynb`
- **Transformações feitas com PySpark**
- **Salva no bucket:** `silver-autoglass`
- **DAGs:**
  - `bronze_to_silver.py`
  - `bronze_to_silver_faixa_valor.py`
  - `bronze_to_silver_total_estado.py`
  - `bronze_to_silver_vendas_consolidadas.py`


## 4. Gold – Conversão para Delta e Disponibilização

- **Conversão dos arquivos Silver para formato Delta**
- **Notebook e DAG:** `silver_to_gold.py`
- **Salva no bucket:** `gold-autoglass`


## Perguntas e Reflexões
1. Quais perguntas você faria para o time de analistas de negócio ou para os 
stakeholders sobre os requisitos?
- Quais demandas esses arquivos atenderão?
- Quem é o responsável pelo planejamento ?
- Aonde posso buscar mais detalhes do projeto?
- Algum dado precisa de controle de acesso?

2. Que outras melhorias ou extensões poderiam ser feitas nesse pipeline?
- Usaria o Azure Databricks para otimizar todo o ETL, dese a extração até a disponibilização para o Snowflake.
- Faria um monitoramento com Prometheus + Grafana para monitorar os dados.
- Colocaria Notificações e alertas para falhas, validações etc

3. Quais diferentes técnicas ou ferramentas você considera que poderiam ser 
utilizadas para resolver o mesmo problema?
- No lugar do S3 usaria o Azure Data Lake, no caso de uso do Databricks ou o MinIO em caso de redução de custo
- Trocaria o uso do Snowflake pelo BigQuery, Athena ou outros. 
     

