from pyspark.sql import SparkSession
import boto3

# Inicializar Spark com suporte a Delta e S3
spark = SparkSession.builder \
    .appName("silver-to-gold") \
    .config("spark.jars.packages", "io.delta:delta-core_2.12:2.4.0,org.apache.hadoop:hadoop-aws:3.3.1") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()

# Conectar ao S3
s3 = boto3.client("s3")
bucket_silver = "silver-autoglass"
bucket_gold = "gold-autoglass"

# Listar objetos Parquet no bucket Silver
resposta = s3.list_objects_v2(Bucket=bucket_silver)
arquivos = resposta.get("Contents", [])

for obj in arquivos:
    nome_arquivo = obj["Key"]
    
    if nome_arquivo.endswith(".parquet"):
        nome_base = nome_arquivo.replace(".parquet", "")
        caminho_silver = f"s3a://{bucket_silver}/{nome_arquivo}"
        caminho_gold = f"s3a://{bucket_gold}/{nome_base}"

        print(f"Convertendo {caminho_silver} → {caminho_gold}")

        df = spark.read.parquet(caminho_silver)
        df.write.format("delta").mode("overwrite").save(caminho_gold)

spark.stop()
