"""
Q4 — Databricks Connect: Pipeline NYC Taxi no VS Code

Semana 2 | Spark no Databricks

Este script replica a transformação Silver do Q3 usando Databricks Connect.
Roda LOCALMENTE no VS Code, mas executa o Spark no cluster remoto do Databricks.

Como executar:
    # Ativar o venv com databricks-connect
    .\.venv\Scripts\activate

    # Executar o script
    python exercicios/semana02_spark_databricks/Q4_taxi_local.py

Diferenças em relação ao notebook Databricks:
    - display() não existe → usar .show() ou .toPandas()
    - dbutils não existe → não há acesso a dbutils.fs, widgets, etc.
    - spark.read.csv() busca o arquivo no DBFS do workspace remoto (não local)
    - toPandas() traz os dados do cluster Databricks para a memória local do VS Code
"""

from databricks.connect import DatabricksSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField,
    StringType, TimestampType, IntegerType, DoubleType,
)

# ── Conexão ─────────────────────────────────────────────────────────────────
# profile("cezar_databricks") usa o perfil do ~/.databrickscfg
# serverless(True) é necessário pois o workspace usa Serverless compute
spark = (
    DatabricksSession.builder
    .profile("cezar_databricks")
    .serverless(True)
    .getOrCreate()
)

print(f"Conectado ao Databricks Runtime: {spark.version}\n")

# ── Schema explícito ─────────────────────────────────────────────────────────
# ATENÇÃO: Spark lê CSV por POSIÇÃO com schema explícito — não por nome de coluna.
# O Yellow Taxi 2019 tem 18 colunas; um schema incompleto causa mapeamento errado
# (ex: tip_amount na posição 7 leria store_and_fwd_flag → "Y"/"N" → null como Double).
schema_taxi = StructType([
    StructField("vendor_id",             StringType(),    True),  # col  1
    StructField("pickup_datetime",       TimestampType(), True),  # col  2
    StructField("dropoff_datetime",      TimestampType(), True),  # col  3
    StructField("passenger_count",       IntegerType(),   True),  # col  4
    StructField("trip_distance",         DoubleType(),    True),  # col  5
    StructField("rate_code_id",          IntegerType(),   True),  # col  6
    StructField("store_and_fwd_flag",    StringType(),    True),  # col  7
    StructField("pu_location_id",        IntegerType(),   True),  # col  8
    StructField("do_location_id",        IntegerType(),   True),  # col  9
    StructField("payment_type",          IntegerType(),   True),  # col 10
    StructField("fare_amount",           DoubleType(),    True),  # col 11 ← posição real
    StructField("extra",                 DoubleType(),    True),  # col 12
    StructField("mta_tax",               DoubleType(),    True),  # col 13
    StructField("tip_amount",            DoubleType(),    True),  # col 14 ← posição real
    StructField("tolls_amount",          DoubleType(),    True),  # col 15
    StructField("improvement_surcharge", DoubleType(),    True),  # col 16
    StructField("total_amount",          DoubleType(),    True),  # col 17 ← posição real
    StructField("congestion_surcharge",  DoubleType(),    True),  # col 18
])

ARQUIVO_TAXI = (
    "dbfs:/databricks-datasets/nyctaxi/tripdata/yellow/"
    "yellow_tripdata_2019-12.csv.gz"
)

# ── Bronze: leitura ──────────────────────────────────────────────────────────
# O arquivo é lido do DBFS do workspace Databricks, não da máquina local
df_raw = spark.read.csv(ARQUIVO_TAXI, schema=schema_taxi, header=True)
print(f"[Bronze] Total de registros: {df_raw.count():,}")

# ── Silver: limpeza + enriquecimento ─────────────────────────────────────────
df_silver = (
    df_raw
    .filter(F.col("fare_amount") > 0)
    .filter(F.col("trip_distance") > 0)
    .filter(F.col("passenger_count").between(1, 6))
    .filter(F.col("pickup_datetime").isNotNull())
    .withColumn(
        "duracao_min",
        (F.unix_timestamp("dropoff_datetime") - F.unix_timestamp("pickup_datetime")) / 60,
    )
    .withColumn("hora_pickup", F.hour("pickup_datetime"))
    .filter(F.col("duracao_min").between(1, 180))
)

print(f"[Silver] Após limpeza: {df_silver.count():,}\n")

# ── Gold: agregação por hora ──────────────────────────────────────────────────
df_gold = (
    df_silver
    .groupBy("hora_pickup")
    .agg(
        F.count("*").alias("qtd_viagens"),
        F.round(F.avg("fare_amount"),   2).alias("tarifa_media"),
        F.round(F.avg("tip_amount"),    2).alias("gorjeta_media"),
        F.round(F.sum("total_amount"),  0).alias("receita_total"),
    )
    .orderBy("hora_pickup")
)

# toPandas() traz os dados do cluster para a memória local do VS Code
# Usar apenas em resultados pequenos (Gold/agregado) — NUNCA no df_raw inteiro
df_pandas = df_gold.toPandas()

print("[Gold] Métricas por hora do dia:")
print(df_pandas.to_string(index=False))

print("\n--- Perguntas para reflexão ---")
print("""
1. spark.read.csv() leu o arquivo de onde?
   → Do DBFS do workspace Databricks (dbfs:/databricks-datasets/...).
     O arquivo NUNCA passou pela máquina local — o cluster remoto fez a leitura
     diretamente no armazenamento do Databricks.

2. toPandas() trouxe os dados para onde?
   → Para a memória RAM do processo Python LOCAL (VS Code).
     Após o toPandas(), df_pandas é um objeto pandas comum na sua máquina.
     Por isso só deve ser usado em resultados pequenos (Gold/agregado) —
     trazer df_raw inteiro (~7 M linhas) estouraria a memória local.

3. O que NÃO funciona aqui vs notebook?
   → display()        : função exclusiva do ambiente Databricks — usar .show() ou .toPandas()
   → dbutils          : não existe fora do Databricks — sem dbutils.fs, widgets, secrets
   → %magic commands  : %%sql, %md, %sh etc. são magics do IPython/Databricks, não Python puro
   → spark.sparkContext: JVM_ATTRIBUTE_NOT_SUPPORTED no Serverless (mesmo no notebook)
""")
