# Semana 2 — Spark no Databricks: clusters, notebooks e DBFS

**Objetivo:** Dominar o ambiente de execução do Databricks — a diferença entre rodar Spark localmente e no Databricks Runtime, e construir um pipeline ETL completo usando os recursos nativos da plataforma.

**Pré-requisitos:**
- Semana 1 concluída (cluster ativo, VS Code conectado)
- SparkSession e DataFrame API básica (TRILHA_DE Semana 9)

---

## Q1 — Databricks Runtime vs Spark local

**Contexto:** O Databricks Runtime (DBR) não é o Spark open-source puro. Ele inclui
otimizações exclusivas. Você precisa saber o que muda.

> ⚠️ **Nota Serverless (validado na prática):**
> No Serverless compute, `spark.databricks.clusterUsageTags.sparkVersion` e
> `spark.databricks.runtime.version` **não estão disponíveis** — o Databricks
> abstrai completamente o runtime e o gerencia de forma automática.
> Isso é esperado e é parte do aprendizado: no Serverless, você não gerencia a versão do DBR.
> As configurações que importam para o seu código (AQE, Delta, shuffle.partitions) **são acessíveis**.

**Tarefa:**

1. No notebook Databricks, execute:
```python
import sys

# clusterUsageTags.sparkVersion não existe no Serverless — usar get_conf_safe()
def get_conf_safe(key, fallback="(não disponível no Serverless)"):
    try:
        return spark.conf.get(key)
    except Exception:
        return fallback

print(f"Spark version : {spark.version}")
print(f"Python version: {sys.version.split()[0]}")

# AQE e Delta — disponíveis no Serverless
print(get_conf_safe("spark.sql.adaptive.enabled"))
print(get_conf_safe("spark.databricks.delta.optimizeWrite.enabled"))
```

2. Compare com um SparkSession local (Databricks Connect no VS Code):
```python
# local_spark_info.py
from databricks.connect import DatabricksSession

# profile + serverless obrigatório para o nosso workspace
spark = (
    DatabricksSession.builder
    .profile("cezar_databricks")
    .serverless(True)
    .getOrCreate()
)

configs = [
    "spark.sql.adaptive.enabled",
    "spark.sql.adaptive.coalescePartitions.enabled",
    "spark.sql.shuffle.partitions",
]
for c in configs:
    print(f"{c} = {spark.conf.get(c, 'não definido')}")
```

3. Liste as diferenças que encontrou e documente no guia_tecnico.md.

**Pergunta:** O `spark.sql.shuffle.partitions` tem valor padrão de 200 no Spark open-source.
Qual é o valor padrão no Databricks Runtime? Por que isso importa para um cluster single-node?

---

## Q2 — dbutils.widgets: parametrização de notebooks

**Contexto:** Em produção, notebooks raramente rodam com parâmetros hardcoded.
Widgets permitem parametrizar notebooks interativamente ou via Jobs.

**Tarefa:**

1. Crie um notebook `parametrizado_etl` com os seguintes widgets:
```python
dbutils.widgets.text("data_inicio", "2026-01-01", "Data de início")
dbutils.widgets.text("data_fim", "2026-12-31", "Data de fim")
dbutils.widgets.dropdown("categoria", "eletronicos",
                          ["eletronicos", "roupas", "alimentos"], "Categoria")
dbutils.widgets.combobox("formato_saida", "delta",
                          ["delta", "parquet", "csv"], "Formato de saída")
```

2. Leia os valores e use-os no pipeline:
```python
data_inicio = dbutils.widgets.get("data_inicio")
data_fim = dbutils.widgets.get("data_fim")
categoria = dbutils.widgets.get("categoria")
formato = dbutils.widgets.get("formato_saida")

print(f"Processando: {categoria} | {data_inicio} → {data_fim} | formato: {formato}")
```

3. Gere dados fictícios filtrados pelos parâmetros:
```python
from pyspark.sql import functions as F
import random
from datetime import date, timedelta

# Gerar 500 pedidos aleatórios
pedidos = [(i,
            random.choice(["eletronicos", "roupas", "alimentos"]),
            round(random.uniform(10, 5000), 2),
            (date(2026, 1, 1) + timedelta(days=random.randint(0, 364))).isoformat()
           ) for i in range(1, 501)]

df = spark.createDataFrame(pedidos, ["id", "categoria", "valor", "data"])

# Filtrar pelos widgets
df_filtrado = (df
    .filter(F.col("categoria") == categoria)
    .filter(F.col("data").between(data_inicio, data_fim))
)
display(df_filtrado.orderBy("data"))
```

4. Salve no DBFS no formato escolhido pelo widget:
```python
output_path = f"/Volumes/workspace/estudos/semana02/{categoria}_{formato}"

if formato == "delta":
    df_filtrado.write.format("delta").mode("overwrite").save(output_path)
elif formato == "parquet":
    df_filtrado.write.parquet(output_path, mode="overwrite")
elif formato == "csv":
    df_filtrado.write.csv(output_path, mode="overwrite", header=True)

print(f"Dados salvos em: {output_path}")
display(dbutils.fs.ls(output_path))
```

---

## Q3 — Pipeline ETL completo no Databricks

**Contexto:** Construir um pipeline de ponta a ponta usando os datasets públicos do Databricks.

**Dataset:** NYC Taxi (disponível em `dbfs:/databricks-datasets/nyctaxi/`)

**Tarefa:**

1. Explore o dataset:
```python
display(dbutils.fs.ls("dbfs:/databricks-datasets/nyctaxi/"))
display(dbutils.fs.ls("dbfs:/databricks-datasets/nyctaxi/tripdata/yellow/"))
```

2. Leia com schema explícito (não use inferSchema em produção!):
```python
from pyspark.sql.types import *

schema_taxi = StructType([
    StructField("vendor_id", StringType(), True),
    StructField("pickup_datetime", TimestampType(), True),
    StructField("dropoff_datetime", TimestampType(), True),
    StructField("passenger_count", IntegerType(), True),
    StructField("trip_distance", DoubleType(), True),
    StructField("fare_amount", DoubleType(), True),
    StructField("tip_amount", DoubleType(), True),
    StructField("total_amount", DoubleType(), True),
])

df_raw = spark.read.csv(
    "dbfs:/databricks-datasets/nyctaxi/tripdata/yellow/yellow_tripdata_2019-12.csv.gz",
    schema=schema_taxi,
    header=True
)
print(f"Total de registros: {df_raw.count():,}")
df_raw.printSchema()
```

3. Aplicar transformações (Silver layer básico):
```python
from pyspark.sql import functions as F

df_clean = (df_raw
    # Remover nulls em colunas críticas
    .filter(F.col("fare_amount").isNotNull())
    .filter(F.col("trip_distance").isNotNull())
    # Filtrar valores inválidos
    .filter(F.col("fare_amount") > 0)
    .filter(F.col("trip_distance") > 0)
    .filter(F.col("passenger_count").between(1, 6))
    # Enriquecer
    .withColumn("duracao_min",
        (F.unix_timestamp("dropoff_datetime") -
         F.unix_timestamp("pickup_datetime")) / 60)
    .withColumn("hora_pickup", F.hour("pickup_datetime"))
    .withColumn("dia_semana", F.dayofweek("pickup_datetime"))
    .withColumn("custo_por_km",
        F.when(F.col("trip_distance") > 0,
               F.col("total_amount") / F.col("trip_distance"))
         .otherwise(None))
    # Remover viagens absurdas
    .filter(F.col("duracao_min").between(1, 180))
    .filter(F.col("duracao_min").isNotNull())
)

print(f"Registros após limpeza: {df_clean.count():,}")
```

4. Análise analítica (Gold layer):
```python
# Análise por hora do dia
df_por_hora = (df_clean
    .groupBy("hora_pickup")
    .agg(
        F.count("*").alias("qtd_viagens"),
        F.avg("fare_amount").alias("tarifa_media"),
        F.avg("trip_distance").alias("distancia_media"),
        F.avg("tip_amount").alias("gorjeta_media"),
        F.sum("total_amount").alias("receita_total")
    )
    .orderBy("hora_pickup")
)
display(df_por_hora)
```

5. Salvar como Delta:
```python
(df_clean.write
    .format("delta")
    .mode("overwrite")
    .partitionBy("hora_pickup")
    .save("/Volumes/workspace/estudos/semana02/taxi_silver"))

# Verificar o que foi criado:
display(dbutils.fs.ls("/Volumes/workspace/estudos/semana02/taxi_silver/"))

# O transaction log!
display(dbutils.fs.ls("/Volumes/workspace/estudos/semana02/taxi_silver/_delta_log/"))
```

6. Consultar com SQL:
```python
# Registrar como view temporária
df_clean.createOrReplaceTempView("taxi_silver")
```

```sql
-- %sql
-- Top 5 horas com maior gorjeta média
SELECT
    hora_pickup,
    ROUND(AVG(tip_amount), 2) AS gorjeta_media,
    COUNT(*) AS qtd_viagens,
    ROUND(SUM(total_amount), 0) AS receita_total
FROM taxi_silver
GROUP BY hora_pickup
ORDER BY gorjeta_media DESC
LIMIT 5
```

---

## Q4 — Databricks Connect: replicar Q3 localmente no VS Code

**Tarefa:** Replique a transformação Silver do Q3 usando Databricks Connect no VS Code
(arquivo `.py` local, executando no cluster remoto).

```python
# semana02_taxi_local.py  →  arquivo: Q4_taxi_local.py
from databricks.connect import DatabricksSession
from pyspark.sql import functions as F
from pyspark.sql.types import *

# profile + serverless obrigatório — getOrCreate() sem args falha no nosso workspace
spark = (
    DatabricksSession.builder
    .profile("cezar_databricks")
    .serverless(True)
    .getOrCreate()
)

schema_taxi = StructType([
    StructField("vendor_id", StringType(), True),
    StructField("pickup_datetime", TimestampType(), True),
    StructField("dropoff_datetime", TimestampType(), True),
    StructField("passenger_count", IntegerType(), True),
    StructField("trip_distance", DoubleType(), True),
    StructField("fare_amount", DoubleType(), True),
    StructField("tip_amount", DoubleType(), True),
    StructField("total_amount", DoubleType(), True),
])

df_raw = spark.read.csv(
    "dbfs:/databricks-datasets/nyctaxi/tripdata/yellow/yellow_tripdata_2019-12.csv.gz",
    schema=schema_taxi,
    header=True
)

# Transformação Silver
df_silver = (df_raw
    .filter(F.col("fare_amount") > 0)
    .filter(F.col("trip_distance") > 0)
    .withColumn("hora_pickup", F.hour("pickup_datetime"))
)

# Aqui usamos toPandas() para visualizar localmente
# (display() só funciona em notebooks Databricks)
print(df_silver.groupBy("hora_pickup").count().orderBy("hora_pickup").toPandas().to_string())
```

**Pergunta para reflexão:**
- O `spark.read.csv()` busca o arquivo de onde? (do DBFS do workspace ou da sua máquina?)
- O `toPandas()` traz os dados para onde?
- Que limitações o Databricks Connect tem vs o notebook Databricks nativo?

---

## Referência

Ver seção **PLATAFORMA DATABRICKS** em [../../documentos/guia_tecnico.md](../../documentos/guia_tecnico.md)
