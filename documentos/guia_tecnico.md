# Guia Técnico — Trilha Databricks
> Referência técnica principal — conceitos aprofundados, padrões de produção e exercícios resolvidos
> Atualizado progressivamente ao longo da trilha

---

## ÍNDICE

- [PLATAFORMA DATABRICKS](#plataforma-databricks)
  - [Arquitetura do Workspace](#arquitetura-do-workspace)
  - [Clusters: tipos e configuração](#clusters-tipos-e-configuração)
  - [DBFS vs Volumes vs Cloud Storage](#dbfs-vs-volumes-vs-cloud-storage)
  - [dbutils: o canivete suíço do Databricks](#dbutils-o-canivete-suíço-do-databricks)
  - [Magic Commands](#magic-commands)
  - [VS Code Extension e Databricks Connect](#vs-code-extension-e-databricks-connect)
- [DELTA LAKE](#delta-lake)
  - [Por que Delta Lake existe](#por-que-delta-lake-existe)
  - [Transaction Log: como o ACID funciona](#transaction-log-como-o-acid-funciona)
  - [DML: UPDATE, DELETE, MERGE](#dml-update-delete-merge)
  - [Time Travel](#time-travel)
  - [OPTIMIZE e Z-ORDER](#optimize-e-z-order)
  - [VACUUM](#vacuum)
  - [Schema Evolution](#schema-evolution)
  - [Liquid Clustering](#liquid-clustering)
- [MEDALLION ARCHITECTURE](#medallion-architecture)
  - [Bronze Layer](#bronze-layer)
  - [Silver Layer](#silver-layer)
  - [Gold Layer](#gold-layer)
  - [Auto Loader](#auto-loader)
- [DATABRICKS SQL](#databricks-sql)
  - [SQL Warehouses](#sql-warehouses)
  - [QUALIFY: window function filtering](#qualify-window-function-filtering)
  - [Data Skipping e Bloom Filters](#data-skipping-e-bloom-filters)
- [UNITY CATALOG](#unity-catalog)
  - [Hierarquia de objetos](#hierarquia-de-objetos)
  - [Managed vs External Tables](#managed-vs-external-tables)
  - [GRANT e controle de acesso](#grant-e-controle-de-acesso)
  - [Row Filters e Column Masks](#row-filters-e-column-masks)
- [WORKFLOWS E DLT](#workflows-e-dlt)
  - [Databricks Workflows](#databricks-workflows)
  - [Delta Live Tables](#delta-live-tables)
  - [DLT Expectations](#dlt-expectations)
- [MLFLOW](#mlflow)
  - [Experiment Tracking](#experiment-tracking)
  - [Model Registry](#model-registry)
  - [Feature Store](#feature-store)
- [PERFORMANCE](#performance)
  - [Photon Engine](#photon-engine)
  - [Adaptive Query Execution (AQE)](#adaptive-query-execution-aqe)
  - [Delta Cache vs Spark Cache](#delta-cache-vs-spark-cache)
- [DATABRICKS ASSET BUNDLES](#databricks-asset-bundles)
- [CERTIFICAÇÃO — Pontos críticos](#certificação--pontos-críticos)

---

## PLATAFORMA DATABRICKS

### Arquitetura do Workspace

O Databricks Workspace é dividido em dois planos:

```
┌──────────────────────────────────────────────────────────┐
│  CONTROL PLANE (gerenciado pela Databricks)              │
│  - Web application (UI)                                  │
│  - Job scheduler                                         │
│  - Cluster manager                                       │
│  - Unity Catalog metastore                               │
│  - MLflow tracking server                                │
└──────────────────────────┬───────────────────────────────┘
                           │ API calls
┌──────────────────────────▼───────────────────────────────┐
│  DATA PLANE (na sua cloud — AWS, Azure, GCP)             │
│  - Clusters (VMs com Spark)                              │
│  - DBFS (armazenamento padrão)                           │
│  - Notebooks executando                                  │
│  - Delta tables no cloud storage (S3/ADLS/GCS)          │
└──────────────────────────────────────────────────────────┘
```

**Por que essa separação importa?**
- Seus dados ficam na sua cloud, não nos servidores da Databricks
- O control plane só vê metadados e instruções
- Implicação de segurança: seus dados nunca saem do seu ambiente cloud

---

### Clusters: tipos e configuração

**All-Purpose Clusters (Interactive)**
```python
# Características:
# - Criado manualmente pelo usuário
# - Fica ativo até ser encerrado (caro se esquecer ligado!)
# - Compartilhado entre múltiplos usuários/notebooks
# - Ideal para: desenvolvimento, exploração interativa, notebooks

# Configuração típica para desenvolvimento:
{
  "cluster_name": "dev-cluster",
  "spark_version": "15.4.x-scala2.12",  # LTS recomendado
  "autotermination_minutes": 30,          # SEMPRE configurar!
  "num_workers": 0,                       # single-node para dev
  "node_type_id": "i3.xlarge"
}
```

**Job Clusters (Automated)**
```python
# Características:
# - Criado automaticamente pelo scheduler quando o job inicia
# - Destruído automaticamente quando o job termina
# - Isolado para aquele job específico
# - Ideal para: produção, jobs agendados (mais barato que all-purpose)

# Regra de ouro:
# DEV → All-Purpose (conveniência)
# PROD → Job Cluster (custo + isolamento)
```

**Serverless Compute**
```
Características:
  - Sem startup de cluster — inicia em < 5 segundos
  - Databricks gerencia o runtime completamente (versão DBR automática)
  - Isolado por sessão — sem estado compartilhado entre usuários
  - Ideal para: desenvolvimento interativo no Free Trial, notebooks exploratórios

O que NÃO está disponível no Serverless (vs All-Purpose):
  - sc (SparkContext) → NotImplementedError
  - spark.databricks.clusterUsageTags.* → CONFIG_NOT_AVAILABLE
  - spark.databricks.runtime.version → CONFIG_NOT_AVAILABLE
  - dbutils.library.install() → não suportado
  - spark.catalog.clearCache() → NOT_SUPPORTED_WITH_SERVERLESS
  - spark.databricks.delta.retentionDurationCheck.enabled → CONFIG_NOT_AVAILABLE (VACUUM RETAIN 0 HOURS bloqueado)
  - spark.conf.set() para configs Delta de retenção → CONFIG_NOT_AVAILABLE
  - input_file_name() → UC_COMMAND_NOT_SUPPORTED — usar F.col("_metadata.file_path")
  - CREATE TABLE ... LOCATION '/Volumes/...' → INVALID_PARAMETER_VALUE (Volume path inválido como LOCATION)
    → LOCATION só aceita cloud paths (s3://, abfss://); para Volumes usar saveAsTable()

O que FUNCIONA normalmente no Serverless:
  - spark (SparkSession) completo
  - spark.conf.get/set para AQE, Delta, shuffle.partitions
  - dbutils.fs, dbutils.widgets, dbutils.notebook, dbutils.secrets
  - %sql, %fs, %sh, %md, %run magic commands
  - display(), createOrReplaceTempView()
  - Delta Lake (leitura e escrita)
  - Unity Catalog (GRANT, Volumes, schemas)

Por que a versão do DBR não aparece no Serverless?
  O Serverless é efêmero por design: não existe um cluster fixo com ID/nome.
  A Databricks rotaciona a versão do runtime automaticamente para a última estável.
  Você sempre roda na versão mais recente sem precisar gerenciar isso.
```

**Free Trial — recursos disponíveis:**
```
✅ Serverless Compute (instantâneo, sem startup de cluster)
✅ All-Purpose e Job Clusters multi-node
✅ Photon Engine
✅ Unity Catalog completo (3 níveis: catalog > schema > table)
✅ Workflows (Jobs agendados, DAGs de tasks)
✅ Delta Live Tables (DLT)
✅ SQL Warehouses (Serverless e Classic)
✅ MLflow gerenciado
✅ Databricks Repos (Git integration)
✅ Databricks Assistant (IA no notebook)
✅ Spark, Delta Lake, Auto Loader
```

---

### DBFS vs Volumes vs Cloud Storage

```
DBFS (Databricks File System):
  - Sistema de arquivos virtual que mapeia para cloud storage
  - dbfs:/ = caminho abstrato que aponta para S3/ADLS/GCS por baixo
  - Legado: ainda funciona, mas Unity Catalog Volumes é o novo padrão
  - Acessível via dbutils.fs e %fs magic command

Unity Catalog Volumes (novo padrão):
  - Armazenamento de arquivos não-tabulares no Unity Catalog
  - Caminho: /Volumes/<catalog>/<schema>/<volume>/
  - Governança: GRANT/REVOKE como tabelas
  - Managed volume: UC gerencia o storage
  - External volume: aponta para S3/ADLS existente

Cloud Storage Direto:
  - s3://bucket/path (AWS)
  - abfss://container@account.dfs.core.windows.net/path (Azure)
  - gs://bucket/path (GCP)
  - Requer configuração de credenciais (Instance Profile / Service Principal)
```

```python
# DBFS — acesso via Python
dbutils.fs.ls("dbfs:/")
dbutils.fs.ls("dbfs:/FileStore/")     # arquivos via UI upload
spark.read.csv("dbfs:/FileStore/meuarquivo.csv")

# Volumes (Unity Catalog)
spark.read.csv("/Volumes/main/landing/raw/meuarquivo.csv")

# Caminhos equivalentes — DBFS vs filesystem local do driver
dbfs_path = "dbfs:/user/data/arquivo.parquet"
driver_path = "/dbfs/user/data/arquivo.parquet"  # para bibliotecas que não entendem dbfs://
```

---

### dbutils: o canivete suíço do Databricks

```python
# ─── dbutils.fs — manipulação de arquivos ───────────────────────────────────
dbutils.fs.ls("dbfs:/")                         # listar diretório
dbutils.fs.mkdirs("dbfs:/tmp/meus_dados/")      # criar diretório
dbutils.fs.cp("dbfs:/origem/", "dbfs:/destino/", recurse=True)
dbutils.fs.mv("dbfs:/antigo/", "dbfs:/novo/")
dbutils.fs.rm("dbfs:/tmp/arquivo.parquet", recurse=True)
dbutils.fs.put("dbfs:/tmp/test.txt", "conteúdo aqui", overwrite=True)
display(dbutils.fs.ls("dbfs:/"))                # versão tabular do ls

# ─── dbutils.notebook — controle de flow entre notebooks ────────────────────
dbutils.notebook.run("./outro_notebook", timeout_seconds=60,
                     arguments={"param1": "valor1"})  # rodar outro notebook
dbutils.notebook.exit("mensagem de saída")  # retornar valor para o chamador

# ─── dbutils.widgets — parametrização ───────────────────────────────────────
dbutils.widgets.text("data_inicio", "2026-01-01", "Data Início")
dbutils.widgets.dropdown("ambiente", "dev", ["dev", "staging", "prod"])
data = dbutils.widgets.get("data_inicio")   # ler o valor

# ─── dbutils.secrets — credenciais seguras ──────────────────────────────────
# NUNCA coloque senhas hardcoded em notebooks!
# Usar sempre dbutils.secrets + Databricks Secret Scope
senha = dbutils.secrets.get(scope="meu-scope", key="db-password")
```

---

### Magic Commands

```python
# ─── Language switching ──────────────────────────────────────────────────────
%python   # Python (default)
%sql      # SQL — retorna resultado como tabela
%scala    # Scala
%r        # R
%md       # Markdown — formata célula como texto rico

# ─── File system ─────────────────────────────────────────────────────────────
%fs ls dbfs:/                          # equivalente a dbutils.fs.ls()
%fs cp dbfs:/origem dbfs:/destino

# ─── Shell ───────────────────────────────────────────────────────────────────
%sh pip list                           # rodar no driver node
%sh cat /etc/os-release               # info do sistema

# ─── Run outro notebook ──────────────────────────────────────────────────────
%run ./utils/helpers                  # importar funções de outro notebook
                                      # (diferente de dbutils.notebook.run!)

# Diferença crítica:
# %run → executa no mesmo escopo (variáveis ficam disponíveis)
# dbutils.notebook.run → executa em sub-escopo separado
```

---

### VS Code Extension e Databricks Connect

**Setup VS Code + Databricks Extension:**

```powershell
# 1. Instalar extensão no VS Code
# Buscar: "Databricks" (publisher: Databricks)
# ou: code --install-extension databricks.databricks

# 2. Configurar conexão
# Ctrl+Shift+P → "Databricks: Configure Workspace"
# Host: https://dbc-962e23c9-e80d.cloud.databricks.com
# Token: User Settings → Access Tokens → Generate New Token

# 3. Instalar Databricks Connect no ambiente local
pip install databricks-connect

# 4. Configurar Databricks Connect
databricks auth configure  # salva em ~/.databrickscfg
# ou via env vars:
# DATABRICKS_HOST=https://dbc-962e23c9-e80d.cloud.databricks.com
# DATABRICKS_TOKEN=dapi...
```

```python
# Usar Databricks Connect no VS Code — SparkSession remota
from databricks.connect import DatabricksSession

spark = DatabricksSession.builder.getOrCreate()
# ^ isso conecta ao cluster configurado na extensão VS Code

df = spark.range(100).toDF("id")
df.show()  # executa no cluster remoto, resultado volta para o VS Code

# Para notebooks locais (.ipynb no VS Code):
# O kernel Python usa o cluster remoto para operações Spark
# Pandas, MLflow e outras libs rodam localmente
```

**Limitações do Databricks Connect (não dependem do plano):**
```
✅ SparkSession conecta ao cluster remoto
✅ DataFrame API completa
✅ Delta Lake read/write
✅ MLflow tracking
❌ dbutils (não disponível via Connect — usar databricks-sdk)
❌ %magic commands (são do notebook, não do Connect)
❌ display() (usar .show() ou pandas)
```

---

## DELTA LAKE

### Por que Delta Lake existe

```
Problema com Data Lakes tradicionais (Parquet puro):

1. SEM ACID:
   - Dois jobs gravando ao mesmo tempo = dados corrompidos
   - Job que falhou no meio = dados incompletos no lake

2. SEM UPDATE/DELETE:
   - Para "deletar" uma linha, você reescreve o arquivo inteiro manualmente
   - LGPD/GDPR (right to be forgotten) é um pesadelo

3. SEM SCHEMA ENFORCEMENT:
   - Alguém muda o schema do CSV fonte → você lê lixo sem saber

4. SEM TIME TRAVEL:
   - "Como eram os dados semana passada?" → impossível
   - Reproducibilidade de modelos de ML → impossível

Delta Lake resolve TODOS esses problemas adicionando um
Transaction Log sobre arquivos Parquet.
```

---

### Transaction Log: como o ACID funciona

```
Estrutura física de uma tabela Delta:
  /minha_tabela/
  ├── _delta_log/
  │   ├── 00000000000000000000.json   ← commit 0 (CREATE TABLE)
  │   ├── 00000000000000000001.json   ← commit 1 (INSERT)
  │   ├── 00000000000000000002.json   ← commit 2 (UPDATE)
  │   ├── 00000000000000000003.json   ← commit 3 (DELETE)
  │   └── 00000000000000000010.checkpoint.parquet  ← checkpoint a cada 10 commits
  ├── part-00000-abc123.parquet
  ├── part-00001-def456.parquet
  └── part-00002-ghi789.parquet
```

```python
# Conteúdo de um commit JSON (simplificado):
{
  "commitInfo": {
    "timestamp": 1716000000000,
    "operation": "WRITE",
    "operationParameters": {"mode": "Append"}
  },
  "add": {
    "path": "part-00000-abc123.parquet",
    "stats": '{"numRecords":1000, "minValues":{"data":"2026-01-01"}}',
    "size": 524288
  }
}

# Como o ACID é garantido:
# - Atomicity: commit só é válido quando o JSON é escrito com sucesso
# - Consistency: schema validation antes de escrever
# - Isolation: Optimistic Concurrency Control (OCC)
# - Durability: arquivos Parquet + log são persistentes em cloud storage
```

```python
# Ver o transaction log de uma tabela
spark.sql("DESCRIBE HISTORY minha_tabela").show(truncate=False)

# Output:
# version | timestamp           | operation | operationParameters
# ------- | ------------------- | --------- | -------------------
# 3       | 2026-05-16 10:00:00 | DELETE    | {"predicate": "..."}
# 2       | 2026-05-15 09:00:00 | UPDATE    | {"predicate": "..."}
# 1       | 2026-05-14 08:00:00 | WRITE     | {"mode": "Append"}
# 0       | 2026-05-13 07:00:00 | CREATE TABLE | {}

# Ver detalhes físicos da tabela
spark.sql("DESCRIBE DETAIL minha_tabela").show(truncate=False)
```

---

### DML: UPDATE, DELETE, MERGE

```python
from delta.tables import DeltaTable

# ─── UPDATE ──────────────────────────────────────────────────────────────────
spark.sql("""
    UPDATE vendas
    SET status = 'cancelado'
    WHERE data_pedido < '2026-01-01' AND status = 'pendente'
""")

# ─── DELETE ──────────────────────────────────────────────────────────────────
spark.sql("""
    DELETE FROM clientes
    WHERE email = 'usuario@deletar.com'
""")

# ─── MERGE INTO (Upsert) ─────────────────────────────────────────────────────
# O operador mais importante do Delta — suporta INSERT, UPDATE, DELETE em uma operação

spark.sql("""
    MERGE INTO clientes AS target
    USING novos_clientes AS source
    ON target.id = source.id
    WHEN MATCHED AND source.ativo = false THEN DELETE
    WHEN MATCHED THEN UPDATE SET
        target.email = source.email,
        target.nome = source.nome,
        target.atualizado_em = current_timestamp()
    WHEN NOT MATCHED THEN INSERT (id, nome, email, criado_em)
        VALUES (source.id, source.nome, source.email, current_timestamp())
""")

# ─── MERGE via Python API ────────────────────────────────────────────────────
target = DeltaTable.forName(spark, "clientes")

(target.alias("t")
    .merge(
        source=novos_clientes_df.alias("s"),
        condition="t.id = s.id"
    )
    .whenMatchedUpdate(set={
        "email": "s.email",
        "atualizado_em": "current_timestamp()"
    })
    .whenNotMatchedInsertAll()
    .execute()
)

# ─── Métricas do MERGE ───────────────────────────────────────────────────────
# ⚠️ ARMADILHA: hist[0] nem sempre é o MERGE!
# Se um OPTIMIZE ou outro comando foi executado depois, hist[0] captura esse.
# Use busca explícita pela operação:
hist = spark.sql("DESCRIBE HISTORY delta.`/path/tabela`").collect()
entrada_merge = next((h for h in hist if h["operation"] == "MERGE"), None)
if entrada_merge:
    m = entrada_merge["operationMetrics"]
    print(m["numTargetRowsInserted"])  # inseridos
    print(m["numTargetRowsUpdated"])   # atualizados
    print(m["numTargetRowsDeleted"])   # deletados
```

---

### Time Travel

```python
# ─── Ler versão específica ────────────────────────────────────────────────────
# Por número de versão
df_ontem = spark.read.format("delta").option("versionAsOf", 5).load("/path/tabela")

# Por timestamp
df_semana_passada = (spark.read.format("delta")
    .option("timestampAsOf", "2026-05-09")
    .load("/path/tabela"))

# SQL
spark.sql("SELECT * FROM vendas VERSION AS OF 3")
spark.sql("SELECT * FROM vendas TIMESTAMP AS OF '2026-05-01'")

# ─── RESTORE ─────────────────────────────────────────────────────────────────
spark.sql("RESTORE TABLE vendas TO VERSION AS OF 5")
spark.sql("RESTORE TABLE vendas TO TIMESTAMP AS OF '2026-05-01'")

# ─── Casos de uso reais ───────────────────────────────────────────────────────
# 1. Auditoria: "quais registros existiam antes do DELETE?"
# 2. Rollback: reverter UPDATE errado em produção
# 3. ML reproducibility: garantir que o modelo foi treinado com exatamente esses dados
# 4. GDPR audit: provar que o dado foi deletado (antes vs depois)

# Quantas versões estão disponíveis?
spark.sql("DESCRIBE HISTORY minha_tabela").select("version", "timestamp", "operation").show()
```

---

### OPTIMIZE e Z-ORDER

```python
# ─── O problema dos small files ──────────────────────────────────────────────
# Cada INSERT ou MERGE cria novos arquivos Parquet
# Após N operações: centenas de arquivos de 1 MB cada
# Leitura de 1 bilhão de linhas = abrir 10.000 arquivos pequenos = LENTO

# ─── OPTIMIZE ────────────────────────────────────────────────────────────────
# Compacta arquivos pequenos em arquivos maiores (~1 GB por arquivo)
spark.sql("OPTIMIZE minha_tabela")

# OPTIMIZE com filtro (só compacta uma partição — mais rápido)
spark.sql("OPTIMIZE minha_tabela WHERE data_particao = '2026-05-16'")

# ─── Z-ORDER ─────────────────────────────────────────────────────────────────
# Co-localiza dados relacionados nos mesmos arquivos Parquet
# Resultado: data skipping elimina arquivos irrelevantes na leitura

# Sem Z-ORDER: query com filtro WHERE user_id = 'abc' → lê todos os arquivos
# Com Z-ORDER: query com filtro WHERE user_id = 'abc' → lê apenas 2-3 arquivos

spark.sql("OPTIMIZE eventos ZORDER BY (user_id, event_date)")

# Quando usar Z-ORDER?
# ✅ Coluna que aparece frequentemente em WHERE, JOIN, GROUP BY
# ✅ Coluna com alta cardinalidade (user_id, product_id, not status)
# ⚠️ Máximo 4 colunas (eficiência diminui com mais colunas)
# ❌ Não usar em colunas de particionamento (já estão separadas por pasta)

# ─── OPTIMIZE automático (Delta 2.0+) ────────────────────────────────────────
spark.sql("""
    ALTER TABLE minha_tabela
    SET TBLPROPERTIES (
        'delta.autoOptimize.optimizeWrite' = 'true',
        'delta.autoOptimize.autoCompact' = 'true'
    )
""")
# optimizeWrite: otimiza o tamanho dos arquivos na escrita (não precisa OPTIMIZE manual)
# autoCompact: compacta automaticamente após writes
```

---

### VACUUM

```python
# Delta mantém arquivos antigos para suportar time travel
# Após muito tempo: terabytes de arquivos que ninguém usa mais

# VACUUM remove arquivos que não fazem parte de nenhuma versão
# dentro do retention period (padrão: 7 dias = 168 horas)

spark.sql("VACUUM minha_tabela")                    # retém 7 dias (padrão)
spark.sql("VACUUM minha_tabela RETAIN 168 HOURS")   # explícito
spark.sql("VACUUM minha_tabela RETAIN 0 HOURS DRY RUN")  # simula sem deletar

# ⚠️ CUIDADO: após VACUUM, time travel para versões antigas FALHA
# Sempre conferir o retention period antes de executar em produção

# ⚠️ SERVERLESS / SPARK CONNECT: VACUUM RETAIN 0 HOURS não funciona!
# CONFIG_NOT_AVAILABLE: spark.databricks.delta.retentionDurationCheck.enabled
# Motivo: Serverless bloqueia configs de retenção por política de segurança.
# Solução: executar em cluster All-Purpose com:
#   spark.conf.set("spark.databricks.delta.retentionDurationCheck.enabled", "false")
#   spark.sql("VACUUM tabela RETAIN 0 HOURS")
# Em Serverless: use RETAIN 168 HOURS (padrão) — suficiente para produção.

# Configuração da tabela:
spark.sql("""
    ALTER TABLE minha_tabela
    SET TBLPROPERTIES ('delta.deletedFileRetentionDuration' = 'interval 30 days')
""")
```

---

### Schema Evolution

```python
# ─── Schema enforcement (comportamento padrão) ────────────────────────────────
# Delta REJEITA escrita com schema diferente por padrão
df_novo = spark.createDataFrame([...])  # tem coluna nova que a tabela não tem

# Isso lança AnalysisException:
df_novo.write.format("delta").mode("append").save("/path/tabela")

# ─── mergeSchema: adicionar colunas novas ────────────────────────────────────
# Aceita colunas novas, mantém as existentes
(df_novo.write.format("delta")
    .mode("append")
    .option("mergeSchema", "true")
    .save("/path/tabela"))

# ─── overwriteSchema: substituir schema completamente ────────────────────────
# CUIDADO: remove colunas que existiam e não existem no novo df
(df_novo.write.format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .save("/path/tabela"))

# ─── O que Delta NUNCA permite mesmo com mergeSchema ─────────────────────────
# - Mudar tipo de coluna (int → string): erro
# - Remover colunas (mergeSchema só adiciona, nunca remove)
# Para mudar tipo: precisa reescrever a tabela inteira
```

---

### Liquid Clustering

```python
# Alternativa moderna ao Z-ORDER + OPTIMIZE manual
# Disponível: Databricks Runtime 13.3+, Delta Lake 3.1+

# Criar tabela com Liquid Clustering:
spark.sql("""
    CREATE TABLE eventos
    CLUSTER BY (user_id, event_date)
    USING DELTA
    AS SELECT * FROM staging_eventos
""")

# Diferenças vs Z-ORDER:
# Z-ORDER: requer OPTIMIZE manual periódico para re-clustering
# Liquid Clustering: re-clustering incremental automático a cada escrita
# Liquid Clustering: não tem particionamento — só clustering
# Liquid Clustering: melhor para dados que mudam muito (updates/deletes frequentes)

# OPTIMIZE ainda funciona (aplica clustering pendente):
spark.sql("OPTIMIZE eventos")

# Ver estado do clustering:
spark.sql("DESCRIBE DETAIL eventos").select("clusteringColumns").show()
```

---

## MEDALLION ARCHITECTURE

### Bronze Layer

```python
# Bronze = dados raw exatamente como chegaram
# Regras:
# 1. NUNCA modifique dados no Bronze
# 2. Adicione metadados de ingestão
# 3. Guarde o schema original (mesmo se tiver erros)
# 4. Estratégia: append-only (novos dados = novos arquivos, nunca sobrescreve)

from pyspark.sql import functions as F

def ingerir_bronze(df_raw, source_name: str, batch_id: str):
    """Padrão de ingestão Bronze."""
    return (df_raw
        .withColumn("_ingest_timestamp", F.current_timestamp())
        .withColumn("_source", F.lit(source_name))
        .withColumn("_batch_id", F.lit(batch_id))
        .withColumn("_year", F.year("_ingest_timestamp"))
        .withColumn("_month", F.month("_ingest_timestamp"))
    )

df_bronze = ingerir_bronze(df_raw, "sistema_vendas", "batch_20260516")

(df_bronze.write
    .format("delta")
    .mode("append")                              # append-only!
    .partitionBy("_year", "_month")
    .save("/mnt/bronze/vendas"))
```

---

### Silver Layer

```python
# Silver = limpo, tipado, deduplicado, pronto para análise
# Estratégia: MERGE com deduplicação (evita duplicatas na Bronze)

from delta.tables import DeltaTable

def processar_silver_vendas(spark, caminho_bronze: str, caminho_silver: str):
    """Pipeline Bronze → Silver com MERGE."""

    df_novos = (spark.read.format("delta").load(caminho_bronze)
        # Filtrar apenas dados do último batch (incremental)
        .filter(F.col("_batch_id") == obter_ultimo_batch())
        # Limpar e tipar
        .withColumn("valor", F.col("valor").cast("decimal(10,2)"))
        .withColumn("data_venda", F.to_date("data_venda_str", "yyyy-MM-dd"))
        .filter(F.col("valor").isNotNull())       # remover nulls críticos
        .filter(F.col("valor") > 0)               # validação de negócio
        # Remover duplicatas dentro do batch
        .dropDuplicates(["id_venda"])
    )

    if DeltaTable.isDeltaTable(spark, caminho_silver):
        tabela_silver = DeltaTable.forPath(spark, caminho_silver)
        (tabela_silver.alias("silver")
            .merge(
                df_novos.alias("bronze"),
                "silver.id_venda = bronze.id_venda"
            )
            .whenMatchedUpdateAll()
            .whenNotMatchedInsertAll()
            .execute()
        )
    else:
        df_novos.write.format("delta").save(caminho_silver)
```

---

### Auto Loader

```python
# Auto Loader: ingestão incremental de arquivos com Structured Streaming
# Detecta novos arquivos automaticamente (S3 Events ou directory listing)
# Ideal para: Bronze layer com chegada contínua de arquivos

df_stream = (spark.readStream
    .format("cloudFiles")                         # Auto Loader
    .option("cloudFiles.format", "json")
    .option("cloudFiles.schemaLocation", "/mnt/checkpoints/schema")
    .option("cloudFiles.inferColumnTypes", "true")
    .load("/mnt/landing/pedidos/")                # pasta monitorada
)

# Escrever na Bronze como stream
(df_stream
    .withColumn("_ingest_timestamp", F.current_timestamp())
    .writeStream
    .format("delta")
    .option("checkpointLocation", "/mnt/checkpoints/bronze_pedidos")
    .outputMode("append")
    .trigger(availableNow=True)                   # processa tudo disponível e para
    # .trigger(processingTime="10 minutes")       # alternativa: a cada 10 min
    .start("/mnt/bronze/pedidos")
)

# Por que usar Auto Loader em vez de spark.read.csv()?
# - spark.read.csv(): lê TUDO toda vez (batch completo)
# - Auto Loader: lê apenas arquivos NOVOS desde a última execução
# - Estado mantido no checkpoint (sobrevive a falhas)
```

---

## DATABRICKS SQL

### SQL Warehouses

```
SQL Warehouse vs All-Purpose Cluster:
  - Warehouse: otimizado para SQL analítico, Photon sempre ativo
  - All-Purpose: para notebooks e jobs Python/Scala/SQL mistos

Tipos de Warehouse:
  - Serverless: Databricks gerencia a infra, inicia em segundos
  - Pro: mais configurações, Photon, auto-stop
  - Classic: legado, sem Photon automático

Auto-stop: warehouse para automaticamente após N minutos de inatividade
Auto-scale: adiciona clusters automaticamente com carga alta
```

---

### QUALIFY: window function filtering

```sql
-- Problema: filtrar resultado de window function
-- SQL padrão (verboso, usa CTE):
WITH ranked AS (
    SELECT *,
           ROW_NUMBER() OVER (PARTITION BY usuario_id ORDER BY data DESC) AS rn
    FROM eventos
)
SELECT * FROM ranked WHERE rn = 1;

-- Databricks SQL (elegante com QUALIFY):
SELECT *,
       ROW_NUMBER() OVER (PARTITION BY usuario_id ORDER BY data DESC) AS rn
FROM eventos
QUALIFY rn = 1;

-- Mais exemplos com QUALIFY:
-- Top 3 produtos por categoria:
SELECT categoria, produto, SUM(vendas) AS total
FROM vendas
GROUP BY categoria, produto
QUALIFY RANK() OVER (PARTITION BY categoria ORDER BY total DESC) <= 3;
```

---

### Data Skipping e Bloom Filters

```python
# Data Skipping: o Delta lê as stats do transaction log
# para PULAR arquivos que não podem conter os dados filtrados

# Cada arquivo Parquet tem stats no _delta_log:
# - minValues e maxValues por coluna
# - nullCount por coluna
# - numRecords

# Query: WHERE user_id = '12345'
# Delta verifica: o arquivo tem minValues.user_id <= '12345' <= maxValues.user_id?
# Se não → skip (nem abre o arquivo)
# Funciona automaticamente — nenhuma configuração necessária

# ─── Bloom Filters (para alta cardinalidade) ──────────────────────────────────
# Data skipping por min/max é ruim para strings UUID ou hashes (muita variação)
# Bloom filter: estrutura probabilística que permite "está neste arquivo?"

spark.sql("""
    ALTER TABLE eventos
    SET TBLPROPERTIES (
        'delta.bloomFilter.user_id.enabled' = 'true',
        'delta.bloomFilter.user_id.fpp' = '0.1',    -- false positive probability
        'delta.bloomFilter.user_id.numItems' = '10000000'
    )
""")
# Após alterar: precisa de OPTIMIZE para reconstruir os bloom filters
spark.sql("OPTIMIZE eventos ZORDER BY (user_id)")
```

---

## UNITY CATALOG

### Hierarquia de objetos

```
Metastore (1 por region/account)
└── Catalog
    └── Schema (= Database)
        ├── Table (managed ou external)
        ├── View
        ├── Function
        └── Volume (para arquivos não-tabulares)

Exemplo de caminho completo (3 levels):
  main.vendas.transacoes_silver
  ↑       ↑         ↑
  catalog schema   table

Comparação com Hive Metastore (legado):
  Hive: apenas 2 níveis (database.table)
  UC: 3 níveis (catalog.schema.table) → isolamento por ambiente (dev/prod)
```

```sql
-- Criar e usar objetos no Unity Catalog
CREATE CATALOG IF NOT EXISTS producao;
CREATE SCHEMA IF NOT EXISTS producao.vendas;

USE CATALOG producao;
USE SCHEMA vendas;

-- Agora tabelas criadas ficam em producao.vendas.*
CREATE TABLE transacoes (
    id BIGINT,
    valor DECIMAL(10,2),
    data_venda DATE
) USING DELTA;
```

---

### GRANT e controle de acesso

```sql
-- Hierarquia de GRANT (herda para baixo):
GRANT USE CATALOG ON CATALOG producao TO `analistas`;
GRANT USE SCHEMA ON SCHEMA producao.vendas TO `analistas`;
GRANT SELECT ON TABLE producao.vendas.transacoes TO `analistas`;

-- Engenheiros têm mais permissão:
GRANT MODIFY ON TABLE producao.vendas.transacoes TO `engenheiros`;
GRANT CREATE ON SCHEMA producao.vendas TO `engenheiros`;

-- Verificar permissões:
SHOW GRANTS ON TABLE producao.vendas.transacoes;

-- Revogar:
REVOKE SELECT ON TABLE producao.vendas.transacoes FROM `analistas`;

-- Principais privileges:
-- SELECT: leitura
-- MODIFY: INSERT, UPDATE, DELETE
-- CREATE: criar objetos no schema
-- ALL PRIVILEGES: tudo (usar com cuidado)
```

---

### Row Filters e Column Masks

```sql
-- ─── Row Filter: esconder linhas baseado no usuário ──────────────────────────
-- Cenário: cada vendedor só vê suas próprias vendas

CREATE FUNCTION vendas.row_filter_vendedor(vendedor_id STRING)
RETURN
    IS_MEMBER('admin') OR             -- admins veem tudo
    vendedor_id = CURRENT_USER();     -- vendedores veem só as deles

ALTER TABLE vendas.transacoes
SET ROW FILTER vendas.row_filter_vendedor ON (vendedor_id);

-- ─── Column Mask: mascarar PII ────────────────────────────────────────────────
-- Cenário: apenas engenheiros de dados veem CPF completo

CREATE FUNCTION seguranca.mask_cpf(cpf STRING)
RETURN
    CASE
        WHEN IS_MEMBER('engenheiros') THEN cpf
        ELSE CONCAT('***.***.', SUBSTRING(cpf, 8, 3), '-**')
    END;

ALTER TABLE clientes
ALTER COLUMN cpf SET MASK seguranca.mask_cpf;
```

---

## WORKFLOWS E DLT

### Databricks Workflows

```python
# Job via Databricks Asset Bundle (databricks.yml):
# (ver seção CI/CD para contexto completo)

bundle:
  name: pipeline-vendas

resources:
  jobs:
    ingestao_diaria:
      name: "Ingestão Diária Vendas"
      schedule:
        quartz_cron_expression: "0 0 6 * * ?"   # todos os dias às 6h
        timezone_id: "America/Sao_Paulo"
      tasks:
        - task_key: bronze
          notebook_task:
            notebook_path: ./notebooks/bronze_vendas
          job_cluster_key: cluster_prod

        - task_key: silver
          depends_on:
            - task_key: bronze
          notebook_task:
            notebook_path: ./notebooks/silver_vendas
          job_cluster_key: cluster_prod

        - task_key: gold
          depends_on:
            - task_key: silver
          notebook_task:
            notebook_path: ./notebooks/gold_metricas
          job_cluster_key: cluster_prod

  job_clusters:
    - job_cluster_key: cluster_prod
      new_cluster:
        spark_version: "15.4.x-scala2.12"
        num_workers: 2
        autotermination_minutes: 30
```

---

### Delta Live Tables

```python
import dlt
from pyspark.sql import functions as F

# ─── LIVE TABLE (batch) ───────────────────────────────────────────────────────
@dlt.table(
    name="bronze_pedidos",
    comment="Pedidos raw — ingestão via Auto Loader",
    table_properties={"quality": "bronze"}
)
def bronze_pedidos():
    return (spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option("cloudFiles.schemaLocation", "/mnt/checkpoints/schema_pedidos")
        .load("/mnt/landing/pedidos/")
    )

# ─── STREAMING LIVE TABLE (incremental) ──────────────────────────────────────
@dlt.table(
    name="silver_pedidos",
    comment="Pedidos limpos e tipados"
)
@dlt.expect_or_drop("valor_positivo", "valor > 0")        # dropar linha inválida
@dlt.expect("id_nao_nulo", "id IS NOT NULL")              # registrar violação, não dropar
@dlt.expect_or_fail("data_valida", "data_pedido IS NOT NULL")  # falhar pipeline
def silver_pedidos():
    return (dlt.read_stream("bronze_pedidos")
        .withColumn("valor", F.col("valor").cast("decimal(10,2)"))
        .withColumn("data_pedido", F.to_date("data_pedido_str", "yyyy-MM-dd"))
        .select("id", "valor", "data_pedido", "cliente_id", "produto_id")
    )

# ─── LIVE TABLE (gold — agrega silver) ───────────────────────────────────────
@dlt.table(name="gold_receita_diaria")
def gold_receita_diaria():
    return (dlt.read("silver_pedidos")
        .groupBy("data_pedido")
        .agg(
            F.sum("valor").alias("receita_total"),
            F.count("id").alias("qtd_pedidos"),
            F.avg("valor").alias("ticket_medio")
        )
    )
```

---

### DLT Expectations

```python
# Expectativas controlam o que acontece com dados inválidos:

# 1. @dlt.expect: registra violações no metrics, mas mantém os dados
@dlt.expect("email_valido", "email LIKE '%@%.%'")

# 2. @dlt.expect_or_drop: remove a linha, pipeline continua
@dlt.expect_or_drop("valor_positivo", "valor > 0")

# 3. @dlt.expect_or_fail: para o pipeline se qualquer linha violar
@dlt.expect_or_fail("id_obrigatorio", "id IS NOT NULL")

# Ver métricas de qualidade no UI do DLT:
# Pipeline → Events → Data Quality
# Mostra: passed_records, failed_records, dropped_records por expectation
```

---

## MLFLOW

### Experiment Tracking

```python
import mlflow
import mlflow.sklearn
from sklearn.ensemble import RandomForestClassifier
from sklearn.model_selection import train_test_split
from sklearn.metrics import accuracy_score, f1_score

mlflow.set_experiment("/Users/user@email.com/experimento_churn")

with mlflow.start_run(run_name="random_forest_v1"):
    # ─── Log parâmetros ───────────────────────────────────────────────────────
    params = {"n_estimators": 100, "max_depth": 5, "random_state": 42}
    mlflow.log_params(params)

    # ─── Treinar modelo ───────────────────────────────────────────────────────
    modelo = RandomForestClassifier(**params)
    modelo.fit(X_train, y_train)

    # ─── Log métricas ─────────────────────────────────────────────────────────
    y_pred = modelo.predict(X_test)
    mlflow.log_metric("accuracy", accuracy_score(y_test, y_pred))
    mlflow.log_metric("f1_score", f1_score(y_test, y_pred))

    # ─── Log artefatos ────────────────────────────────────────────────────────
    mlflow.log_artifact("feature_importance.png")
    mlflow.log_dict({"features": list(X_train.columns)}, "features.json")

    # ─── Log modelo ───────────────────────────────────────────────────────────
    mlflow.sklearn.log_model(
        modelo,
        artifact_path="model",
        registered_model_name="modelo-churn"    # registra no Model Registry
    )

# ─── autolog (alternativa sem boilerplate) ────────────────────────────────────
mlflow.sklearn.autolog()   # loga automaticamente params, metrics e modelo
modelo.fit(X_train, y_train)
```

---

### Model Registry

```python
from mlflow.tracking import MlflowClient

client = MlflowClient()

# ─── Promover para Production ─────────────────────────────────────────────────
# MLflow < 2.9 (stages):
client.transition_model_version_stage(
    name="modelo-churn",
    version=3,
    stage="Production"
)

# MLflow >= 2.9 (aliases — novo padrão):
client.set_registered_model_alias("modelo-churn", "champion", 3)
client.set_registered_model_alias("modelo-churn", "challenger", 4)

# ─── Carregar modelo do registry ─────────────────────────────────────────────
# Por alias (recomendado):
modelo = mlflow.pyfunc.load_model("models:/modelo-churn@champion")

# Por stage (legado):
modelo = mlflow.pyfunc.load_model("models:/modelo-churn/Production")

# Por versão (desenvolvimento):
modelo = mlflow.pyfunc.load_model("models:/modelo-churn/3")

# ─── Inferência em batch com Spark ────────────────────────────────────────────
predict_udf = mlflow.pyfunc.spark_udf(spark, "models:/modelo-churn@champion")
df_com_predicoes = df.withColumn("predicao", predict_udf(*feature_cols))
```

---

## PERFORMANCE

### Photon Engine

```
O que é Photon:
- Engine de execução vetorizada em C++ dentro do Databricks Runtime
- Substitui o executor JVM padrão do Spark para operações elegíveis
- Processamento em colunas (SIMD) — muito mais rápido para analytics

Quando Photon é ativado automaticamente:
✅ Leitura/escrita de Delta Lake e Parquet
✅ SQL queries
✅ Aggregations (groupBy, agg)
✅ Joins (sort-merge, broadcast)
✅ Sorting

Quando Photon NÃO ajuda (recai para JVM):
❌ Python UDFs (rodam fora do Photon)
❌ Pandas UDFs (rodam via Arrow, não Photon)
❌ RDD operations
❌ Código Scala/Java customizado fora do Spark SQL

Implicação prática:
→ Prefira funções built-in (pyspark.sql.functions) sobre UDFs Python
→ Se precisar de UDF: use Pandas UDF (Arrow) > Python UDF (pickle)
→ SQL puro é o mais rápido com Photon
```

---

### Adaptive Query Execution (AQE)

```python
# AQE habilitado por padrão no Databricks Runtime 8+
# spark.conf.set("spark.sql.adaptive.enabled", "true")

# ─── Feature 1: Coalesce partitions ──────────────────────────────────────────
# Problema: depois de um shuffle, muitas partições pequenas (ex: 200 de 1 MB cada)
# AQE: combina automaticamente partições pequenas em partições maiores
# Parâmetro:
spark.conf.set("spark.sql.adaptive.coalescePartitions.minPartitionNum", "1")
spark.conf.set("spark.sql.adaptive.advisoryPartitionSizeInBytes", "128m")

# ─── Feature 2: Skew Join ──────────────────────────────────────────────────────
# Problema: uma chave de join tem 80% dos dados → uma task processa tudo sozinha
# AQE: detecta automaticamente e divide a partição skewed em sub-partes
spark.conf.set("spark.sql.adaptive.skewJoin.enabled", "true")
spark.conf.set("spark.sql.adaptive.skewJoin.skewedPartitionThresholdInBytes", "256m")

# ─── Feature 3: Join Strategy ─────────────────────────────────────────────────
# AQE pode mudar sort-merge join → broadcast join em runtime
# (quando descobre que o lado menor cabe em memória)
# Parâmetro:
spark.conf.set("spark.sql.adaptive.localShuffleReader.enabled", "true")

# Ver efeito do AQE no Spark UI:
# Job → Stages → clique num stage → "AQE Plan" tab
```

---

### Delta Cache vs Spark Cache

```
São coisas COMPLETAMENTE diferentes:

Delta Cache (cache de disco — Databricks exclusivo):
  - Cache dos arquivos Parquet/Delta no SSD local dos workers
  - Transparente: acontece automaticamente, não precisa de .cache()
  - Persiste entre queries diferentes no mesmo cluster
  - Configuração: spark.databricks.io.cache.enabled = true (padrão em alguns DBR)
  - Ideal para: datasets grandes lidos repetidamente (dashboards, iterative ML)

Spark Cache (.cache() / .persist()):
  - Cache dos dados JÁ processados (depois de transformações) na RAM/disco
  - Explícito: você decide quando cachear
  - Não persiste entre SparkSessions
  - Ideal para: DataFrames intermediários usados múltiplas vezes no mesmo job
  - .cache() = MEMORY_AND_DISK
  - .persist(StorageLevel.MEMORY_ONLY) = apenas RAM (mais rápido, pode ser evicted)

Quando usar cada um:
  Delta Cache → para leitura de tabelas Delta grandes (automático)
  Spark .cache() → para DataFrames intermediários usados 2+ vezes no mesmo script

df_filtrado = df.filter(condicao_complexa).cache()    # usar 2+ vezes
resultado1 = df_filtrado.groupBy("a").count()
resultado2 = df_filtrado.groupBy("b").sum("valor")
df_filtrado.unpersist()                               # liberar quando não precisar mais
```

---

## DATABRICKS ASSET BUNDLES

```yaml
# databricks.yml — Infrastructure as Code para Databricks

bundle:
  name: pipeline-vendas

variables:
  env:
    description: "Ambiente de deploy (dev/prod)"
    default: dev

targets:
  dev:
    mode: development
    default: true
    workspace:
      host: https://dbc-962e23c9-e80d.cloud.databricks.com
    variables:
      env: dev

  prod:
    mode: production
    workspace:
      host: https://seu-workspace.azuredatabricks.net
    variables:
      env: prod

resources:
  jobs:
    pipeline_vendas:
      name: "Pipeline Vendas [${var.env}]"
      tasks:
        - task_key: ingestao
          python_wheel_task:
            package_name: pipeline_vendas
            entry_point: main
          job_cluster_key: cluster_padrao
      job_clusters:
        - job_cluster_key: cluster_padrao
          new_cluster:
            spark_version: "15.4.x-scala2.12"
            num_workers: 2

  pipelines:
    dlt_medallion:
      name: "DLT Medallion [${var.env}]"
      libraries:
        - notebook:
            path: ./notebooks/dlt_pipeline
      continuous: false
      catalog: main
      schema: vendas_${var.env}
```

```powershell
# Comandos do Databricks CLI / Bundle:
databricks bundle validate              # verifica syntax do databricks.yml
databricks bundle deploy --target dev  # deploy para dev
databricks bundle run pipeline_vendas  # rodar job
databricks bundle destroy              # remover recursos deployados
```

---

## CERTIFICAÇÃO — Pontos Críticos

### Domínio 1: Databricks Lakehouse Platform (24%)

```
Conceitos que sempre caem no exame:

1. Diferença All-Purpose vs Job Cluster:
   - All-Purpose: interativo, caro, persiste até ser parado
   - Job: automático, barato, destruído após o job

2. Unity Catalog:
   - Hierarquia: Metastore > Catalog > Schema > Table/Volume
   - Managed table: UC controla localização e lifecycle
   - External table: você controla o storage, UC só gerencia metadados

3. DBFS vs Volumes:
   - DBFS: legado, não é governado por UC
   - Volumes: governado por UC, recomendado para novos projetos

4. Photon: disponível no Free Trial (DBR 9+), ativo por padrão em clusters recentes
   Photon acelera: SQL, Delta reads/writes, aggregate/join/sort — não acelera UDFs Python
```

### Domínio 2: ELT with Spark (29%)

```
Conceitos que sempre caem:

1. Transformations lazy vs Actions:
   - filter, select, withColumn → lazy (não executam)
   - show, count, collect, write → actions (executam)

2. Broadcast join:
   spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "10m")
   F.broadcast(df_pequeno)  # forçar broadcast

3. MERGE INTO:
   - QUANDO MATCHED THEN UPDATE
   - WHEN NOT MATCHED THEN INSERT
   - WHEN NOT MATCHED BY SOURCE THEN DELETE (novo em Delta 2.3)

4. Schema enforcement vs evolution:
   - Enforcement: bloqueio por padrão
   - mergeSchema: adiciona colunas novas
   - overwriteSchema: substitui completamente (overwrite mode)
```

### Domínio 3: Incremental Processing (22%)

```
Conceitos críticos:

1. Auto Loader vs spark.readStream.format("csv"):
   - Auto Loader: escala para bilhões de arquivos, state eficiente
   - readStream direto: funciona mas não escala para muitos arquivos

2. Trigger types no Structured Streaming:
   - trigger(availableNow=True): micro-batch, processa tudo disponível e para
   - trigger(processingTime="10 minutes"): intervalo fixo
   - trigger(once=True): legado, use availableNow
   - trigger(continuous="1 second"): baixa latência, experimental

3. Checkpoint:
   - Obrigatório para streams stateful
   - Localização: .option("checkpointLocation", "/path/checkpoint")
   - Sem checkpoint: stream começa do início a cada restart

4. DLT vs Structured Streaming manual:
   - DLT: abstraí o checkpoint, restart e qualidade de dados
   - Streaming manual: mais controle, mais código de manutenção
```

### Domínio 4: Production Pipelines (16%)

```
1. Job Clusters vs All-Purpose para produção:
   → SEMPRE Job Clusters em produção (mais barato + isolamento)

2. Retry policies:
   max_retries: 3
   retry_on_timeout: true
   min_retry_interval_millis: 60000

3. Task dependencies no Workflow:
   depends_on: [task_a, task_b]  → fan-in (espera ambas)
   task_a e task_b sem depends_on → fan-out (rodam em paralelo)
```

### Domínio 5: Data Governance (9%)

```
1. Privilege model no UC:
   SELECT < MODIFY < CREATE < ALL PRIVILEGES
   + USE CATALOG e USE SCHEMA para navegar na hierarquia

2. Data lineage:
   - Automático no Unity Catalog
   - Mostra: upstream (de onde veio) e downstream (quem usa)

3. Audit logs:
   - Todos os acessos são logados automaticamente no UC
   - Disponível via system.access.audit table
```
