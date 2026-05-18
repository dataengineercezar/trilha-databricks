# PLANO PERSONALIZADO — Databricks Data Engineer → Certificação
> Gerado em: Mai/2026 | 12h/semana | Meta: Databricks Certified Data Engineer Associate
> **Pré-requisito:** TRILHA_DE Semanas 1–10 concluídas (Python, SQL, Spark, Docker)

---

## PROGRESSO ATUAL

| Fase | Semanas | Status | Notebooks |
|------|---------|--------|-----------|
| Setup + Plataforma | 1–2 | ✅ | Q1-Q4 criados e testados |
| Delta Lake | 3–4 | ✅ | 7 notebooks — fundamentos e avançado |
| Arquitetura Medallion + SQL | 5–6 | ✅ | Auto Loader + QUALIFY + Window Functions |
| Governança (Unity Catalog) | 7 | ✅ | Managed/External, GRANT, Column Mask |
| Workflows + DLT | 8 | ✅ | Jobs YAML + @dlt.expect patterns |
| MLflow | 9 | ✅ | Tracking + Registry UC + spark_udf |
| Performance + CI/CD | 10–11 | ✅ | Photon/AQE + Asset Bundles + GitHub Actions |
| **Certificação** | **12** | ✅ | Flashcards + 20 questões simulado |

---

## DIAGNÓSTICO DE ENTRADA

> Atualizado: Mai/2026 — início da trilha Databricks

| Habilidade | Nível Entrada | Meta |
|---|---|---|
| Python / PySpark | Intermediário ✅ | Avançado no contexto Databricks |
| SQL | Intermediário ✅ | Databricks SQL proficiente |
| Spark (local) | Intermediário ✅ | Databricks Runtime + Photon |
| Delta Lake | Superficial | Proficiente (ACID, DML, time travel) |
| Databricks Workspace | Superficial | Proficiente |
| Unity Catalog | Zero | Entende e configura |
| Workflows / DLT | Zero | Cria pipelines em produção |
| MLflow | Superficial | Tracking + registry em produção |
| Databricks Asset Bundles | Zero | Deploys com CI/CD |
| Certificação DE Associate | Zero | Aprovação no exame |

**Vantagem:** você já sabe Spark, Python e SQL. Aqui o foco é a **plataforma**
e os **padrões de produção** do ecossistema Databricks.

---

## DISTRIBUIÇÃO SEMANAL (12h)

```
Seg  → 2h (Teoria + documentação oficial)
Ter  → 2h (Prática no workspace / notebooks)
Qui  → 2h (Prática / exercícios do guia técnico)
Sex  → 1h (Revisão + anotações no guia)
Sáb  → 3h (Projeto prático / mini-pipeline)
Dom  → 2h (Mock exam / desafios)
────────────────────────────────────────
Total → 12h
```

---

## FASE 1 — PLATAFORMA DATABRICKS (Semanas 1–2)

### SEMANA 1 — Setup: Free Trial + VS Code Extension + Serverless

**Por que começar pelo setup e não direto com código?**
Databricks tem uma plataforma própria com conceitos específicos (workspace, clusters,
DBFS, Repos, Unity Catalog). Entender a navegação e o modelo de acesso antes de
escrever código poupa confusão depois.

**Conteúdo:**
```
Segunda 2h → Criar conta Free Trial (databricks.com/try-databricks)
             Navegar: Workspace, Compute, SQL, Catalog, Jobs & Pipelines, Marketplace
             Entender: Serverless vs All-Purpose Cluster vs Job Cluster

Terça 2h   → Instalar extensão Databricks no VS Code (databricks.databricks)
             Configurar conexão: URL do workspace + Access Token
             Navegar workspace pelo VS Code: cluster manager, file browser

Quinta 2h  → Databricks CLI: instalar, configurar, comandos básicos
             databricks workspace list /
             databricks clusters list
             databricks fs ls dbfs:/

Sexta 1h   → Revisão: anotar o que cada recurso faz no guia_tecnico.md

Sábado 3h  → PROJETO: criar notebook "hello_databricks.ipynb" no workspace
             Rodar com o cluster ativo
             Sincronizar o notebook localmente via extensão do VS Code
             Testar Databricks Connect: rodar o mesmo código no VS Code

Domingo 2h → Ler: docs.databricks.com/getting-started (conceitos de workspace)
             Quiz pessoal: consegue responder "o que é DBFS vs Volumes?"
```

**Recursos:**
- [Databricks Free Trial](https://www.databricks.com/try-databricks)
- [VS Code Extension](https://marketplace.visualstudio.com/items?itemName=databricks.databricks)
- [Databricks CLI docs](https://docs.databricks.com/dev-tools/cli/index.html)

---

### SEMANA 2 — Spark no Databricks: clusters, notebooks e magic commands

**Por que essa semana é diferente do Spark local?**
No Databricks, o Spark roda em clusters gerenciados com o Databricks Runtime (DBR),
que inclui otimizações proprietárias (Photon, Delta Engine). Os notebooks têm
magic commands exclusivos. O DBFS é o sistema de arquivos distribuído do workspace.

**Conteúdo:**
```
Segunda 2h → Tipos de cluster: All-Purpose vs Job clusters
             Single-node vs multi-node (Free Trial: até multi-node)
             Databricks Runtime versions: LTS vs latest
             Auto-termination e configurações de custo

Terça 2h   → Notebooks no Databricks:
             Magic commands: %python, %sql, %scala, %r, %md, %sh, %fs
             dbutils: dbutils.fs, dbutils.secrets, dbutils.notebook, dbutils.widgets
             displayHTML(), display() vs show()

Quinta 2h  → DBFS: o filesystem do Databricks
             dbutils.fs.ls("dbfs:/")
             dbutils.fs.cp(), dbutils.fs.rm(), dbutils.fs.mkdirs()
             Diferença DBFS vs Volumes (Unity Catalog) vs DBFS Root
             Upload de arquivo CSV via UI → ler com Spark

Sexta 1h   → Revisão e documentação dos magic commands no guia_tecnico.md

Sábado 3h  → PROJETO: ETL completo em notebook Databricks
             1. Upload de CSV para DBFS
             2. Leitura com spark.read.csv() (schema explícito)
             3. Transformações com PySpark (filter, withColumn, groupBy)
             4. Gravação em Parquet no DBFS
             5. Usar %sql para consultar os dados gravados

Domingo 2h → Exercícios: ex2_spark_databricks (ver pasta semana02)
```

**Recursos:**
- [Magic commands docs](https://docs.databricks.com/notebooks/notebooks-use.html)
- [dbutils reference](https://docs.databricks.com/dev-tools/databricks-utils.html)
- [DBFS reference](https://docs.databricks.com/dbfs/index.html)

---

## FASE 2 — DELTA LAKE (Semanas 3–4)

> Delta Lake é o coração da plataforma Databricks. Sem dominar Delta, você
> não entende produção em Databricks.

### SEMANA 3 — Delta Lake Fundamentos

**Por que Delta Lake e não Parquet puro?**
Parquet é imutável — para fazer UPDATE ou DELETE você reescreve arquivos inteiros
manualmente. Delta Lake adiciona uma camada transacional (transaction log) sobre
Parquet que garante ACID, schema enforcement e time travel.

**Conteúdo:**
```
Segunda 2h → O que é Delta Lake:
             Transaction log (_delta_log/) — como funciona cada commit
             ACID no contexto de Data Lakes (vs Parquet puro)
             Formatos: Delta vs Parquet vs JSON vs CSV — quando usar cada um

Terça 2h   → Criar tabelas Delta:
             spark.write.format("delta").save(path)
             CREATE TABLE ... USING DELTA
             DESCRIBE HISTORY table_name  ← o transaction log
             DESCRIBE DETAIL table_name

Quinta 2h  → DML em Delta Lake:
             INSERT INTO, UPDATE, DELETE (impossível em Parquet puro)
             Comportamento transacional: partial failures são impossíveis
             MERGE INTO (upsert) — o operador mais poderoso do Delta

Sexta 1h   → Revisão: anotar diferenças Delta vs Parquet no guia_tecnico.md

Sábado 3h  → PROJETO: migração Parquet → Delta
             1. Criar tabela Parquet com dataset de transações (1000 linhas)
             2. Converter para Delta: CONVERT TO DELTA
             3. Executar UPDATE, DELETE, MERGE
             4. Ver o DESCRIBE HISTORY e identificar cada operação
             5. Reverter com time travel: VERSION AS OF

Domingo 2h → Exercícios: ex3_delta_fundamentos (ver pasta semana03)
```

**Conceitos críticos:**
```
Transaction log (_delta_log/):
  - Um arquivo JSON por commit (0000000000000000000.json, ...)
  - Cada JSON contém: add/remove de arquivos, stats, schema
  - É isso que garante ACID — uma única fonte de verdade

Schema enforcement:
  - Por padrão, Delta rejeita dados com schema diferente
  - mergeSchema: permite adicionar colunas novas (schema evolution)
  - overwriteSchema: substitui o schema completamente (perigoso!)
```

---

### SEMANA 4 — Delta Lake Avançado

**Conteúdo:**
```
Segunda 2h → Time Travel:
             SELECT * FROM table VERSION AS OF 5
             SELECT * FROM table TIMESTAMP AS OF '2026-01-15'
             RESTORE TABLE table TO VERSION AS OF 3
             Caso de uso: auditoria, rollback, reprodutibilidade de ML

Terça 2h   → OPTIMIZE e Z-ORDER:
             Por que arquivos pequenos matam performance (small file problem)
             OPTIMIZE table: compacta arquivos pequenos em arquivos maiores (~1GB)
             Z-ORDER BY (col1, col2): clustering multidimensional no arquivo
             Quando usar Z-ORDER: colunas de filtro frequente (data, user_id)

Quinta 2h  → VACUUM:
             Por que o Delta guarda arquivos antigos (para time travel)
             VACUUM table RETAIN 168 HOURS — limpar arquivos com >7 dias
             Cuidado: após VACUUM, time travel para versões antigas falha
             Auto-optimize: delta.autoOptimize.optimizeWrite + autoCompact

Sexta 1h   → Schema Evolution:
             mergeSchema vs overwriteSchema
             ALTER TABLE ADD COLUMN, CHANGE COLUMN
             Incompatibilidades que o Delta bloqueia (remoção de coluna)

Sábado 3h  → PROJETO: pipeline Delta com qualidade industrial
             1. Criar tabela Delta particionada por (ano, mes)
             2. Inserir dados em lote (simulando ingestão diária)
             3. Executar OPTIMIZE + Z-ORDER BY (data_venda, produto_id)
             4. Medir diferença de tempo antes/depois do Z-ORDER
             5. Configurar auto-optimize no nível da tabela
             6. Executar VACUUM e verificar versões disponíveis

Domingo 2h → Exercícios: ex4_delta_avancado (ver pasta semana04)
```

---

## FASE 3 — ARQUITETURA MEDALLION + DATABRICKS SQL (Semanas 5–6)

### SEMANA 5 — Medallion Architecture no Databricks

**Por que Medallion é o padrão de facto?**
A arquitetura Bronze → Silver → Gold separa claramente ingestão raw, limpeza e
modelagem analítica. Databricks foi quem popularizou esse padrão — está em toda
oferta de emprego que menciona a plataforma.

**Conteúdo:**
```
Segunda 2h → Conceito de Medallion:
             Bronze: dados raw exatamente como chegaram (imutável, append-only)
             Silver: dados limpos, tipados, deduplicados (SELECT pronto para uso)
             Gold: agregações analíticas, métricas de negócio (pronto para BI)
             Por que 3 camadas e não 2? (Auditabilidade + separação de concerns)

Terça 2h   → Bronze layer:
             Schema: adicionar metadados (_ingest_timestamp, _source_file, _batch_id)
             Estratégia: append-only ou MERGE com deduplicação por event_id?
             Auto Loader: ingestão incremental de arquivos (spark.readStream)

Quinta 2h  → Silver layer:
             Limpeza: tipos corretos, nulls tratados, enums validados
             Deduplicação: MERGE INTO silver USING bronze ON (id, event_time)
             Particionamento: por data de processamento vs data do evento

Sexta 1h   → Gold layer e padrões de modelagem:
             Star Schema no Delta Lake
             Fatos e dimensões como tabelas Delta
             Atualização incremental de dimensões (SCD Tipo 2 com Delta MERGE)

Sábado 3h  → PROJETO: pipeline Medallion completo
             Dataset: NYC Taxi (CSV público do Databricks Datasets)
             Bronze → Silver → Gold em 3 notebooks encadeados
             Cada camada é uma tabela Delta com DESCRIBE HISTORY

Domingo 2h → Exercícios: ex5_medallion (ver pasta semana05)
```

---

### SEMANA 6 — Databricks SQL

**Conteúdo:**
```
Segunda 2h → SQL Warehouses (ex Endpoints SQL):
             Serverless vs Pro vs Classic
             Auto-stop e auto-scale
             Conexão via JDBC/ODBC (Power BI, Tableau, DBeaver)

Terça 2h   → Databricks SQL — interface e queries:
             Query editor com autocomplete
             Visualizações e dashboards (Lakeview)
             Alerts (notificações quando threshold é atingido)
             Query history e performance profiling

Quinta 2h  → SQL avançado no Databricks:
             PIVOT e UNPIVOT
             QUALIFY (substituto limpo de WHERE em window functions)
             ai_gen(), ai_analyze_sentiment() — funções de IA no SQL
             LIVE tables e STREAMING LIVE TABLE (DLT, preview)

Sexta 1h   → Otimização de queries SQL:
             Bloom filters em Delta
             Data skipping: como o transaction log permite pular arquivos
             Liquid Clustering (alternativa moderna ao Z-ORDER particionado)

Sábado 3h  → PROJETO: dashboard analítico no Databricks SQL
             Usando a Gold layer do projeto Medallion da semana 5
             3 queries analíticas com window functions
             2 visualizações + 1 dashboard Lakeview

Domingo 2h → Exercícios: ex6_databricks_sql (ver pasta semana06)
```

---

## FASE 4 — GOVERNANÇA E ORQUESTRAÇÃO (Semanas 7–8)

### SEMANA 7 — Unity Catalog

> Unity Catalog é a resposta do Databricks para governança de dados.
> É obrigatório no exame de certificação e em qualquer empresa enterprise.

**Conteúdo:**
```
Segunda 2h → O que é Unity Catalog e por que existe:
             Hierarquia: Metastore → Catalog → Schema → Table/Volume/Function
             Comparação com Hive Metastore (legado): por que UC é superior
             Account-level vs workspace-level

Terça 2h   → Criando objetos no Unity Catalog:
             CREATE CATALOG, CREATE SCHEMA, CREATE TABLE
             Managed tables vs External tables
             Volumes: o substituto de DBFS para arquivos não-tabulares

Quinta 2h  → Controle de acesso (Data Governance):
             GRANT/REVOKE: SELECT, MODIFY, CREATE, USAGE
             Row-level security com Row Filters
             Column-level security com Column Masks
             Data lineage: ver de onde vêm os dados de uma tabela

Sexta 1h   → Tags, comentários e Data Catalog:
             COMMENT ON TABLE/COLUMN
             ALTER TABLE SET TAGS
             Pesquisa no Catalog Explorer

Sábado 3h  → PROJETO: Unity Catalog end-to-end
             Criar catalog e schema para o projeto Medallion
             Migrar tabelas Hive legado para Unity Catalog
             Aplicar GRANT correto para "analysts" e "engineers"
             Demonstrar column masking em coluna de PII (CPF, email)

Domingo 2h → Exercícios + leitura docs Unity Catalog
```

---

### SEMANA 8 — Workflows + Delta Live Tables

**Conteúdo:**
```
Segunda 2h → Databricks Workflows:
             Jobs vs Workflows (nomenclatura atual)
             Tasks: Notebook, Python script, SQL, dbt, Spark JAR
             Task dependencies: linear, fan-out, fan-in (DAG de tasks)
             Retry policies, timeouts, email alerts

Terça 2h   → Agendamento de Jobs:
             Triggers: schedule (cron), file arrival, manual
             Parametrizar jobs: widgets e job parameters
             Job clusters vs All-Purpose clusters (custo!)

Quinta 2h  → Delta Live Tables (DLT):
             Declarative pipelines: você define O QUE quer, DLT faz o COMO
             @dlt.table decorator em Python
             STREAMING LIVE TABLE vs LIVE TABLE
             Expectations: @dlt.expect, @dlt.expect_or_drop, @dlt.expect_or_fail
             Qualidade de dados automática com DLT

Sexta 1h   → DLT avançado:
             Pipeline modes: triggered vs continuous
             Monitoring: pipeline dashboard, event log
             DLT com Unity Catalog

Sábado 3h  → PROJETO: pipeline DLT + Workflow orquestrado
             DLT pipeline: Bronze → Silver (com expectations de qualidade)
             Workflow: DLT pipeline → Gold dbt model → SQL dashboard refresh

Domingo 2h → Exercícios: ex7_workflows_dlt (ver pasta semana08)
```

---

## FASE 5 — MLFLOW (Semana 9)

### SEMANA 9 — MLflow no Databricks

**Conteúdo:**
```
Segunda 2h → MLflow fundamentos:
             4 componentes: Tracking, Projects, Models, Registry
             Experiments e Runs: como o Databricks organiza
             mlflow.autolog() — log automático para sklearn, xgboost, etc.

Terça 2h   → Experiment Tracking:
             mlflow.log_param(), log_metric(), log_artifact()
             Comparar runs no UI do Databricks
             Custom metrics e plots como artifacts

Quinta 2h  → Model Registry:
             Registrar um modelo: mlflow.register_model()
             Estágios: None → Staging → Production → Archived
             Model aliases e tags (MLflow 2.x)
             Carregar modelo do registry para inferência

Sexta 1h   → Feature Store (Databricks):
             FeatureStoreClient.create_table()
             Write features, lookup features
             Point-in-time lookup para evitar data leakage

Sábado 3h  → PROJETO: pipeline de ML completo com MLflow
             1. Dataset: sklearn.datasets (wine, breast_cancer)
             2. Treinar 3 modelos com diferentes hyperparams
             3. Logar tudo no MLflow (params, metrics, artifacts)
             4. Comparar no UI e promover o melhor para Production
             5. Servir o modelo via mlflow.pyfunc.load_model()

Domingo 2h → Exercícios: ex8_mlflow (ver pasta semana09)
```

---

## FASE 6 — PERFORMANCE + CI/CD (Semanas 10–11)

### SEMANA 10 — Performance Tuning

**Conteúdo:**
```
Segunda 2h → Photon Engine:
             O que é Photon (vectorized execution engine em C++)
             Quando Photon ajuda: SQL, Delta, Parquet reads/writes
             Quando Photon NÃO ajuda: UDFs Python, RDD operations
             Habilitar Photon: basta usar cluster com DBR 9+

Terça 2h   → Adaptive Query Execution (AQE):
             3 features: coalesce partitions, skew join, join reordering
             spark.sql.adaptive.enabled = true (padrão em DBR 8+)
             Skew join hints: SKEW_JOIN
             Observar no Spark UI: DAG, stages, shuffle read/write

Quinta 2h  → Caching e persistência:
             .cache() vs .persist(StorageLevel)
             Delta cache vs Spark cache (totalmente diferentes!)
             Delta cache: cache de arquivos Parquet no SSD do worker
             Quando usar cada um

Sexta 1h   → Liquid Clustering (novo padrão 2024+):
             vs Z-ORDER: não requer OPTIMIZE manual periódico
             CLUSTER BY (col1, col2) na criação da tabela
             Incremental clustering automático

Sábado 3h  → PROJETO: otimização de performance
             Dataset: 10M linhas (NYC Taxi completo ou TPC-H)
             Benchmark: query sem otimização vs com Z-ORDER vs com Liquid Clustering
             Benchmark: Python UDF vs Pandas UDF vs built-in function
             Analisar no Spark UI: identificar bottleneck de shuffle

Domingo 2h → Exercícios: ex9_performance (ver pasta semana10)
```

---

### SEMANA 11 — CI/CD e Databricks Asset Bundles

**Conteúdo:**
```
Segunda 2h → Databricks Repos:
             Git integration no workspace (GitHub, GitLab, Azure DevOps)
             Branches no Repos: checkout, pull, merge no UI
             .gitignore para Databricks (.databricks/, __pycache__)

Terça 2h   → Databricks Asset Bundles (DABs):
             O que é DAB: Infrastructure-as-Code para Databricks
             databricks.yml: definir Jobs, Pipelines, clusters como código
             databricks bundle validate, deploy, run
             Ambientes: dev, staging, prod com variáveis diferentes

Quinta 2h  → GitHub Actions para Databricks:
             databricks/run-notebook action
             Workflow CI: lint → test → deploy bundle
             Gerenciar secrets no GitHub e injetar no bundle

Sexta 1h   → Testing de notebooks e jobs:
             pytest com Databricks Connect: testar funções localmente
             nutter: framework de testes para notebooks Databricks

Sábado 3h  → PROJETO: CI/CD completo
             Criar Job no Databricks como código (databricks.yml)
             GitHub Actions: push → deploy automático para dev
             Push para main → deploy para prod com aprovação manual

Domingo 2h → Leitura: Databricks Asset Bundles docs
```

---

## FASE 7 — CERTIFICAÇÃO (Semana 12)

### SEMANA 12 — Databricks Certified Data Engineer Associate

**Sobre o exame:**
```
- 45 questões de múltipla escolha
- 90 minutos
- Score mínimo: 70%
- Validade: 2 anos
- Custo: USD 200 (vouchers disponíveis no Databricks Academy)
- Idioma: Inglês
- Formato: remoto (Kryterion proctored) ou presencial

Domínios do exame (pesos aproximados):
  1. Databricks Lakehouse Platform (24%)
  2. ELT with Apache Spark (29%)
  3. Incremental Data Processing (22%)
  4. Production Pipelines (16%)
  5. Data Governance (9%)
```

**Conteúdo semana 12:**
```
Segunda 2h → Revisão domínio 1: Lakehouse, Unity Catalog, cluster types
Terça 2h   → Revisão domínio 2: Spark, Delta DML, schema enforcement
Quinta 2h  → Revisão domínio 3: Auto Loader, Structured Streaming, DLT
Sexta 1h   → Revisão domínio 4: Workflows, job clusters vs all-purpose
Sábado 3h  → Mock exam completo (usar Databricks Academy practice exam)
             Revisar erros e pontos fracos
Domingo 2h → Revisão final dos pontos fracos + registro do exame
```

**Recursos gratuitos para a certificação:**
- [Databricks Academy (gratuito)](https://academy.databricks.com) — cursos oficiais
- [Practice exams no Academy](https://academy.databricks.com) — simulados oficiais
- [Exam guide oficial](https://www.databricks.com/learn/certification/data-engineer-associate)
- Flashcards: Delta Lake concepts, DLT syntax, Unity Catalog hierarchy

---

## CHECKPOINTS DE AVALIAÇÃO

| Semana | Checkpoint | Status |
|--------|-----------|--------|
| 2 | VS Code + Free Trial funcionando, notebook ETL rodando | ⬜ |
| 4 | Pipeline Delta com ACID, time travel e OPTIMIZE demonstrado | ⬜ |
| 6 | Arquitetura Medallion completa publicada no GitHub | ⬜ |
| 8 | DLT pipeline + Workflow orquestrado funcionando | ⬜ |
| 10 | Benchmark de performance documentado no guia_tecnico.md | ⬜ |
| 12 | Aprovação no exame Databricks Certified Data Engineer Associate | ⬜ |

---

## PORTFÓLIO — ENTREGAS

| Projeto | Semana | Tecnologias | GitHub |
|---------|--------|-------------|--------|
| Hello Databricks (setup) | 1 | Free Trial, VS Code Extension, Serverless | ⬜ |
| ETL Spark no Databricks | 2 | Databricks Runtime, DBFS, dbutils | ⬜ |
| Delta Lake Migration | 3–4 | Delta Lake, ACID, time travel | ⬜ |
| Pipeline Medallion NYC Taxi | 5 | Bronze/Silver/Gold, Auto Loader | ⬜ |
| Dashboard Lakeview | 6 | Databricks SQL, SQL Warehouse | ⬜ |
| Catalog Governado | 7 | Unity Catalog, RBAC, column masking | ⬜ |
| DLT + Workflow Pipeline | 8 | DLT, Workflows, expectations | ⬜ |
| ML com MLflow | 9 | MLflow, Model Registry, Feature Store | ⬜ |
| Pipeline com CI/CD | 11 | DABs, GitHub Actions, testing | ⬜ |
