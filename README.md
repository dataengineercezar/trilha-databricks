# Trilha Databricks — Data Engineer → Certificação

Repositório de estudos pessoal — referência técnica, exercícios resolvidos e projetos práticos para dominar a plataforma Databricks do zero até a certificação.
Organizado como guia de consulta para revisão rápida e aprofundamento progressivo.

---

## Onde estou agora

| Semana | Tema | Status | Notebooks |
|--------|------|--------|-----------|
| 1 | Setup: Free Trial + VS Code Extension + Databricks CLI + Serverless | ✅ | `semana01_setup/` |
| 2 | Spark no Databricks — clusters, notebooks, magic commands, DBFS | ✅ | Q1–Q4 + local script |
| 3 | Delta Lake fundamentos — ACID, time travel, schema enforcement | ✅ | Q1_Parquet_vs_Delta, Q2_Transaction_Log, Q3_MERGE_INTO, Q4_Time_Travel |
| 4 | Delta Lake avançado — OPTIMIZE, VACUUM, Z-ORDER, MERGE | ✅ | Q1_Small_Files_OPTIMIZE, Q2_ZORDER_DataSkipping, Q3_VACUUM_SchemaEvolution |
| 5 | Medallion Architecture — Bronze / Silver / Gold no Databricks | ✅ | bronze_ingest (AutoLoader) |
| 6 | Databricks SQL — QUALIFY, window functions, PIVOT, Delta SQL | ✅ | SQL_Avancado_Databricks |
| 7 | Unity Catalog — Managed vs External, GRANT, Row Filter, Column Mask | ✅ | Unity_Catalog_Governanca |
| 8 | Workflows + Delta Live Tables — Jobs, DLT, @dlt.expect | ✅ | Workflows_DLT |
| 9 | MLflow — tracking, Model Registry UC, batch inference com Spark UDF | ✅ | MLflow_Experimentos_Registry |
| 10 | Performance — Photon, AQE, Delta Cache, Partition Pruning, Spark UI | ✅ | Performance_Photon_AQE_Cache |
| 11 | CI/CD — Repos, Databricks Asset Bundles, GitHub Actions | ✅ | CICD_Asset_Bundles |
| 12 | Certificação — revisão completa + 20 questões simulado | ✅ | Revisao_Certificacao |

---

## Conteúdo Premium — Semanas 03 a 12

As semanas 03–12 estão disponíveis no repositório privado, com acesso liberado automaticamente após a compra.

> **[Adquirir acesso premium → datawizard8.gumroad.com/l/vdmiqz](https://datawizard8.gumroad.com/l/vdmiqz)**

No checkout, informe seu **usuário do GitHub** — o convite ao repositório privado é enviado automaticamente em segundos.

| Semana | Tema |
|--------|------|
| 3 | Delta Lake fundamentos — ACID, time travel, schema enforcement |
| 4 | Delta Lake avançado — OPTIMIZE, VACUUM, Z-ORDER, MERGE |
| 5 | Medallion Architecture — Bronze / Silver / Gold, AutoLoader |
| 6 | Databricks SQL — QUALIFY, window functions, PIVOT |
| 7 | Unity Catalog — GRANT, Row Filter, Column Mask |
| 8 | Workflows + Delta Live Tables |
| 9 | MLflow — tracking, Model Registry, Spark UDF |
| 10 | Performance — Photon, AQE, Delta Cache, Spark UI |
| 11 | CI/CD — Asset Bundles, GitHub Actions |
| 12 | Certificação — revisão completa + simulado |

---

## Estrutura do repositório

```
trilha-databricks/
│
├── README.md                          ← você está aqui
│
├── documentos/
│   └── guia_tecnico.md                ← guia técnico principal (referência viva)
│
├── exercicios/
│   ├── semana01_setup/                ← Free Trial, Serverless, VS Code Extension, CLI
│   ├── semana02_spark_databricks/     ← Clusters, notebooks, DBFS, magic commands
│   ├── semana03_delta_fundamentos/    ← ACID, time travel, schema enforcement
│   ├── semana04_delta_avancado/       ← OPTIMIZE, VACUUM, Z-ORDER, MERGE
│   ├── semana05_medallion/            ← Bronze/Silver/Gold, streaming ingest
│   ├── semana06_databricks_sql/       ← SQL Warehouses, dashboards
│   ├── semana07_unity_catalog/        ← Governança, catalogs, row/col-level security
│   ├── semana08_workflows_dlt/
│   ├── semana09_mlflow/
│   ├── semana10_performance/
│   ├── semana11_cicd/
│   └── semana12_certificacao/
│
└── PLANO_PERSONALIZADO.md             ← diagnóstico, distribuição semanal e metas
```

---

## O guia técnico principal

Tudo que for estudado será documentado em
[documentos/guia_tecnico.md](documentos/guia_tecnico.md).

Ele contém:
- Conceitos aprofundados com explicação de **como funcionam por dentro**
- Código real com saídas e análise de planos de execução
- Padrões de produção e armadilhas comuns
- Exercícios resolvidos com comentários de review

---

## Setup do ambiente — 100% gratuito

### Opção 1 — Databricks Free Trial (recomendada)

```
1. Acesse: https://www.databricks.com/try-databricks
2. Preencha o formulário (e-mail + senha) — sem cartão de crédito
3. Escolha AWS como cloud provider
4. Confirme o e-mail
5. Acesse: https://dbc-XXXXXXXX.cloud.databricks.com
```

> O Free Trial (14–30 dias) inclui tudo que a trilha usa:
> Unity Catalog, Workflows, Delta Live Tables, SQL Warehouses,
> Serverless Compute, Photon, Repos e Databricks Assistant.
> Ao expirar, crie uma nova conta com outro e-mail.

### Opção 2 — VS Code + Databricks Extension (desenvolvimento local)

```powershell
# 1. Instalar a extensão Databricks no VS Code
#    ID: databricks.databricks

# 2. Instalar Databricks CLI
pip install databricks-sdk
databricks configure --host https://dbc-962e23c9-e80d.cloud.databricks.com

# 3. Instalar Databricks Connect (código local → cluster remoto)
pip install databricks-connect
```

> Com Databricks Connect v2, você escreve PySpark no VS Code e ele
> executa no cluster do Trial — IntelliSense, debug e testes locais.

### Opção 3 — Delta Lake local (100% offline)

```powershell
# Para exercícios que não exigem o workspace (semanas 3–4)
pip install delta-spark pyspark mlflow
```

### Ambiente Python local

```powershell
python -m venv .venv
.\.venv\Scripts\Activate.ps1
# ATENÇÃO: não instale pyspark separadamente — conflita com databricks-connect
pip install "databricks-connect==18.1.*" databricks-sdk databricks-cli
pip install mlflow delta-spark pandas pyarrow ipykernel scikit-learn ruff pytest
```

---

## Databricks VS Code Extension — guia rápido

A extensão oficial (ID: `databricks.databricks`) fornece:

| Funcionalidade | Descrição |
|----------------|-----------|
| Workspace browser | Navegar e editar notebooks no workspace |
| Cluster manager | Iniciar/parar clusters diretamente do VS Code |
| File sync | Sincronizar arquivos locais com Repos do workspace |
| Databricks Connect | Rodar PySpark localmente contra cluster remoto |
| Run on Databricks | Executar scripts Python/notebooks no cluster |

Configuração básica após instalar a extensão:
1. `Ctrl+Shift+P` → "Databricks: Configure Workspace"
2. Inserir URL do workspace (ex: `https://dbc-962e23c9-e80d.cloud.databricks.com`)
3. Inserir token de acesso (User Settings → Access Tokens no workspace)

---

## Importando notebooks para o Databricks

### Opção 1 — Upload pela UI (notebook individual)

1. Acesse o workspace Databricks
2. No menu lateral, clique em **Workspace** → navegue até a pasta de destino
3. Clique em **⋮** ou **Create** → **Import**
4. Arraste o arquivo `.ipynb` ou clique para selecionar
5. Clique **Import**

### Opção 2 — Databricks CLI (recomendada, importa a pasta inteira)

```powershell
# Importar TODOS os exercícios da trilha de uma vez
databricks workspace import-dir `
  "d:\3_Estudos\TRILHA_DATABRICKS\exercicios" `
  /Users/<seu-email>@gmail.com `
  --overwrite

# Importar uma semana específica
databricks workspace import-dir `
  "d:\3_Estudos\TRILHA_DATABRICKS\exercicios\semana02_spark_databricks\notebooks" `
  /Users/<seu-email>@gmail.com/semana02

# Para um notebook individual
databricks workspace import `
  "d:\3_Estudos\TRILHA_DATABRICKS\exercicios\semana02_spark_databricks\notebooks\Q1_DBR_Configs.ipynb" `
  /Users/<seu-email>@gmail.com/semana02/Q1_DBR_Configs `
  --format JUPYTER
```

> O perfil padrão é configurado automaticamente pela VS Code Extension em `~/.databrickscfg`.
> O flag `--format JUPYTER` é necessário para arquivos `.ipynb`.

---

## Recursos gratuitos de referência

| Recurso | URL | Tipo |
|---------|-----|------|
| Databricks Academy | academy.databricks.com | Cursos gratuitos + certificação |
| Databricks Docs | docs.databricks.com | Documentação oficial |
| Delta Lake docs | delta.io/learn | Delta Lake open-source |
| MLflow docs | mlflow.org/docs | MLflow open-source |
| Databricks GitHub | github.com/databricks | Exemplos e notebooks |
| Data + AI Summit talks | youtube.com/@Databricks | Vídeos técnicos avançados |
