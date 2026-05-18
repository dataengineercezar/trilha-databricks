# Semana 1 — Setup: Free Trial + VS Code Extension + CLI

**Objetivo:** Ter o ambiente Databricks 100% funcional no VS Code, sem pagar nada, antes de escrever a primeira linha de Spark.

**Pré-requisitos:**
- Conta Google ou Microsoft (para criar conta no Databricks)
- VS Code instalado com Python extension
- Python 3.11+ no sistema

---

## Checklist de setup

> ⚠️ **Compatível com Serverless** — Este guia foi validado com Serverless compute (Free Trial 2025+).
> Alguns recursos de API legada não estão disponíveis no Serverless:
>
> | Recurso | Serverless | Cluster clássico |
> |---------|------------|------------------|
> | `spark` (SparkSession) | ✅ | ✅ |
> | `display(df)` em notebook | ✅ | ✅ |
> | `dbutils.fs`, `dbutils.secrets` | ✅ | ✅ |
> | `sc` / SparkContext | ❌ | ✅ |
> | `%sh` | ✅ (driver node) | ✅ |
> | Databricks Connect | ✅ (18.1.x) | ✅ |

### Passo 1 — Databricks Free Trial

```
1. Acesse: https://www.databricks.com/try-databricks
2. Clique em "Try Databricks" ou "Get started for free"
3. Preencha o formulário com e-mail e senha
4. Escolha um cloud provider (AWS recomendado — mais simples)
5. Confirme o email
6. Faça login — a URL será: https://dbc-XXXXXXXX.cloud.databricks.com
```

> ℹ️ **Free Trial vs Community Edition:**
> O trial padrão (14–30 dias, sem cartão) é **superior** ao Community Edition:
> inclui Unity Catalog, Jobs & Pipelines, DLT, SQL Warehouses e Serverless.
> Após o trial, você pode criar um novo e-mail para um novo trial.
> O **Community Edition** (`community.cloud.databricks.com`) ainda existe mas
> não tem Unity Catalog nem Workflows — evite para esta trilha.

**Navegue pelo workspace e identifique:**
- [ ] Workspace (notebooks e pastas)
- [ ] Catalog (Unity Catalog — catálogo, schemas, tabelas)
- [ ] Jobs & Pipelines (Workflows e Delta Live Tables)
- [ ] Compute (clusters interativos e SQL Warehouses)
- [ ] SQL → SQL Editor / SQL Warehouses
- [ ] Marketplace (datasets e modelos públicos)
- [ ] Data Engineering → Runs / Data Ingestion

---

### Passo 2 — Compute: Serverless (rápido) ou Cluster (completo)

**Opção A — Serverless (recomendado para exercícios interativos):**
```
No notebook: canto superior direito → Connect → Serverless
Início instantâneo, sem esperar cluster ligar.
Ideal para a maioria dos notebooks desta trilha.
```

**Opção B — Serverless também para Databricks Connect (sem cluster necessário):**

O Databricks Connect v2 (versão 14+) suporta **Serverless** como alvo de execução.
Não é necessário criar um All-Purpose cluster — o Serverless cobre todos os casos desta trilha.

```python
# hello_connect_serverless.py
from databricks.connect import DatabricksSession

# Substitua "SEU_PERFIL" pelo nome do perfil em ~/.databrickscfg
spark = DatabricksSession.builder.profile("SEU_PERFIL").serverless(True).getOrCreate()

df = spark.range(10).toDF("numero")
df.show()
print(f"Spark version: {spark.version}")
```

> ℹ️ Workspaces novos (Trial 2025+) são Serverless-first.
> A opção de criar All-Purpose Clusters pode não estar disponível na UI padrão.
> Use Serverless para tudo — é mais rápido e cobre 100% dos exercícios desta trilha.

---

### Passo 3 — Gerar Access Token

```
1. Canto superior direito → seu nome → Settings
2. Developer → Access Tokens → Manage
3. Generate new token
4. Preencha:
   - Name: "vscode-dev"
   - Lifetime (days): 90
   - Scope: Other APIs       ← para VS Code, CLI e Databricks Connect
   - API scope(s): deixar em branco  ← acesso irrestrito a todas as APIs
5. Clique em Generate
6. COPIE O TOKEN AGORA — ele não será exibido novamente
7. Salve em um lugar seguro (não no Git!)
```

> ℹ️ **Scope "BI Tools"** é exclusivo para ferramentas como Tableau e Power BI
> (apenas SQL). Não use para desenvolvimento.

---

### Passo 4 — VS Code Extension

> 📁 **Pasta:** qualquer uma — o comando `code` e a extensão são globais.

```powershell
# Instalar extensão (terminal do VS Code ou terminal do sistema)
code --install-extension databricks.databricks

# Ou: Ctrl+Shift+X → buscar "Databricks" → instalar a extensão da Databricks Inc.
```

**Configurar conexão:**
```
1. Na sidebar do VS Code, clique no ícone do Databricks
2. "Add workspace" ou "Configure"
3. Host: https://<seu-workspace>.cloud.databricks.com
4. Authentication: Personal Access Token
5. Token: cole o token gerado no Passo 3
6. "Select a cluster" → escolha Serverless
7. Python Environment → PULE este passo (faremos manualmente no Passo 5)
```

> ⚠️ **NÃO use o botão "Install databricks-connect" da extensão.**
> Ele instala no venv ativo no momento — geralmente o errado.
> Sempre instale manualmente no venv do projeto (Passo 5).

A extensão criará um perfil em `~/.databrickscfg` com o nome da conexão que você configurou.
Anote esse nome — você usará no Passo 7.

---

### Passo 5 — Ambiente Python local

> 📁 **Pasta:** `d:\3_Estudos\TRILHA_DATABRICKS` — o venv deve ser criado dentro do projeto.

```powershell
# No diretório TRILHA_DATABRICKS
cd d:\3_Estudos\TRILHA_DATABRICKS
python -m venv .venv
.\.venv\Scripts\Activate.ps1

# 1. Instalar databricks-connect PRIMEIRO (versão 18.1.x — compatível com Serverless)
#    NUNCA instale pyspark separadamente — o databricks-connect já o inclui embutido
pip install "databricks-connect==18.1.*"

# 2. Instalar demais dependências
pip install databricks-sdk
pip install mlflow pandas pyarrow ipykernel jupyter ruff pytest

# 3. Verificar
python -c "from databricks.connect import DatabricksSession; print('databricks-connect OK')"
```

> ⚠️ **Regras críticas para este venv:**
> - `pyspark` e `databricks-connect` **não podem coexistir** — o connect já inclui PySpark
> - `delta-spark` também puxa `pyspark` como dependência — não instale junto
> - Instale sempre o `databricks-connect` **antes** de qualquer outro pacote que possa puxar pyspark
> - Sempre use este venv para código Spark — nunca misture com outros projetos

---

### Passo 6 — Databricks CLI v2

> 📁 **Pasta:** qualquer uma — o CLI é instalado no sistema, não no venv.
> Os comandos `winget` e `databricks` são globais e funcionam em qualquer diretório.

O Databricks CLI v2 é um binário Go independente — **não é um pacote pip**.
Instale uma única vez no sistema:

```powershell
# Instalar via winget (Windows Package Manager)
winget install Databricks.DatabricksCLI

# Feche e reabra o terminal após a instalação
# Verificar:
databricks --version
# Saída esperada: Databricks CLI v0.2xx.x
# Obs: a versão exibida pode diferir da versão baixada pelo winget (comportamento normal)
```

**Configurar com o perfil já existente (criado pela extensão VS Code):**

```powershell
# Ver o perfil criado pela extensão:
Get-Content "$env:USERPROFILE\.databrickscfg"
# Saída esperada:
# [nome_do_perfil]
# host = https://<seu-workspace>.cloud.databricks.com
# token = dapi...
```

Se quiser adicionar um perfil `DEFAULT` para não precisar especificá-lo nos comandos:

```powershell
# Isso pergunta o host e o token e salva como [DEFAULT]
databricks configure --token
# Host: https://<seu-workspace>.cloud.databricks.com
# Token: (cole o token do Passo 3)
```

**Testar a conexão:**

```powershell
databricks current-user me          # retorna JSON com seu usuário — confirma auth OK
databricks workspace list /         # lista a raiz do workspace
```

Saída esperada de `current-user me`:
```json
{
  "active": true,
  "userName": "seu@email.com",
  "entitlements": [ ... ],
  "groups": [ ... ]
}
```

> ℹ️ O CLI v2 usa automaticamente o único perfil do `~/.databrickscfg` quando não há `[DEFAULT]`.
> Em workspaces Serverless-first, `databricks clusters list` retorna vazio — é normal.

---

### Passo 7 — Databricks Connect (execução local → Serverless remoto)

> 📁 **Pasta:** `d:\3_Estudos\TRILHA_DATABRICKS` com o venv ativo.

> ℹ️ Já instalado no Passo 5. Versão necessária: `18.1.*` (18.2+ não suporta Serverless).

**Descubra o nome do seu perfil:**
```powershell
Get-Content "$env:USERPROFILE\.databrickscfg"
# A linha [nome_entre_colchetes] é o nome do perfil
```

```python
# hello_connect.py — SALVE este código em um arquivo .py, não execute no PowerShell
# O PowerShell não interpreta Python — use sempre: python nome_do_arquivo.py
from databricks.connect import DatabricksSession

# Substitua "SEU_PERFIL" pelo nome entre colchetes no ~/.databrickscfg
# Ex: "DEFAULT", "dev", "cezar_databricks" — depende do que você configurou
PROFILE = "SEU_PERFIL"

spark = DatabricksSession.builder.profile(PROFILE).serverless(True).getOrCreate()

df = spark.range(10).toDF("numero")
df.show()

print(f"Spark version: {spark.version}")
print("Serverless funcionando!")
```

```powershell
# Executar com o venv ativo:
# ⚠️ Não cole o código Python no PowerShell — salve em arquivo .py e execute assim:
python hello_connect.py
```

> ℹ️ **Dica para evitar repetir o nome do perfil em todo arquivo:**
> Adicione `[DEFAULT]` ao `~/.databrickscfg` com as mesmas credenciais.
> Com um perfil `DEFAULT`, `.profile()` pode ser omitido e o SDK usa automaticamente.

---

## Exercício — Exploração do Workspace

No notebook Databricks (crie em Workspace → New → Notebook):

```python
# Célula 1: Explorar DBFS
display(dbutils.fs.ls("dbfs:/"))
```

```python
# Célula 2: Ver datasets de exemplo incluídos no Databricks
display(dbutils.fs.ls("dbfs:/databricks-datasets/"))
```

```python
# Célula 3: Qual é o runtime e a versão do Spark?
print(spark.version)
# sc.version não existe no Serverless — use spark.sparkContext apenas em clusters clássicos

import sys
print(f"Python: {sys.version}")
print(f"Spark: {spark.version}")
```

```python
# Célula 4: Ler um dataset de exemplo e registrar como view temporária
df = spark.read.csv(
    "dbfs:/databricks-datasets/samples/population-vs-price/data_geo.csv",
    header=True,
    inferSchema=True
)
df.printSchema()
display(df)

# Registrar como view para usar no %sql da próxima célula
df.createOrReplaceTempView("population_price")
```

```sql
-- Célula 5 (use %sql):
-- Colunas com espaços no nome precisam de backticks no SQL
SELECT `State Code`, `State`, `2015 median sales price`
FROM (
  SELECT *,
         ROW_NUMBER() OVER (ORDER BY `2015 median sales price` DESC) as rank
  FROM population_price
)
WHERE rank <= 10
```

---

## Q1 — dbutils.fs profundo

**Tarefa:** Usando apenas `dbutils.fs` (sem `spark.read`):

1. Liste o conteúdo de `dbfs:/databricks-datasets/`
2. Identifique 3 datasets disponíveis com suas descrições
3. Crie a pasta `/Volumes/workspace/estudos/semana01/`
4. Crie um arquivo de texto nessa pasta com `dbutils.fs.put()`
5. Leia o arquivo de volta com `dbutils.fs.head()`
6. Delete a pasta com `dbutils.fs.rm(..., recurse=True)`

**Pergunta para reflexão:** O que acontece com os arquivos em `dbfs:/tmp/` quando a sessão Serverless termina?
E como isso difere do comportamento em um cluster clássico que é encerrado?

> 💡 Dica: No Serverless, não há "ciclo de vida de cluster" — o compute é efemero mas o DBFS é persistente.

---

## Q2 — Magic commands na prática

Em um notebook Databricks, demonstre o uso de pelo menos 5 magic commands diferentes:
- `%fs`, `%sh`, `%md`, `%sql`, `%run` (ou `%scala`)

Para `%sh`, execute:
```bash
pip list | grep -E "pyspark|delta|mlflow"
java -version
echo "Hostname: $(hostname)"
```

**Pergunta:** O `%sh` roda no driver node ou nos worker nodes?

---

## Armadilhas comuns e soluções

Problemas reais encontrados ao seguir este guia — documentados para evitar retrabalho.

### 1. `sc` não existe no Serverless
```
NotImplementedError: sc is not supported on serverless compute
```
**Causa:** `sc` (SparkContext) é API legada, não exposta no Serverless.  
**Solução:** Use sempre `spark` (SparkSession). Substitua `sc.version` por `spark.version`.

---

### 2. `delta.`caminho`` em arquivo CSV
```
[DELTA_MISSING_TRANSACTION_LOG] There is no transaction log present
```
**Causa:** A sintaxe `delta.`...`` só funciona para tabelas Delta (com `_delta_log/`).  
**Solução:** Para CSV/Parquet, use `spark.read.csv(...)` + `createOrReplaceTempView("nome")` e referencie a view no `%sql`.

---

### 3. Nomes de colunas com espaços no SQL
```
[UNRESOLVED_COLUMN] column `2015_median_sales_price` cannot be resolved.
Did you mean `2015 median sales price`?
```
**Causa:** O CSV tinha nomes de colunas com espaços. SQL interpreta `2015_median_sales_price` como identificador inválido.  
**Solução:** Use backticks: `` `2015 median sales price` ``

---

### 4. `databricks-connect 18.2+` não suporta Serverless
```
Exception: Databricks Connect 18.2.0 is unsupported with serverless.
Install the recommended version: pip install --upgrade "databricks-connect==18.1.*"
```
**Causa:** A versão 18.2 removeu suporte a Serverless temporariamente.  
**Solução:** Fixar a versão: `pip install "databricks-connect==18.1.*"`

---

### 5. `pyspark` instalado junto com `databricks-connect`
```
Exception: pyspark and databricks-connect cannot be installed at the same time.
```
**Causa:** `databricks-connect` já inclui PySpark embutido. Ter os dois gera conflito.  
**Solução:**
```powershell
pip uninstall -y pyspark databricks-connect
pip install "databricks-connect==18.1.*"
```
> Atenção: `delta-spark` também pede `pyspark` como dependência — não instale no mesmo venv.

---

### 6. Botão "Install databricks-connect" da extensão VS Code instala no venv errado
**Causa:** A extensão instala no venv ativo no momento, não necessariamente no venv do projeto.  
**Solução:** **Não use o botão.** Instale manualmente com o venv do projeto ativo:
```powershell
cd d:\3_Estudos\TRILHA_DATABRICKS
.\.venv\Scripts\Activate.ps1
pip install "databricks-connect==18.1.*"
```

---

### 7. Sem perfil `[DEFAULT]` no `~/.databrickscfg`
```
ValueError: cannot configure default credentials
```
**Causa:** A extensão VS Code cria um perfil com nome personalizado, não `[DEFAULT]`. O SDK sem especificar perfil falha.  
**Solução:** Sempre especifique o perfil no código:
```python
spark = DatabricksSession.builder.profile("SEU_PERFIL").serverless(True).getOrCreate()
```
Ou adicione manualmente `[DEFAULT]` ao `~/.databrickscfg` com as mesmas credenciais.

---

## Referência

Ver seção **PLATAFORMA DATABRICKS** em [../../documentos/guia_tecnico.md](../../documentos/guia_tecnico.md)
