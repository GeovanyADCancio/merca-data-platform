# Databricks notebook source
# MAGIC %md
# MAGIC # Comparação de Schema — Squad 1 vs Squad 3 (tabelas de e-commerce)
# MAGIC
# MAGIC O Lead autorizou o uso de dados da Squad 3 (batch) como histórico complementar para o treino do
# MAGIC modelo de anomalias, já que ela cobre um volume maior do que o disponível em tempo real. Um colega
# MAGIC da Squad 3 confirmou verbalmente que o schema das tabelas de e-commerce é o mesmo, apenas em modo
# MAGIC batch. Este notebook confirma essa afirmação de forma programática, comparando o conjunto de
# MAGIC colunas de cada tabela entre os dois containers — prática coerente com a missão de Data Quality da
# MAGIC squad, que não deveria depender de confirmação verbal para decisões que afetam o pipeline.
# MAGIC
# MAGIC O código é adaptado de um trecho compartilhado pela Squad 3, reescrito para usar os helpers já
# MAGIC existentes (`get_delta_path`, `get_storage_options`) em vez de funções que só existiam no notebook
# MAGIC de origem.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Pré-requisito: versão da biblioteca `deltalake`
# MAGIC
# MAGIC A versão pré-instalada da biblioteca `deltalake` neste ambiente não suporta leitura de tabelas
# MAGIC com o recurso *Deletion Vectors* ativado — uma limitação conhecida de versões mais antigas do
# MAGIC motor `delta-rs`. Como as tabelas da Squad 3 usam esse recurso (provavelmente por conta de
# MAGIC operações de `MERGE` no pipeline batch), é necessário atualizar a biblioteca antes de prosseguir.
# MAGIC O reinício do interpretador Python é obrigatório: sem ele, a versão antiga permanece carregada
# MAGIC em memória mesmo após a instalação da nova.

# COMMAND ----------

# MAGIC %pip install -U deltalake

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %run ../99_utils/feat_squad1_99_helpers

# COMMAND ----------

# MAGIC %md
# MAGIC ## Funções de comparação
# MAGIC
# MAGIC `get_delta_path_squad3` replica a lógica de `get_delta_path` (já definida no helper da squad),
# MAGIC apontando para o container `squad3` em vez de `squad1` — mesma Storage Account, mesmas credenciais.
# MAGIC `obter_colunas` lê apenas os metadados da tabela Delta (não os dados em si), retornando o conjunto
# MAGIC de nomes de coluna.

# COMMAND ----------

from deltalake import DeltaTable

def get_delta_path_squad3(camada: str, tabela: str) -> str:
    return f"abfss://squad3@{ADLS_STORAGE_ACCOUNT}.dfs.core.windows.net/{camada}/{tabela}"

def obter_colunas(camada: str, tabela: str, path_fn) -> set:
    try:
        dt = DeltaTable(path_fn(camada, tabela), storage_options=get_storage_options())
        return {campo["name"] for campo in dt.schema().json()["fields"]}
    except Exception as e:
        log.warning(f"Não foi possível ler '{tabela}' via {path_fn.__name__}: {str(e)[:100]}")
        return set()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Comparação tabela por tabela
# MAGIC
# MAGIC As cinco tabelas mapeadas no diagnóstico original da Squad 1 são comparadas contra a mesma
# MAGIC tabela na camada Silver da Squad 3.

# COMMAND ----------

TABELAS_COMPARAR = [
    "ecommerce_pedidos",
    "ecommerce_itens_pedido",
    "ecommerce_produtos",
    "ecommerce_categorias",
    "ecommerce_clientes",
]

resultado_comparacao = {}

for tabela in TABELAS_COMPARAR:
    print(f"\n🔍 Comparando: {tabela}")
    colunas_squad1 = obter_colunas("silver", tabela, get_delta_path)
    colunas_squad3 = obter_colunas("silver", tabela, get_delta_path_squad3)

    if not colunas_squad1 or not colunas_squad3:
        print("   ⏩ Não foi possível comparar — uma das duas tabelas não foi lida (ver aviso acima).")
        resultado_comparacao[tabela] = "não verificado"
        continue

    if colunas_squad1 == colunas_squad3:
        print(f"   ✅ Schemas idênticos ({len(colunas_squad1)} colunas)")
        resultado_comparacao[tabela] = "idêntico"
    else:
        so_squad1 = colunas_squad1 - colunas_squad3
        so_squad3 = colunas_squad3 - colunas_squad1
        print(f"   ⚠️ Schemas diferentes")
        print(f"      Só na Squad 1: {so_squad1 or '(nenhuma)'}")
        print(f"      Só na Squad 3: {so_squad3 or '(nenhuma)'}")
        resultado_comparacao[tabela] = "diferente"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Resumo

# COMMAND ----------

print("📋 Resumo da comparação de schema:\n")
for tabela, status in resultado_comparacao.items():
    print(f"   {status:15s} — {tabela}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Volumetria comparada
# MAGIC
# MAGIC A comparação de schema confirma que os dados são *utilizáveis* em conjunto, mas não diz nada
# MAGIC sobre o ganho real de volume. Squad 1 opera sobre um fluxo em tempo real ainda em estabilização
# MAGIC (conforme orientação do Lead), o que explica o volume reduzido observado até aqui; Squad 3,
# MAGIC sendo batch, tende a operar sobre um histórico já consolidado. Esta seção conta as linhas de cada
# MAGIC lado para substituir a expectativa por um número concreto.

# COMMAND ----------

def contar_linhas(camada: str, tabela: str, path_fn) -> int:
    try:
        dt = DeltaTable(path_fn(camada, tabela), storage_options=get_storage_options())
        return dt.to_pandas().shape[0]
    except Exception as e:
        log.warning(f"Não foi possível contar '{tabela}' via {path_fn.__name__}: {str(e)[:100]}")
        return -1

print("📊 Volumetria — Squad 1 vs Squad 3 (camada Silver):\n")
for tabela in TABELAS_COMPARAR:
    total_squad1 = contar_linhas("silver", tabela, get_delta_path)
    total_squad3 = contar_linhas("silver", tabela, get_delta_path_squad3)
    ganho = f"{total_squad3 / total_squad1:.1f}x" if total_squad1 > 0 else "n/d"
    print(f"{tabela:25s} squad1={total_squad1:>8} | squad3={total_squad3:>8} | ganho={ganho}")

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC Se todas as tabelas relevantes ao modelo de anomalia aparecerem como "idêntico", os dados da
# MAGIC Squad 3 podem ser incorporados como histórico adicional na etapa de *Modeling* — bastando aplicar
# MAGIC a mesma lógica de `construir_features_pedidos` sobre a união (`union`) dos dois conjuntos de dados.
# MAGIC Qualquer tabela marcada como "diferente" precisa de reconciliação de schema antes de ser usada,
# MAGIC e deve ser reportada à squad antes de prosseguir.
# MAGIC
# MAGIC **Alerta para quando os dados da Squad 3 forem efetivamente lidos (não só o schema):** a função
# MAGIC `ler_delta` do helper possui um fallback que ignora a biblioteca `deltalake` e lê os arquivos
# MAGIC Parquet diretamente via Azure SDK, caso a leitura padrão falhe. Esse fallback não interpreta o
# MAGIC protocolo Delta de forma alguma — ou seja, se ele for acionado numa tabela com Deletion Vectors,
# MAGIC as linhas marcadas como removidas **não serão filtradas**, e aparecerão de volta nos dados como se
# MAGIC nunca tivessem sido deletadas. Com a biblioteca atualizada (célula acima), esse fallback não deve
# MAGIC ser necessário para as tabelas da Squad 3 — mas vale essa checagem manual antes de usar os dados
# MAGIC para treinar o modelo.
