# Databricks notebook source
# MAGIC %md
# MAGIC # Pipeline Medallion - `ecommerce_pedidos`
# MAGIC
# MAGIC Este notebook implementa a arquitetura **Medallion (Bronze -> Silver -> Gold)** para a tabela de pedidos do e-commerce, utilizando **PySpark no Databricks** com armazenamento no **Azure Data Lake Storage Gen2 (ADLS)**.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Fluxo do Pipeline
# MAGIC
# MAGIC ```
# MAGIC RAW (CSV no ADLS)
# MAGIC       |
# MAGIC       v
# MAGIC   BRONZE  - Ingestao bruta com metadados de auditoria + validacao de contrato
# MAGIC       |
# MAGIC       v
# MAGIC   SILVER  - Deduplicacao, parse de data, normalizacao de status/canal/pagamento
# MAGIC       |
# MAGIC       v
# MAGIC   GOLD    - Data Marts batch para analise de receita, pagamento, cancelamento, dia da semana e estado
# MAGIC       |
# MAGIC       v
# MAGIC   SQL SERVER - Exportacao do KPI para consumo em relatorios/BI
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Regras de Negocio Aplicadas (Silver)
# MAGIC
# MAGIC | # | Regra | Descricao |
# MAGIC |---|-------|-----------|
# MAGIC | 1 | Unicidade e deduplicacao | Remocao de registros com `id_pedido` nulo e eliminacao de duplicatas pela mesma chave |
# MAGIC | 2 | Tipagem temporal | Conversao de `dt_pedido` para timestamp e descarte de registros com data invalida |
# MAGIC | 3 | Normalizacao textual | Padronizacao de `status_pedido` e `metodo_pagamento` |
# MAGIC | 4 | Normalizacao financeira | Conversao de `valor_total` e `valor_frete` para `Decimal(10,2)`, com virgula decimal tratada |
# MAGIC | 5 | Compatibilidade entre pipelines | Escrita da Silver em Parquet para atender o consumo atual de `ecommerce_itens_pedido` |
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## KPI Gerado (Gold)
# MAGIC
# MAGIC | Tabela Gold | Descricao | Granularidade |
# MAGIC |-------------|-----------|---------------|
# MAGIC | `gold_kpi_receita_mom` | Crescimento Mes a Mes (MoM) de receita liquida | Ano / Mes |
# MAGIC | `gold_kpi_ticket_medio_pagamento_anual` | Ticket medio anual por metodo de pagamento | Ano / Metodo de Pagamento |
# MAGIC | `gold_kpi_taxa_cancelamento_mensal` | Taxa de cancelamento mensal (%) | Ano / Mes |
# MAGIC | `gold_kpi_volume_dia_semana` | Volume historico de pedidos por dia da semana | Dia da Semana |
# MAGIC | `gold_kpi_receita_media_estado_entrega` | Receita media por pedido agrupada por estado de entrega | Ano / Mes / Estado |

# COMMAND ----------

# MAGIC %md
# MAGIC ## 0. Limpeza pontual do path Bronze (MANUAL - rodar UMA UNICA VEZ)
# MAGIC
# MAGIC > ⚠️ **MANTER 100% COMENTADO NAS EXECUCOES AGENDADAS.**
# MAGIC >
# MAGIC > Esta celula serve apenas para remover, **uma unica vez**, arquivos nao-Delta
# MAGIC > antigos que ficaram em `bronze/ecommerce_pedidos` e geram o erro
# MAGIC > `DELTA_MISSING_TRANSACTION_LOG`. O job semanal **NAO** deve executar isto —
# MAGIC > se rodar, apaga a camada Bronze toda segunda-feira.
# MAGIC >
# MAGIC > **Como usar (so na primeira vez):** descomente as duas celulas abaixo, rode-as,
# MAGIC > confirme que o Bronze foi limpo e **comente tudo de novo** antes de agendar.

# COMMAND ----------

# # ============================================================
# # 0.1 INSTALACAO DO SDK AZURE  --  *** MANTER COMENTADO NO JOB AGENDADO ***
# # ============================================================
# # (descomente apenas para a limpeza manual; reinicia o Python)

# %pip install azure-storage-file-datalake azure-identity python-dotenv -q

# dbutils.library.restartPython()

# COMMAND ----------

# # ============================================================
# # 0.2 LIMPEZA DO PATH BRONZE  --  *** MANTER COMENTADO NO JOB AGENDADO ***
# # ============================================================
# # Rode UMA UNICA VEZ, junto com a celula 0.1. Depois comente tudo novamente.

# import os
# from dotenv import load_dotenv
# from azure.identity import ClientSecretCredential
# from azure.storage.filedatalake import DataLakeServiceClient

# load_dotenv("../env"); load_dotenv("../.env"); load_dotenv(".env")

# _storage   = os.getenv("STORAGE_ACCOUNT_NAME")
# _container = os.getenv("SQUAD_CONTAINER", "squad3")

# _cred = ClientSecretCredential(
#     tenant_id=os.getenv("TENANT_ID"),
#     client_id=os.getenv("CLIENT_ID"),
#     client_secret=os.getenv("CLIENT_SECRET"),
# )
# _fs = DataLakeServiceClient(
#     account_url=f"https://{_storage}.dfs.core.windows.net",
#     credential=_cred,
# ).get_file_system_client(_container)

# # Apaga so o Bronze de pedidos (o que esta travando a escrita Delta).
# # Para um reprocesso 100% limpo, descomente tambem silver/gold.
# for rel in [
#     "bronze/ecommerce_pedidos",
#     "silver/ecommerce_pedidos",
#     # "gold/gold_kpi_receita_mom",
#     # "gold/gold_kpi_ticket_medio_pagamento_anual",
#     # "gold/gold_kpi_taxa_cancelamento_mensal",
#     # "gold/gold_kpi_volume_dia_semana",
#     # "gold/gold_kpi_receita_media_estado_entrega",
# ]:
#     d = _fs.get_directory_client(rel)
#     if d.exists():
#         d.delete_directory()
#         print(f"Removido: {_container}/{rel}")
#     else:
#         print(f"Nao existe (ok): {_container}/{rel}")

# COMMAND ----------

# ============================================================
# 1. SETUP E CREDENCIAIS (ecommerce_pedidos)
# ============================================================

import os
from datetime import date

from pyspark.sql.functions import (
    current_timestamp,
    year,
    month,
    col,
    count,
    avg,
    round,
    when,
    regexp_replace,
    coalesce,
    lit,
    date_format,
    to_timestamp,
    trim,
    upper,
    initcap,
    lower,
    dayofweek,
    lag,
    sum as _sum
)

from pyspark.sql.window import Window


# ============================================================
# 1.0 DOTENV OPCIONAL
# ============================================================
# No Databricks Free Edition, o pacote python-dotenv pode nao estar
# instalado. O pipeline nao deve depender dele: em Jobs, os valores
# entram por parametros; em execucao local, este fallback le arquivos
# simples no formato CHAVE=VALOR quando existirem.
# ============================================================

try:
    from dotenv import load_dotenv
except ModuleNotFoundError:
    def load_dotenv(dotenv_path=None, *args, **kwargs):
        if dotenv_path is None:
            return False

        try:
            with open(dotenv_path, "r", encoding="utf-8") as env_file:
                for raw_line in env_file:
                    line = raw_line.strip()

                    if not line or line.startswith("#") or "=" not in line:
                        continue

                    key, value = line.split("=", 1)
                    key = key.strip()
                    value = value.strip().strip('"').strip("'")

                    if key and key not in os.environ:
                        os.environ[key] = value

            return True
        except OSError:
            return False


# ============================================================
# 1.1 CARREGAMENTO DO .ENV + FALLBACK PARA JOB PARAMETERS
# ============================================================

try:
    load_dotenv("../env")
    load_dotenv("../.env")
    load_dotenv(".env")
    print("Tentativa de carregamento do .env realizada.")
except Exception as e:
    print(f"Nao foi possivel carregar .env. Seguindo com fallback. Detalhe: {e}")


def get_config_value(name: str, required: bool = True, default: str = "") -> str:
    """
    Busca uma configuracao na seguinte ordem:

    1. Variaveis de ambiente carregadas pelo .env
    2. Databricks Job Parameters, via dbutils.widgets.get()
    3. Valor default, quando informado
    """

    value = os.getenv(name)

    if value is None or str(value).strip() == "":
        try:
            value = dbutils.widgets.get(name)
        except Exception:
            value = None

    if value is None or str(value).strip() == "":
        value = default

    value = str(value).strip() if value is not None else ""

    if required and value == "":
        raise ValueError(
            f"Configuracao obrigatoria nao encontrada: {name}. "
            f"Verifique se ela existe no .env ou nos Job Parameters do Databricks."
        )

    return value


# ============================================================
# 1.2 VARIAVEIS ADLS GEN2
# ============================================================

client_id = get_config_value("CLIENT_ID")
tenant_id = get_config_value("TENANT_ID")
client_secret = get_config_value("CLIENT_SECRET")
storage_account = get_config_value("STORAGE_ACCOUNT_NAME")

# Pelo path padrao do projeto, o arquivo esta em:
# abfss://raw@storage/batch-data/ecommerce_pedidos.csv
source_folder = get_config_value("CONTAINER_NAME", required=False, default="batch-data")

# Containers usados no projeto
raw_container = get_config_value("RAW_CONTAINER", required=False, default="raw")
squad_container = get_config_value("SQUAD_CONTAINER", required=False, default="squad3")
reset_non_delta_paths = get_config_value("RESET_NON_DELTA_PATHS", required=False, default="false").lower() == "true"


# ============================================================
# 1.3 OPCOES DE AUTENTICACAO ADLS GEN2
# ============================================================
# Mantemos as opcoes em dicionario e usamos .options(**adls_options)
# para compatibilidade com Databricks Serverless/Free.
# ============================================================

storage_account_fqdn = f"{storage_account}.dfs.core.windows.net"

adls_options = {
    f"fs.azure.account.auth.type.{storage_account_fqdn}": "OAuth",
    f"fs.azure.account.oauth.provider.type.{storage_account_fqdn}": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
    f"fs.azure.account.oauth2.client.id.{storage_account_fqdn}": client_id,
    f"fs.azure.account.oauth2.client.secret.{storage_account_fqdn}": client_secret,
    f"fs.azure.account.oauth2.client.endpoint.{storage_account_fqdn}": f"https://login.microsoftonline.com/{tenant_id}/oauth2/token"
}

adls_options_generic = {
    "fs.azure.account.auth.type": "OAuth",
    "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
    "fs.azure.account.oauth2.client.id": client_id,
    "fs.azure.account.oauth2.client.secret": client_secret,
    "fs.azure.account.oauth2.client.endpoint": f"https://login.microsoftonline.com/{tenant_id}/oauth2/token"
}


# ============================================================
# 1.4 PATHS DA TABELA
# ============================================================

tabela = "ecommerce_pedidos"

path_raw = (
    f"abfss://{raw_container}@{storage_account}.dfs.core.windows.net/"
    f"{source_folder}/{tabela}.csv"
)

path_bronze = (
    f"abfss://{squad_container}@{storage_account}.dfs.core.windows.net/"
    f"bronze/{tabela}"
)

path_silver = (
    f"abfss://{squad_container}@{storage_account}.dfs.core.windows.net/"
    f"silver/{tabela}"
)

path_silver_enderecos = (
    f"abfss://{squad_container}@{storage_account}.dfs.core.windows.net/"
    "silver/ecommerce_enderecos"
)


# ============================================================
# 1.5 PERIODO DE REFERENCIA DA EXECUCAO
# ============================================================

_hoje = date.today()
ANO_EXEC = _hoje.year
MES_EXEC = _hoje.month
DIA_EXEC = _hoje.day


# ============================================================
# 1.6 LOG SEGURO DE VALIDACAO
# ============================================================

print(f"Configuracao finalizada para a tabela: {tabela}")
print(f"Periodo de execucao: {DIA_EXEC}/{MES_EXEC}/{ANO_EXEC}")
print(f"Storage Account: {storage_account}")
print(f"Raw Container: {raw_container}")
print(f"Source Folder: {source_folder}")
print(f"Squad Container: {squad_container}")
print(f"Path Raw: {path_raw}")
print(f"Path Bronze: {path_bronze}")
print(f"Path Silver: {path_silver}")
print(f"Path Silver Enderecos: {path_silver_enderecos}")
print(f"Reset automatico de paths nao Delta: {reset_non_delta_paths}")
print("Credenciais carregadas sem expor secrets.")


# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Camada Bronze - Ingestao RAW -> Bronze
# MAGIC
# MAGIC Realiza a ingestao do arquivo CSV bruto da camada Raw para a camada Bronze, incluindo validacao de contrato e metadados de auditoria.

# COMMAND ----------

# ============================================================
# 2. EXTRACAO E CARGA (RAW -> BRONZE)
# ============================================================
print(f"Lendo {tabela} da camada Raw...")

df_raw = (
    spark.read
    .format("csv")
    .option("header", "true")
    .option("inferSchema", "false")
    .options(**adls_options)
    .load(path_raw)
)

print(f"Total de registros encontrados: {df_raw.count()}")

colunas_esperadas = [
    "id_pedido",
    "id_cliente",
    "id_endereco_entrega",
    "dt_pedido",
    "status_pedido",
    "valor_total",
    "valor_frete",
    "metodo_pagamento",
    "dt_ultima_atualizacao_status"
]

colunas_atuais = df_raw.columns
colunas_faltantes = set(colunas_esperadas) - set(colunas_atuais)
colunas_extras = set(colunas_atuais) - set(colunas_esperadas)

if len(colunas_atuais) != len(colunas_esperadas) or colunas_faltantes or colunas_extras:
    raise ValueError(
        f"ERRO CRITICO: Contrato invalido. "
        f"Esperado: {colunas_esperadas}. Recebido: {colunas_atuais}. "
        f"Faltando: {list(colunas_faltantes)}. Extras: {list(colunas_extras)}."
    )

if colunas_atuais != colunas_esperadas:
    raise ValueError(
        f"ERRO CRITICO: A ordem das colunas esta incorreta. "
        f"Esperado: {colunas_esperadas}. Recebido: {colunas_atuais}."
    )

print("Validacao estrutural OK: quantidade, nomes e ordem das colunas corretos.")

timestamp_carga = current_timestamp()

df_bronze = (
    df_raw
    .withColumn("bronze_ingested_at", timestamp_carga)
    .withColumn("bronze_source_file", col("_metadata.file_path"))
    .withColumn("ano_particao", year(timestamp_carga))
    .withColumn("mes_particao", month(timestamp_carga))
)

print(f"Gravando fisicamente na camada Bronze: {path_bronze}")

# Escrita Delta direta, no mesmo padrao do pipeline de itens_pedido:
# a autenticacao vem por .options(**adls_options), sem spark.conf / spark._jvm
# (que sao bloqueados no Databricks Free/Serverless).
(
    df_bronze.write
    .format("delta")
    .mode("append")
    .options(**adls_options)
    .partitionBy("ano_particao", "mes_particao")
    .save(path_bronze)
)

print("Ingestao Bronze finalizada com sucesso!")
display(df_bronze.limit(5))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Camada Silver - Limpeza, Tipagem e Compatibilidade
# MAGIC
# MAGIC A Silver de `ecommerce_pedidos` e gravada em **Parquet** para manter compatibilidade com o pipeline existente de `ecommerce_itens_pedido`, que ja consome `silver/ecommerce_pedidos` nesse formato.

# COMMAND ----------

# ============================================================
# 3. CAMADA SILVER (PEDIDOS)
# ============================================================

print(f"Iniciando processamento da camada Silver para: {tabela}...")

df_bronze_read = spark.read.format("delta").options(**adls_options).load(path_bronze)

dt_pedido_parseado = coalesce(
    to_timestamp(col("dt_pedido"), "yyyy-MM-dd HH:mm:ss"),
    to_timestamp(col("dt_pedido"), "yyyy-MM-dd"),
    to_timestamp(col("dt_pedido"), "dd/MM/yyyy HH:mm:ss"),
    to_timestamp(col("dt_pedido"), "dd/MM/yyyy")
)

dt_status_parseado = coalesce(
    to_timestamp(col("dt_ultima_atualizacao_status"), "yyyy-MM-dd HH:mm:ss"),
    to_timestamp(col("dt_ultima_atualizacao_status"), "yyyy-MM-dd"),
    to_timestamp(col("dt_ultima_atualizacao_status"), "dd/MM/yyyy HH:mm:ss"),
    to_timestamp(col("dt_ultima_atualizacao_status"), "dd/MM/yyyy")
)

df_silver = (
    df_bronze_read

    # Regra 1: id_pedido nao nulo e unico
    .dropna(subset=["id_pedido"])
    .dropDuplicates(["id_pedido"])

    # Tipagens de identificadores
    .withColumn("id_pedido", col("id_pedido").cast("string"))
    .withColumn("id_cliente", col("id_cliente").cast("string"))
    .withColumn("id_endereco_entrega", col("id_endereco_entrega").cast("string"))

    # Regra 2: data do pedido tipada e valida
    .withColumn("dt_pedido", dt_pedido_parseado)
    .withColumn("dt_ultima_atualizacao_status", dt_status_parseado)
    .filter(col("dt_pedido").isNotNull())

    # Regra 3: normalizacao textual
    .withColumn("status_pedido", upper(trim(col("status_pedido").cast("string"))))
    .withColumn("metodo_pagamento", initcap(lower(trim(col("metodo_pagamento").cast("string")))))

    # Regra 4: normalizacao financeira
    .withColumn("valor_total", regexp_replace(col("valor_total"), ",", ".").cast("decimal(10,2)"))
    .withColumn("valor_total", coalesce(col("valor_total"), lit(0).cast("decimal(10,2)")))
    .withColumn("valor_frete", regexp_replace(col("valor_frete"), ",", ".").cast("decimal(10,2)"))
    .withColumn("valor_frete", coalesce(col("valor_frete"), lit(0).cast("decimal(10,2)")))

    # Particionamento historico baseado na data do pedido
    .withColumn("ano_particao", year(col("dt_pedido")))
    .withColumn("mes_particao", month(col("dt_pedido")))

    # Auditoria
    .withColumn("silver_processed_at", current_timestamp())
)

df_silver_final = df_silver.select(
    "id_pedido",
    "id_cliente",
    "id_endereco_entrega",
    "dt_pedido",
    "status_pedido",
    "valor_total",
    "valor_frete",
    "metodo_pagamento",
    "dt_ultima_atualizacao_status",
    "ano_particao",
    "mes_particao",
    "silver_processed_at"
)

print(f"Gravando dados limpos em Parquet: {path_silver}")

(
    df_silver_final.write
    .format("parquet")
    .mode("append")
    .options(**adls_options)
    .partitionBy("ano_particao", "mes_particao")
    .save(path_silver)
)

print("SUCESSO! Pedidos refinados e salvos na Silver em Parquet.\n")
display(df_silver_final.limit(5))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Camada Gold - KPIs Batch de Pedidos
# MAGIC
# MAGIC A camada Gold consome a Silver e gera os Data Marts batch de pedidos:
# MAGIC
# MAGIC | Tabela Gold | Regra de negocio |
# MAGIC |-------------|------------------|
# MAGIC | `gold_kpi_receita_mom` | Crescimento Mes a Mes (MoM) de receita liquida (`valor_total`) |
# MAGIC | `gold_kpi_ticket_medio_pagamento_anual` | Ticket medio anual por metodo de pagamento |
# MAGIC | `gold_kpi_taxa_cancelamento_mensal` | Taxa de cancelamento mensal (%) |
# MAGIC | `gold_kpi_volume_dia_semana` | Volume historico de pedidos por dia da semana |
# MAGIC | `gold_kpi_receita_media_estado_entrega` | Receita media por pedido agrupada por estado de entrega |

# COMMAND ----------

# ============================================================
# 4. CAMADA GOLD (PEDIDOS - REGRAS DE NEGOCIO)
# ============================================================

print("Iniciando processamento da camada Gold (KPIs Batch de Pedidos)...")

timestamp_gold = date_format(current_timestamp(), "yyyy-MM-dd HH:mm:ss")

df_silver_gold = spark.read.format("parquet").options(**adls_options).load(path_silver)

df_gold_base = (
    df_silver_gold
    .select(
        "id_pedido",
        "id_cliente",
        "id_endereco_entrega",
        "dt_pedido",
        "status_pedido",
        "valor_total",
        "valor_frete",
        "metodo_pagamento",
        "ano_particao",
        "mes_particao"
    )
    .withColumn("status_pedido", upper(trim(col("status_pedido"))))
    .withColumn("metodo_pagamento", coalesce(col("metodo_pagamento"), lit("NAO_INFORMADO")))
    .withColumn("receita_liquida", coalesce(col("valor_total"), lit(0).cast("decimal(10,2)")))
)


def sql_literal(valor):
    if valor is None:
        return "NULL"

    if isinstance(valor, str):
        return "'" + valor.replace("'", "''") + "'"

    return str(valor)


def gravar_data_mart_delta(tabela_nome, df_mart, partition_cols=None):
    partition_cols = partition_cols or []
    path_gold = f"abfss://{squad_container}@{storage_account}.dfs.core.windows.net/gold/{tabela_nome}"

    print(f"Gravando Data Mart {tabela_nome} em: {path_gold}")

    # Delete + append por particao para idempotencia (mesmo padrao do pipeline de itens).
    # Na primeira carga a tabela ainda nao existe; o try/except segue direto para o append.
    try:
        (
            spark.read
            .format("delta")
            .options(**adls_options)
            .load(path_gold)
            .createOrReplaceTempView(f"tmp_{tabela_nome}")
        )

        delete_cols = partition_cols + ["dia_exec"]
        particoes_reprocessadas = df_mart.select(*delete_cols).distinct().collect()

        for particao in particoes_reprocessadas:
            condicoes = [
                f"{coluna} = {sql_literal(particao[coluna])}"
                for coluna in delete_cols
            ]

            spark.sql(f"""
                DELETE FROM delta.`{path_gold}`
                WHERE {" AND ".join(condicoes)}
            """)

        print(f"Delete executado: {tabela_nome} ({len(particoes_reprocessadas)} particao(oes)).")
    except Exception:
        print(f"Primeira carga detectada - sem delete: {tabela_nome}")

    writer = (
        df_mart.write
        .format("delta")
        .mode("append")
        .option("mergeSchema", "true")
        .options(**adls_options)
    )

    if partition_cols:
        writer = writer.partitionBy(*partition_cols)

    writer.save(path_gold)

    print(f"SUCESSO! Data Mart atualizado: {tabela_nome}")

    return path_gold


def ler_dim_enderecos():
    for formato in ["delta", "parquet"]:
        try:
            df_enderecos = (
                spark.read
                .format(formato)
                .options(**adls_options)
                .load(path_silver_enderecos)
            )
            print(f"Dimensao de enderecos lida em formato {formato}: {path_silver_enderecos}")
            return df_enderecos
        except Exception:
            pass

    print(
        "Dimensao silver/ecommerce_enderecos nao encontrada. "
        "O KPI por estado sera gerado como SEM_ESTADO_INFORMADO."
    )
    return None


def preparar_dim_enderecos():
    df_enderecos = ler_dim_enderecos()

    if df_enderecos is None:
        return None

    colunas = df_enderecos.columns

    if "id_endereco_entrega" in colunas:
        coluna_id = "id_endereco_entrega"
    elif "id_endereco" in colunas:
        coluna_id = "id_endereco"
    else:
        print("Dimensao de enderecos sem coluna de identificador esperada.")
        return None

    if "estado" in colunas:
        coluna_estado = "estado"
    elif "uf" in colunas:
        coluna_estado = "uf"
    else:
        print("Dimensao de enderecos sem coluna `estado` ou `uf`.")
        return None

    return (
        df_enderecos
        .select(
            col(coluna_id).cast("string").alias("id_endereco_entrega"),
            upper(trim(col(coluna_estado).cast("string"))).alias("estado_entrega")
        )
        .dropDuplicates(["id_endereco_entrega"])
    )


# ------------------------------------------------------------
# KPI 1: Crescimento Mes a Mes (MoM) de receita liquida
# Formula: (receita_mes_atual - receita_mes_anterior) / receita_mes_anterior * 100
# ------------------------------------------------------------
window_mom = Window.orderBy("ano_particao", "mes_particao")

df_kpi_receita_mom = (
    df_gold_base
    .groupBy("ano_particao", "mes_particao")
    .agg(
        round(_sum("receita_liquida"), 2).alias("receita_mes_atual")
    )
    .withColumn("receita_mes_anterior", lag("receita_mes_atual").over(window_mom))
    .withColumn(
        "crescimento_mom_perc",
        when(
            col("receita_mes_anterior").isNull() | (col("receita_mes_anterior") == 0),
            lit(None).cast("decimal(10,2)")
        ).otherwise(
            round(
                ((col("receita_mes_atual") - col("receita_mes_anterior")) / col("receita_mes_anterior")) * 100,
                2
            )
        )
    )
    .withColumn("dia_exec", lit(DIA_EXEC))
    .withColumn("gold_processed_at", timestamp_gold)
)

# ------------------------------------------------------------
# KPI 2: Ticket medio anual por metodo de pagamento
# ------------------------------------------------------------
df_kpi_ticket_pagamento_anual = (
    df_gold_base
    .groupBy("ano_particao", "metodo_pagamento")
    .agg(
        count("id_pedido").alias("qtd_pedidos"),
        round(_sum("receita_liquida"), 2).alias("receita_total"),
        round(avg("receita_liquida"), 2).alias("ticket_medio_anual")
    )
    .withColumn("dia_exec", lit(DIA_EXEC))
    .withColumn("gold_processed_at", timestamp_gold)
)

# ------------------------------------------------------------
# KPI 3: Taxa de cancelamento mensal (%)
# ------------------------------------------------------------
df_kpi_taxa_cancelamento_mensal = (
    df_gold_base
    .groupBy("ano_particao", "mes_particao")
    .agg(
        count("id_pedido").alias("qtd_pedidos"),
        _sum(when(col("status_pedido").contains("CANCEL"), 1).otherwise(0)).alias("qtd_pedidos_cancelados")
    )
    .withColumn(
        "taxa_cancelamento_perc",
        round((col("qtd_pedidos_cancelados") / col("qtd_pedidos")) * 100, 2)
    )
    .withColumn("dia_exec", lit(DIA_EXEC))
    .withColumn("gold_processed_at", timestamp_gold)
)

# ------------------------------------------------------------
# KPI 4: Volume historico de pedidos por dia da semana (Sun-Sat)
# ------------------------------------------------------------
df_kpi_volume_dia_semana = (
    df_gold_base
    .withColumn("dia_semana_num", dayofweek(col("dt_pedido")))
    .withColumn(
        "dia_semana",
        when(col("dia_semana_num") == 1, lit("Sun"))
        .when(col("dia_semana_num") == 2, lit("Mon"))
        .when(col("dia_semana_num") == 3, lit("Tue"))
        .when(col("dia_semana_num") == 4, lit("Wed"))
        .when(col("dia_semana_num") == 5, lit("Thu"))
        .when(col("dia_semana_num") == 6, lit("Fri"))
        .otherwise(lit("Sat"))
    )
    .groupBy("dia_semana_num", "dia_semana")
    .agg(
        count("id_pedido").alias("qtd_pedidos")
    )
    .withColumn("dia_exec", lit(DIA_EXEC))
    .withColumn("gold_processed_at", timestamp_gold)
)

# ------------------------------------------------------------
# KPI 5: Receita media por pedido agrupada por estado de entrega
# ------------------------------------------------------------
df_dim_enderecos = preparar_dim_enderecos()

if df_dim_enderecos is not None:
    df_base_estado = (
        df_gold_base
        .join(df_dim_enderecos, on="id_endereco_entrega", how="left")
        .withColumn("estado_entrega", coalesce(col("estado_entrega"), lit("SEM_ESTADO_INFORMADO")))
    )
else:
    df_base_estado = df_gold_base.withColumn("estado_entrega", lit("SEM_ESTADO_INFORMADO"))

df_kpi_receita_media_estado = (
    df_base_estado
    .groupBy("ano_particao", "mes_particao", "estado_entrega")
    .agg(
        count("id_pedido").alias("qtd_pedidos"),
        round(_sum("receita_liquida"), 2).alias("receita_total"),
        round(avg("receita_liquida"), 2).alias("receita_media_pedido")
    )
    .withColumn("dia_exec", lit(DIA_EXEC))
    .withColumn("gold_processed_at", timestamp_gold)
)

data_marts_gold = {
    "gold_kpi_receita_mom": {
        "df": df_kpi_receita_mom,
        "partition_cols": ["ano_particao", "mes_particao"]
    },
    "gold_kpi_ticket_medio_pagamento_anual": {
        "df": df_kpi_ticket_pagamento_anual,
        "partition_cols": ["ano_particao"]
    },
    "gold_kpi_taxa_cancelamento_mensal": {
        "df": df_kpi_taxa_cancelamento_mensal,
        "partition_cols": ["ano_particao", "mes_particao"]
    },
    "gold_kpi_volume_dia_semana": {
        "df": df_kpi_volume_dia_semana,
        "partition_cols": []
    },
    "gold_kpi_receita_media_estado_entrega": {
        "df": df_kpi_receita_media_estado,
        "partition_cols": ["ano_particao", "mes_particao"]
    }
}

for tabela_nome, config_mart in data_marts_gold.items():
    gravar_data_mart_delta(
        tabela_nome=tabela_nome,
        df_mart=config_mart["df"],
        partition_cols=config_mart["partition_cols"]
    )

print("SUCESSO! Todos os KPIs batch de pedidos foram gravados na Gold.\n")
display(df_kpi_receita_mom.limit(5))
display(df_kpi_ticket_pagamento_anual.limit(5))
display(df_kpi_taxa_cancelamento_mensal.limit(5))
display(df_kpi_volume_dia_semana.limit(7))
display(df_kpi_receita_media_estado.limit(5))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Exportacao para SQL Server
# MAGIC
# MAGIC Exporta os KPIs para o SQL Server no schema `squad3`, em modo `append`.

# COMMAND ----------

# ============================================================
# 5. EXPORTACAO PARA O SQL SERVER (SERVING LAYER)
# ============================================================

print("Iniciando a exportacao do KPI para o SQL Server (MODO APPEND)...")

load_dotenv("../env")
load_dotenv("../.env")
load_dotenv(".env")

jdbc_hostname = os.getenv("SQL_HOST")
jdbc_port = "1433"
jdbc_database = os.getenv("SQL_DATABASE")
jdbc_username = os.getenv("SQL_USERNAME")
jdbc_password = os.getenv("SQL_PASSWORD")

for tabela_nome, config_mart in data_marts_gold.items():
    tabela_sql_server = f"squad3.{tabela_nome}"

    try:
        (
            config_mart["df"].write
            .format("sqlserver")
            .option("host", jdbc_hostname)
            .option("port", jdbc_port)
            .option("database", jdbc_database)
            .option("dbtable", tabela_sql_server)
            .option("user", jdbc_username)
            .option("password", jdbc_password)
            .mode("append")
            .save()
        )
        print(f"SUCESSO! Dados exportados para {tabela_sql_server} no SQL Server.")
    except Exception as e:
        print(f"Erro ao exportar para {tabela_sql_server}:\n{e}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Data Quality - Sanidade Analitica (Pedidos)
# MAGIC
# MAGIC Analise grafica e textual da qualidade dos dados em cada etapa de transformacao, monitorando problemas comuns em pedidos.

# COMMAND ----------

# ============================================================
# 6. DATA QUALITY - MONITORAMENTO DE PEDIDOS
# ============================================================
from pyspark.sql.functions import col as col_func
from builtins import round as python_round
import matplotlib.pyplot as plt
import pandas as pd

print("Gerando insights de qualidade da Silver (Pedidos)...\n")

df_bronze_check = spark.read.format("delta").options(**adls_options).load(path_bronze)
df_silver_check = spark.read.format("parquet").options(**adls_options).load(path_silver)
total_registros_bronze = df_bronze_check.count()
total_registros_silver = df_silver_check.count()

# REGRA 1: id_pedido nulo na Bronze
regra_1_erros = df_bronze_check.filter(col_func("id_pedido").isNull()).count()

# REGRA 2: id_pedido duplicado na Bronze
regra_2_erros = total_registros_bronze - df_bronze_check.dropDuplicates(["id_pedido"]).count()

# REGRA 3: dt_pedido invalido na Bronze
df_data_check = df_bronze_check.withColumn("dt_pedido_parseado", coalesce(
    to_timestamp(col_func("dt_pedido"), "yyyy-MM-dd HH:mm:ss"),
    to_timestamp(col_func("dt_pedido"), "yyyy-MM-dd"),
    to_timestamp(col_func("dt_pedido"), "dd/MM/yyyy HH:mm:ss"),
    to_timestamp(col_func("dt_pedido"), "dd/MM/yyyy")
))
regra_3_erros = df_data_check.filter(col_func("dt_pedido_parseado").isNull()).count()

# REGRA 4: valores financeiros invalidos na Bronze
df_valores_check = (
    df_bronze_check
    .withColumn("valor_total_cast", regexp_replace(col_func("valor_total"), ",", ".").cast("decimal(10,2)"))
    .withColumn("valor_frete_cast", regexp_replace(col_func("valor_frete"), ",", ".").cast("decimal(10,2)"))
)
regra_4_erros = df_valores_check.filter(
    col_func("valor_total_cast").isNull()
    | col_func("valor_frete_cast").isNull()
).count()

# REGRA 5: campos de classificacao vazios na Bronze
regra_5_erros = df_bronze_check.filter(
    col_func("status_pedido").isNull()
    | (trim(col_func("status_pedido")) == "")
    | col_func("metodo_pagamento").isNull()
    | (trim(col_func("metodo_pagamento")) == "")
).count()

dados_qualidade = {
    "Regra": [
        "R1: id_pedido\nNULO (Bronze)",
        "R2: id_pedido\nDuplicado (Bronze)",
        "R3: dt_pedido\nInvalida (Bronze)",
        "R4: Valores\nInvalidos (Bronze)",
        "R5: Classificacao\nVazia (Bronze)"
    ],
    "Erros Encontrados": [
        regra_1_erros,
        regra_2_erros,
        regra_3_erros,
        regra_4_erros,
        regra_5_erros
    ],
    "Taxa (%)": [
        python_round(100 * regra_1_erros / total_registros_bronze, 2) if total_registros_bronze > 0 else 0,
        python_round(100 * regra_2_erros / total_registros_bronze, 2) if total_registros_bronze > 0 else 0,
        python_round(100 * regra_3_erros / total_registros_bronze, 2) if total_registros_bronze > 0 else 0,
        python_round(100 * regra_4_erros / total_registros_bronze, 2) if total_registros_bronze > 0 else 0,
        python_round(100 * regra_5_erros / total_registros_bronze, 2) if total_registros_bronze > 0 else 0,
    ]
}

df_qualidade = pd.DataFrame(dados_qualidade)

print(f"\n{'='*70}")
print("ANALISE DE QUALIDADE - REGRAS SILVER (PEDIDOS)")
print(f"{'='*70}")
print(f"Total de registros na Bronze: {total_registros_bronze:,}")
print(f"Total de registros na Silver: {total_registros_silver:,}")
print(f"Registros rejeitados/removidos: {total_registros_bronze - total_registros_silver:,}")
print(f"Taxa de rejeicao geral: {python_round(100 * (total_registros_bronze - total_registros_silver) / total_registros_bronze, 2) if total_registros_bronze > 0 else 0}%\n")
print(df_qualidade.to_string(index=False))
print(f"{'='*70}\n")

fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(14, 5))

colors_gradient = ["#ff6b6b", "#ff8c8c", "#ffa9a9", "#ffc6c6", "#ffe0e0"]
bars1 = ax1.bar(range(len(df_qualidade)), df_qualidade["Erros Encontrados"], color=colors_gradient, edgecolor="#333", linewidth=1.5)
ax1.set_ylabel("Quantidade de Registros com Erro", fontsize=11, fontweight="bold")
ax1.set_title("Erros Detectados por Regra de Validacao", fontsize=12, fontweight="bold")
ax1.set_xticks(range(len(df_qualidade)))
ax1.set_xticklabels(df_qualidade["Regra"], fontsize=9)
ax1.grid(axis="y", alpha=0.3, linestyle="--")

for bar in bars1:
    height = bar.get_height()
    if height > 0:
        ax1.text(bar.get_x() + bar.get_width()/2., height,
                f"{int(height):,}",
                ha="center", va="bottom", fontsize=9, fontweight="bold")

colors_taxa = ["#e74c3c" if x > 5 else "#f39c12" if x > 1 else "#27ae60" for x in df_qualidade["Taxa (%)"]]
bars2 = ax2.bar(range(len(df_qualidade)), df_qualidade["Taxa (%)"], color=colors_taxa, edgecolor="#333", linewidth=1.5)
ax2.set_ylabel("Taxa de Erro (%)", fontsize=11, fontweight="bold")
ax2.set_title("Taxa de Erro por Regra (% do Total)", fontsize=12, fontweight="bold")
ax2.set_xticks(range(len(df_qualidade)))
ax2.set_xticklabels(df_qualidade["Regra"], fontsize=9)
ax2.grid(axis="y", alpha=0.3, linestyle="--")
ax2.axhline(y=1, color="#f39c12", linestyle="--", linewidth=1, alpha=0.5, label="Limite Aceitavel (1%)")

for bar in bars2:
    height = bar.get_height()
    if height > 0:
        ax2.text(bar.get_x() + bar.get_width()/2., height,
                f"{height:.2f}%",
                ha="center", va="bottom", fontsize=9, fontweight="bold")

plt.tight_layout()
plt.show()

print("Insights de qualidade gerados com sucesso.\n")

# COMMAND ----------

# # ============================================================
# # TRUNCATE (executar UMA VEZ para limpar duplicatas acumuladas)
# # ============================================================

# path_bronze_trunc = f"abfss://{squad_container}@{storage_account}.dfs.core.windows.net/bronze/{tabela}"
# path_silver_trunc = f"abfss://{squad_container}@{storage_account}.dfs.core.windows.net/silver/{tabela}"
# gold_tables_trunc = [
#     "gold_kpi_receita_mom",
#     "gold_kpi_ticket_medio_pagamento_anual",
#     "gold_kpi_taxa_cancelamento_mensal",
#     "gold_kpi_volume_dia_semana",
#     "gold_kpi_receita_media_estado_entrega"
# ]

# for path, formato in [
#     (path_bronze_trunc, "delta"),
#     (path_silver_trunc, "parquet")
# ]:
#     try:
#         df_vazio = spark.read.format(formato).options(**adls_options).load(path).limit(0)
#         df_vazio.write.format(formato).mode("overwrite").options(**adls_options).save(path)
#         print(f"Truncado: {path.split('/')[-1]}")
#     except Exception:
#         print(f"Tabela nao encontrada (skip): {path.split('/')[-1]}")

# for tabela_gold_trunc in gold_tables_trunc:
#     path_gold_trunc = f"abfss://{squad_container}@{storage_account}.dfs.core.windows.net/gold/{tabela_gold_trunc}"
#     try:
#         df_vazio = spark.read.format("delta").options(**adls_options).load(path_gold_trunc).limit(0)
#         df_vazio.write.format("delta").mode("overwrite").options(**adls_options).save(path_gold_trunc)
#         print(f"Truncado: {tabela_gold_trunc}")
#     except Exception:
#         print(f"Tabela nao encontrada (skip): {tabela_gold_trunc}")

# print("\nPronto! Re-execute o pipeline do inicio.")
