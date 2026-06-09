# Databricks notebook source
# MAGIC %md
# MAGIC Squad 2 | Camada Bronze
# MAGIC
# MAGIC **Tabela** | ecommerce_categorias |
# MAGIC
# MAGIC **Origem** | vendas_raw/ (parquet) |
# MAGIC
# MAGIC **Destino** | squad2/bronze/ecommerce_categorias (Delta) |
# MAGIC
# MAGIC **Modo** | Delta Streaming — Structured Streaming |
# MAGIC
# MAGIC **Objetivo** | Ingerir dados brutos na camada Bronze |
# MAGIC
# MAGIC **Checkpoint** | squad2/checkpoints/bronze/ecommerce_categorias |
# MAGIC
# MAGIC **Depende de** | feat_squad2_99_helpers |

# COMMAND ----------

# DBTITLE 1,Carrega funções
# MAGIC %run ../utils/feat_squad2_99_helpers

# COMMAND ----------

import logging
from pyspark.sql.functions import lit, current_timestamp

logging.getLogger("azure").setLevel(logging.WARNING)

TABELA      = "ecommerce_categorias"
BRONZE_PATH = get_delta_path("bronze", TABELA)

inicio = log_inicio(f"feat_squad2_bronze_{TABELA}")

log.info(f"Tabela      : {TABELA}")
log.info(f"Bronze Path : {BRONZE_PATH}")

# COMMAND ----------

# DBTITLE 1,Listar e verificar snapshots disponíveis
try:
    snapshots = sorted(listar_snapshots())
    log.info(f"{len(snapshots)} snapshot(s) disponível(is):\n")
    for snap in snapshots:
        print(f"  Pacote {snap}")

except Exception as e:
    log.error(f"Erro ao listar snapshots: {str(e)}")
    raise

# COMMAND ----------

# DBTITLE 1,Verificar checkpoint de snapshots já processados
try:
    processados = ler_checkpoint("bronze", TABELA)
    novos       = [s for s in snapshots if s not in processados]
    log.info(f"{len(novos)} snapshot(s) novo(s) para processar")

except Exception as e:
    log.error(f"Erro ao verificar checkpoint: {str(e)}")
    raise

# COMMAND ----------

# DBTITLE 1,Ingerir Snapshots na Bronze (Delta)
try:
    total_linhas = 0

    if not novos:
        log.info("Nenhum snapshot novo para processar!")
    else:
        for snapshot_id in novos:
            log.info(f"Processando: {snapshot_id}")

            df        = ler_parquet(snapshot_id, TABELA)

            # Colunas de auditoria conforme especificação original
            from pyspark.sql.functions import (
                lit, current_timestamp,
                year, month, dayofmonth, hour
            )

            df_bronze = df \
                .withColumn(
                    "bronze_source_file",
                    lit(f"{PATHS['raw']}/{snapshot_id}/{TABELA}.parquet")
                ) \
                .withColumn("bronze_ingested_at", current_timestamp()) \
                .withColumn("_source",            lit("real-time-data")) \
                .withColumn("_camada",            lit("bronze")) \
                .withColumn("ingestion_year",     year(current_timestamp()).cast("string")) \
                .withColumn("ingestion_month",    month(current_timestamp()).cast("string")) \
                .withColumn("ingestion_day",      dayofmonth(current_timestamp()).cast("string")) \
                .withColumn("ingestion_hour",     hour(current_timestamp()).cast("string"))

            sucesso       = gravar_delta(df_bronze, "bronze", TABELA)
            count         = df_bronze.count()
            total_linhas += count
            processados.add(snapshot_id)

            log.info(f"  OK {snapshot_id} → {count} linhas")

        salvar_checkpoint("bronze", TABELA, processados)
        log.info(f" Total gravado: {total_linhas} linhas")

except Exception as e:
    log.error(f"Erro na ingestão Bronze: {str(e)}")
    raise

# COMMAND ----------

# DBTITLE 1,Validar camada Bronze
try:
    df_bronze = ler_delta("bronze", TABELA)
    total     = df_bronze.count()

    log.info(f"   Validação Bronze OK!")
    log.info(f"   Path            : {BRONZE_PATH}")
    log.info(f"   Total registros : {total}")
    log.info(f"   Colunas         : {len(df_bronze.columns)}")

    print("\n Schema Bronze:")
    df_bronze.printSchema()

    print("\n Amostra:")
    display(df_bronze)

except Exception as e:
    log.error(f"Erro na validação: {str(e)}")
    raise

# COMMAND ----------

# DBTITLE 1,Histórico Delta
try:
    squad2_client = get_squad2_client()
    paths         = list(squad2_client.get_paths(
        path      = f"bronze/{TABELA}",
        recursive = True
    ))

    parquets = [p for p in paths if p.name.endswith(".parquet")]

    log.info(f" Histórico de arquivos gravados: {len(parquets)} arquivo(s)\n")
    for p in sorted(parquets, key=lambda x: x.name):
        tamanho = p.content_length if p.content_length else 0
        print(f"  Arquivo {p.name.split('bronze/')[1]} | {tamanho} bytes")

except Exception as e:
    log.warning(f" Histórico ignorado: {str(e)[:80]}")

# COMMAND ----------

log_fim(f"feat_squad2_bronze_{TABELA}", inicio)
