# Databricks notebook source
# MAGIC %md
# MAGIC # Bronze - ecommerce_rastreamento
# MAGIC Le arquivos parquet da camada raw e grava em Delta na bronze com metadados de ingestao.

# COMMAND ----------

# MAGIC %run ../00.config/feat_squad2_00_setup_config

# COMMAND ----------

from pyspark.sql.functions import (
    col,
    current_timestamp,
    dayofmonth,
    hour,
    input_file_name,
    month,
    year,
)

# COMMAND ----------

df_arquivos = (
    spark.read.format("binaryFile")
    .option("recursiveFileLookup", "true")
    .load(raw_input_path)
)

df_rastreamento_files = df_arquivos.filter(col("path").contains("ecommerce_rastreamento"))

display(df_rastreamento_files.select("path", "modificationTime", "length"))

arquivos_rastreamento = [row.path for row in df_rastreamento_files.select("path").collect()]
checkpoint_bronze = carregar_checkpoint("bronze", table_name)
arquivos_processados = set(checkpoint_bronze.get("arquivos_processados", []))
arquivos_novos = [arquivo for arquivo in arquivos_rastreamento if arquivo not in arquivos_processados]

print(f"Arquivos encontrados: {len(arquivos_rastreamento)}")
print(f"Arquivos ja processados: {len(arquivos_processados)}")
print(f"Arquivos novos: {len(arquivos_novos)}")

if not arquivos_rastreamento:
    raise ValueError(f"Nenhum arquivo ecommerce_rastreamento encontrado em {raw_input_path}")

if not arquivos_novos:
    dbutils.notebook.exit("Nenhum arquivo novo para processar na bronze.")

# COMMAND ----------

df_bronze = (
    spark.read.format("parquet")
    .option("spark.sql.parquet.nanosAsLong", "true")
    .load(*arquivos_novos)
    .withColumn("bronze_ingested_at", current_timestamp())
    .withColumn("bronze_source_file", input_file_name())
    .withColumn("ano_ingestao", year(col("bronze_ingested_at")))
    .withColumn("mes_ingestao", month(col("bronze_ingested_at")))
    .withColumn("dia_ingestao", dayofmonth(col("bronze_ingested_at")))
    .withColumn("hora_ingestao", hour(col("bronze_ingested_at")))
)

colunas_ausentes = [coluna for coluna in required_tracking_columns if coluna not in df_bronze.columns]
if colunas_ausentes:
    raise ValueError(f"Schema incompleto no micro-lote. Colunas ausentes: {colunas_ausentes}")

display(df_bronze.limit(20))

# COMMAND ----------

(
    df_bronze.write.format("delta")
    .mode("append")
    .partitionBy("ano_ingestao", "mes_ingestao", "dia_ingestao", "hora_ingestao")
    .save(bronze_path)
)

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {database_bronze}.{table_name}
USING DELTA
LOCATION '{bronze_path}'
""")

print(f"Bronze gravada em: {bronze_path}")

checkpoint_bronze["arquivos_processados"] = sorted(arquivos_processados.union(arquivos_novos))
checkpoint_bronze["total_arquivos_processados"] = len(checkpoint_bronze["arquivos_processados"])
checkpoint_bronze["ultimo_lote"] = arquivos_novos
checkpoint_bronze["destino"] = bronze_path
salvar_checkpoint("bronze", table_name, checkpoint_bronze)

