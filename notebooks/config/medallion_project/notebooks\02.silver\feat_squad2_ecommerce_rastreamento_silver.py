# Databricks notebook source
# MAGIC %md
# MAGIC # Silver - ecommerce_rastreamento
# MAGIC Padroniza tipos, remove duplicidades e aplica regras basicas de qualidade.

# COMMAND ----------

# MAGIC %run ../00.config/feat_squad2_00_setup_config

# COMMAND ----------

from pyspark.sql import Window
from pyspark.sql.functions import col, current_timestamp, desc, lit, lower, row_number, trim

# COMMAND ----------

df_bronze = spark.read.format("delta").load(bronze_path)

df_silver_pre = (
    df_bronze.select(
        col("id_rastreamento").cast("long").alias("id_rastreamento"),
        col("id_pedido_ecommerce").cast("long").alias("id_pedido_ecommerce"),
        trim(col("codigo_rastreio")).alias("codigo_rastreio"),
        col("id_transportadora").cast("long").alias("id_transportadora"),
        trim(col("status_entrega")).alias("status_entrega"),
        col("dt_evento").cast("timestamp").alias("dt_evento"),
        trim(col("observacao")).alias("observacao"),
        col("bronze_source_file"),
        col("bronze_ingested_at"),
    )
    .withColumn("status_entrega_normalizado", lower(trim(col("status_entrega"))))
)

df_quarentena_status = (
    df_silver_pre.filter(~col("status_entrega_normalizado").isin(status_entrega_permitidos))
    .withColumn("motivo_quarentena", lit("status_entrega fora do fluxo logistico permitido"))
)

df_quarentena_data = (
    df_silver_pre.filter(col("dt_evento").isNull() | (col("dt_evento") > current_timestamp()))
    .withColumn("motivo_quarentena", lit("dt_evento nula ou futura"))
)

df_silver_valida = (
    df_silver_pre
    .filter(col("id_rastreamento").isNotNull())
    .filter(col("id_pedido_ecommerce").isNotNull())
    .filter(col("dt_evento").isNotNull())
    .filter(col("dt_evento") <= current_timestamp())
    .filter(col("status_entrega_normalizado").isin(status_entrega_permitidos))
)

if spark.catalog.tableExists(pedidos_silver_table):
    df_pedidos = spark.table(pedidos_silver_table).select("id_pedido_ecommerce").distinct()
    df_quarentena_pedido_orfao = (
        df_silver_valida.join(df_pedidos, on="id_pedido_ecommerce", how="left_anti")
        .withColumn("motivo_quarentena", lit("id_pedido_ecommerce sem correspondencia na Silver de pedidos"))
    )
    df_silver_valida = df_silver_valida.join(df_pedidos, on="id_pedido_ecommerce", how="left_semi")
else:
    raise ValueError(f"Tabela obrigatoria nao encontrada para validar pedidos: {pedidos_silver_table}")

janela_deduplicacao = Window.partitionBy("id_rastreamento").orderBy(
    desc("dt_evento"),
    desc("bronze_ingested_at"),
)

df_silver = (
    df_silver_valida.withColumn("rn", row_number().over(janela_deduplicacao))
    .filter(col("rn") == 1)
    .drop("rn", "status_entrega_normalizado")
    .withColumn("silver_updated_at", current_timestamp())
)

display(df_silver.limit(20))

# COMMAND ----------

df_quarentena = (
    df_quarentena_status
    .unionByName(df_quarentena_data)
    .unionByName(df_quarentena_pedido_orfao)
    .dropDuplicates(["id_rastreamento", "motivo_quarentena"])
)

(
    df_quarentena.write.format("delta")
    .mode("append")
    .option("mergeSchema", "true")
    .save(quarantine_path)
)

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {database_silver}.{business_table_name}_quarentena
USING DELTA
LOCATION '{quarantine_path}'
""")

# COMMAND ----------

(
    df_silver.write.format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .save(silver_path)
)

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {database_silver}.{table_name}
USING DELTA
LOCATION '{silver_path}'
""")

print(f"Silver gravada em: {silver_path}")

checkpoint_silver = carregar_checkpoint("silver", table_name)
checkpoint_silver["origem"] = bronze_path
checkpoint_silver["destino"] = silver_path
checkpoint_silver["registros_processados"] = df_silver.count()
checkpoint_silver["registros_quarentena"] = df_quarentena.count()
checkpoint_silver["regra_deduplicacao"] = "id_rastreamento ordenado por dt_evento e bronze_ingested_at desc"
checkpoint_silver["validacoes"] = [
    "schema completo validado na bronze",
    "status_entrega dentro do fluxo logistico permitido",
    "dt_evento nao nula e nao futura",
    "id_rastreamento deduplicado",
    f"id_pedido_ecommerce validado contra {pedidos_silver_table}",
]
salvar_checkpoint("silver", table_name, checkpoint_silver)

