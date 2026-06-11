# Databricks notebook source
# MAGIC %md
# MAGIC # Gold - ecommerce_rastreamento
# MAGIC Cria tabelas agregadas para acompanhamento logistico dos pedidos do e-commerce.

# COMMAND ----------

# MAGIC %run ../00.config/feat_squad2_00_setup_config

# COMMAND ----------

from pyspark.sql import Window
from pyspark.sql.functions import col, count, countDistinct, current_timestamp, desc, lit, max as spark_max, row_number, to_date

# COMMAND ----------

df_silver = spark.read.format("delta").load(silver_path)
df_pedidos = spark.table(pedidos_silver_table)

# COMMAND ----------

df_gold_status_diario = (
    df_silver.withColumn("data_evento", to_date("dt_evento"))
    .groupBy("data_evento", "status_entrega")
    .agg(
        count("*").alias("qtd_eventos"),
        countDistinct("id_pedido_ecommerce").alias("qtd_pedidos"),
        countDistinct("codigo_rastreio").alias("qtd_codigos_rastreio"),
    )
    .withColumn("gold_updated_at", current_timestamp())
)

status_diario_path = gold_path + "/status_diario"
(
    df_gold_status_diario.write.format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .save(status_diario_path)
)

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {database_gold}.{table_name}_status_diario
USING DELTA
LOCATION '{status_diario_path}'
""")

display(df_gold_status_diario)

# COMMAND ----------

janela_ultimo_evento = Window.partitionBy("id_pedido_ecommerce").orderBy(desc("dt_evento"))

df_ultimo_evento = (
    df_silver.withColumn("rn", row_number().over(janela_ultimo_evento))
    .filter("rn = 1")
    .select(
        "id_pedido_ecommerce",
        col("dt_evento").alias("ultima_data_evento"),
        col("status_entrega").alias("ultimo_status_entrega"),
        col("codigo_rastreio").alias("ultimo_codigo_rastreio"),
    )
)

df_eventos_por_pedido = (
    df_silver.groupBy("id_pedido_ecommerce")
    .agg(count("*").alias("qtd_eventos_rastreamento"))
)

df_gold_pedido_ultima_posicao = (
    df_ultimo_evento.join(df_eventos_por_pedido, on="id_pedido_ecommerce", how="left")
    .withColumn("gold_updated_at", current_timestamp())
)

pedido_ultima_posicao_path = gold_path + "/pedido_ultima_posicao"
(
    df_gold_pedido_ultima_posicao.write.format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .save(pedido_ultima_posicao_path)
)

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {database_gold}.{table_name}_pedido_ultima_posicao
USING DELTA
LOCATION '{pedido_ultima_posicao_path}'
""")

display(df_gold_pedido_ultima_posicao)

print(f"Gold gravada em: {gold_path}")

# COMMAND ----------

df_gold_saiu_entrega_2h = (
    df_silver.filter("lower(status_entrega) = 'saiu para entrega'")
    .filter("dt_evento >= current_timestamp() - INTERVAL 2 HOURS")
    .agg(countDistinct("id_pedido_ecommerce").alias("qtd_pedidos_saiu_para_entrega_2h"))
    .withColumn("gold_updated_at", current_timestamp())
)

saiu_entrega_2h_path = gold_path + "/saiu_para_entrega_2h"
(
    df_gold_saiu_entrega_2h.write.format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .save(saiu_entrega_2h_path)
)

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {database_gold}.{table_name}_saiu_para_entrega_2h
USING DELTA
LOCATION '{saiu_entrega_2h_path}'
""")

display(df_gold_saiu_entrega_2h)

# COMMAND ----------

df_gold_sla_entrega = spark.sql(f"""
WITH entregues AS (
  SELECT
    r.id_pedido_ecommerce,
    r.dt_evento,
    p.dt_pedido,
    datediff(r.dt_evento, p.dt_pedido) AS dias_ate_entrega
  FROM {database_silver}.{table_name} r
  INNER JOIN {pedidos_silver_table} p
    ON r.id_pedido_ecommerce = p.id_pedido_ecommerce
  WHERE lower(r.status_entrega) = 'entregue'
)
SELECT
  count(DISTINCT id_pedido_ecommerce) AS qtd_pedidos_entregues,
  count(DISTINCT CASE WHEN dias_ate_entrega <= {sla_entrega_dias} THEN id_pedido_ecommerce END) AS qtd_pedidos_no_prazo,
  round(
    100.0 * count(DISTINCT CASE WHEN dias_ate_entrega <= {sla_entrega_dias} THEN id_pedido_ecommerce END)
    / nullif(count(DISTINCT id_pedido_ecommerce), 0),
    2
  ) AS percentual_entregue_no_prazo,
  current_timestamp() AS gold_updated_at
FROM entregues
""")

sla_entrega_path = gold_path + "/sla_entrega"
(
    df_gold_sla_entrega.write.format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .save(sla_entrega_path)
)

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {database_gold}.{table_name}_sla_entrega
USING DELTA
LOCATION '{sla_entrega_path}'
""")

display(df_gold_sla_entrega)

# COMMAND ----------

df_alerta_entrega_duplicada = (
    df_silver.filter("lower(status_entrega) = 'entregue'")
    .groupBy("id_pedido_ecommerce")
    .agg(count("*").alias("qtd_eventos_entregue"))
    .filter(col("qtd_eventos_entregue") > 1)
    .withColumn("gold_updated_at", current_timestamp())
)

alerta_entrega_duplicada_path = gold_path + "/alerta_entrega_duplicada"
(
    df_alerta_entrega_duplicada.write.format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .save(alerta_entrega_duplicada_path)
)

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {database_gold}.{table_name}_alerta_entrega_duplicada
USING DELTA
LOCATION '{alerta_entrega_duplicada_path}'
""")

display(df_alerta_entrega_duplicada)

# COMMAND ----------

ultima_ingestao_bronze = df_silver.agg(spark_max("bronze_ingested_at")).collect()[0][0]
df_micro_lote_atual = df_silver.filter(col("bronze_ingested_at") == lit(ultima_ingestao_bronze))

df_gold_top3_transportadoras_micro_lote = (
    df_micro_lote_atual.groupBy("id_transportadora")
    .agg(count("*").alias("qtd_eventos_micro_lote"))
    .orderBy(desc("qtd_eventos_micro_lote"))
    .limit(3)
    .withColumn("gold_updated_at", current_timestamp())
)

top3_transportadoras_path = gold_path + "/top3_transportadoras_micro_lote"
(
    df_gold_top3_transportadoras_micro_lote.write.format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .save(top3_transportadoras_path)
)

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {database_gold}.{table_name}_top3_transportadoras_micro_lote
USING DELTA
LOCATION '{top3_transportadoras_path}'
""")

display(df_gold_top3_transportadoras_micro_lote)

# COMMAND ----------

df_alerta_sem_evento_apos_coleta = spark.sql(f"""
WITH eventos AS (
  SELECT
    id_pedido_ecommerce,
    max(CASE WHEN lower(status_entrega) = 'coletado' THEN dt_evento END) AS dt_ultima_coleta,
    max(dt_evento) AS dt_ultimo_evento
  FROM {database_silver}.{table_name}
  GROUP BY id_pedido_ecommerce
)
SELECT
  id_pedido_ecommerce,
  dt_ultima_coleta,
  dt_ultimo_evento,
  datediff(current_timestamp(), dt_ultimo_evento) AS dias_sem_evento,
  current_timestamp() AS gold_updated_at
FROM eventos
WHERE dt_ultima_coleta IS NOT NULL
  AND dt_ultimo_evento = dt_ultima_coleta
  AND datediff(current_timestamp(), dt_ultimo_evento) > 3
""")

alerta_sem_evento_path = gold_path + "/alerta_sem_evento_apos_coleta"
(
    df_alerta_sem_evento_apos_coleta.write.format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .save(alerta_sem_evento_path)
)

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {database_gold}.{table_name}_alerta_sem_evento_apos_coleta
USING DELTA
LOCATION '{alerta_sem_evento_path}'
""")

display(df_alerta_sem_evento_apos_coleta)

checkpoint_gold_status = carregar_checkpoint("gold", f"{table_name}_status_diario")
checkpoint_gold_status["origem"] = silver_path
checkpoint_gold_status["destino"] = status_diario_path
checkpoint_gold_status["registros_processados"] = df_gold_status_diario.count()
checkpoint_gold_status["granularidade"] = "data_evento, status_entrega"
salvar_checkpoint("gold", f"{table_name}_status_diario", checkpoint_gold_status)

checkpoint_gold_pedido = carregar_checkpoint("gold", f"{table_name}_pedido_ultima_posicao")
checkpoint_gold_pedido["origem"] = silver_path
checkpoint_gold_pedido["destino"] = pedido_ultima_posicao_path
checkpoint_gold_pedido["registros_processados"] = df_gold_pedido_ultima_posicao.count()
checkpoint_gold_pedido["granularidade"] = "id_pedido_ecommerce"
salvar_checkpoint("gold", f"{table_name}_pedido_ultima_posicao", checkpoint_gold_pedido)

for nome_tabela, destino, df_kpi in [
    (f"{table_name}_saiu_para_entrega_2h", saiu_entrega_2h_path, df_gold_saiu_entrega_2h),
    (f"{table_name}_sla_entrega", sla_entrega_path, df_gold_sla_entrega),
    (f"{table_name}_alerta_entrega_duplicada", alerta_entrega_duplicada_path, df_alerta_entrega_duplicada),
    (f"{table_name}_top3_transportadoras_micro_lote", top3_transportadoras_path, df_gold_top3_transportadoras_micro_lote),
    (f"{table_name}_alerta_sem_evento_apos_coleta", alerta_sem_evento_path, df_alerta_sem_evento_apos_coleta),
]:
    checkpoint_gold = carregar_checkpoint("gold", nome_tabela)
    checkpoint_gold["origem"] = silver_path
    checkpoint_gold["destino"] = destino
    checkpoint_gold["registros_processados"] = df_kpi.count()
    salvar_checkpoint("gold", nome_tabela, checkpoint_gold)

