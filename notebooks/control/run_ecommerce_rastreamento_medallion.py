# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "2"
# ///
# MAGIC %md
# MAGIC # Controle - Pipeline ecommerce_rastreamento
# MAGIC Executa as camadas bronze, silver e gold em ordem.

# COMMAND ----------

etapas = [
    "../01.bronze/feat_squad2_ecommerce_rastreamento_bronze",
    "../02.silver/feat_squad2_ecommerce_rastreamento_silver",
    "../03.gold/feat_squad2_ecommerce_rastreamento_gold",
]

for etapa in etapas:
    print(f"Executando: {etapa}")
    resultado = dbutils.notebook.run(etapa, timeout_seconds=0)
    print(f"Concluido: {etapa} | retorno: {resultado}")

print("Pipeline bronze -> silver -> gold finalizado.")
