# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "2"
# ///
# MAGIC %md
# MAGIC # Controle - Pipeline ecommerce_rastreamento
# MAGIC Executa as camadas bronze, silver e gold em ordem.

# COMMAND ----------

dbutils.notebook.run("../01.bronze/feat_squad2_ecommerce_rastreamento_bronze", timeout_seconds=0)
dbutils.notebook.run("../02.silver/feat_squad2_ecommerce_rastreamento_silver", timeout_seconds=0)
dbutils.notebook.run("../03.gold/feat_squad2_ecommerce_rastreamento_gold", timeout_seconds=0)

print("Pipeline bronze -> silver -> gold finalizado.")

