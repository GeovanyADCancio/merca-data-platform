# Databricks notebook source
# MAGIC %md
# MAGIC # Investigação — Itens Órfãos e Log de Monitoramento (Squad 1)
# MAGIC
# MAGIC Este notebook não faz parte da entrega oficial do card de Engenharia de Features — é uma
# MAGIC investigação pontual, disparada por uma desproporção observada durante a validação do pipeline
# MAGIC (1037 linhas em `itens_pedido` para apenas 13 em `pedidos`). Fica isolado aqui, em
# MAGIC `01_diagnostico`, justamente para não misturar exploração com o pipeline de produção que gera
# MAGIC `squad1/gold/features_pedidos`.
# MAGIC
# MAGIC O achado está documentado no resumo entregue ao Lead: ~94% dos registros de `itens_pedido` na
# MAGIC Silver não possuem `id_pedido` correspondente em `pedidos`, e o problema já é monitorado (mas
# MAGIC não filtrado) pela regra `R2_PEDIDO_FK_ORFAO`.

# COMMAND ----------

# MAGIC %run ../99_utils/feat_squad1_99_helpers

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Quantificação dos itens órfãos
# MAGIC
# MAGIC Compara o `id_pedido` de cada linha em `itens_pedido` contra o conjunto de `id_pedido`
# MAGIC realmente existentes em `pedidos`, ambos lidos da camada Silver.

# COMMAND ----------

df_pedidos_check = ler_delta("silver", "ecommerce_pedidos")
df_itens_full = ler_delta("silver", "ecommerce_itens_pedido")

ids_pedidos_validos = {row["id_pedido"] for row in df_pedidos_check.select("id_pedido").distinct().collect()}

total_itens = df_itens_full.count()
itens_com_pedido_valido = df_itens_full.filter(
    df_itens_full.id_pedido.isin(ids_pedidos_validos)
).count()
itens_orfaos = total_itens - itens_com_pedido_valido

print(f"Total de itens na Silver          : {total_itens}")
print(f"Itens com pedido correspondente    : {itens_com_pedido_valido}")
print(f"Itens órfãos (sem pedido na Silver): {itens_orfaos} ({itens_orfaos/total_itens:.1%})")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Duplicidade de `id_pedido` na Silver
# MAGIC
# MAGIC Checagem motivada pelo mesmo problema encontrado na base da Squad 3 (`id_pedido` repetido entre
# MAGIC snapshots, com status divergente entre cópias — ver `feat_ml_diagnostico2_dedup_squad3.py`,
# MAGIC compartilhado pela Squad 2). A arquitetura de ingestão da Squad 1 (Bronze append-only por
# MAGIC snapshot) é suscetível ao mesmo padrão, então vale monitorar mesmo sem evidência de problema
# MAGIC até aqui.

# COMMAND ----------

total_linhas = df_pedidos_check.count()
total_ids_distintos = df_pedidos_check.select("id_pedido").distinct().count()
duplicados = total_linhas - total_ids_distintos

print(f"Total de linhas     : {total_linhas}")
print(f"id_pedido distintos : {total_ids_distintos}")
print(f"Duplicados          : {duplicados}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Inspeção da pasta `dq_monitoring_logs`
# MAGIC
# MAGIC Etapa exploratória inicial: antes de tentar ler a tabela como Delta, confirma que a pasta
# MAGIC contém de fato um `_delta_log` com versões e arquivos de dados (ou seja, não está vazia).

# COMMAND ----------

fs_client = get_squad1_client()

print("📂 Conteúdo de 'dq_monitoring_logs':\n")
for path in fs_client.get_paths(path="dq_monitoring_logs", recursive=True):
    tipo = "📁 pasta" if path.is_directory else "📄 arquivo"
    print(f" - {tipo}: {path.name}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Leitura do conteúdo de `dq_monitoring_logs`
# MAGIC
# MAGIC Como essa tabela é um Delta "solto" na raiz do container (não segue o padrão `camada/tabela`
# MAGIC do resto do Data Lake), o caminho é montado manualmente em vez de usar `get_delta_path`.

# COMMAND ----------

from deltalake import DeltaTable

path_dq_logs = f"abfss://squad1@{ADLS_STORAGE_ACCOUNT}.dfs.core.windows.net/dq_monitoring_logs"

dt_dq_logs = DeltaTable(path_dq_logs, storage_options=get_storage_options())
df_dq_logs = spark.createDataFrame(dt_dq_logs.to_pandas())

print(f"Total de linhas: {df_dq_logs.count()}\n")
print("📋 Schema:")
df_dq_logs.printSchema()

print("\n👀 Amostra:")
display(df_dq_logs.limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Conclusão
# MAGIC
# MAGIC A regra `R2_PEDIDO_FK_ORFAO`, visível na amostra acima, confirma de forma independente o número
# MAGIC calculado na Seção 1 — a squad já monitora esse problema, mas o resultado da checagem não é
# MAGIC usado como filtro na gravação da Silver. Reportado ao Lead separadamente do card de Engenharia
# MAGIC de Features, já que se trata de uma lacuna no pipeline de dados, não na camada de features.
