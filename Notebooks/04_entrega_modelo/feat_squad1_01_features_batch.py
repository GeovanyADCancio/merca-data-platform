# Databricks notebook source
# MAGIC %md
# MAGIC # Notebook 1/3 — Feature Engineer (Squad 3 / Batch)
# MAGIC **Entrega do modelo de IA — conforme estrutura solicitada:**
# MAGIC 1. Feature engineer: seleção e tratamento de colunas *(este notebook)*
# MAGIC 2. Modelo: treino + avaliação
# MAGIC 3. Insights: gráficos e análises
# MAGIC
# MAGIC **Por que só Squad 3:** o teste inicial unindo Squad 1 (tempo real) e Squad 3 (batch) mostrou um
# MAGIC viés de escala entre as duas bases (ticket médio ~7x maior na Squad 3), fazendo o modelo aprender
# MAGIC "normal" como "parecido com a Squad 3" — 100% dos pedidos da Squad 1 eram sinalizados como
# MAGIC anômalos só por vir de outra fonte. Por orientação do Geovany, esta entrega usa exclusivamente a
# MAGIC base batch, que não tem esse problema.
# MAGIC
# MAGIC Sem colisão de squad, `id_pedido`/`id_cliente` não precisam de prefixo aqui (diferente do
# MAGIC experimento de união) — mantidos no formato original.

# COMMAND ----------

# MAGIC %run ../99_utils/feat_squad1_99_helpers

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Leitura — Squad 3 via leitor nativo Spark
# MAGIC
# MAGIC `ler_delta_squad3` usa o motor nativo do Spark (não a biblioteca `deltalake`), necessário porque
# MAGIC as tabelas da Squad 3 usam Deletion Vectors.

# COMMAND ----------

df_pedidos_raw = ler_delta_squad3("silver", "ecommerce_pedidos")
df_itens = ler_delta_squad3("silver", "ecommerce_itens_pedido")
df_produtos = ler_delta_squad3("silver", "ecommerce_produtos")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Deduplicação
# MAGIC
# MAGIC Checagem mantida como rede de segurança, mesmo tendo confirmado 0% de duplicidade na última
# MAGIC verificação (o volume da Squad 3 muda com o tempo, então vale reconferir a cada execução).

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.window import Window

total_bruto = df_pedidos_raw.count()
ids_distintos = df_pedidos_raw.select("id_pedido").distinct().count()
duplicados = total_bruto - ids_distintos

print(f"Pedidos Squad 3 (bruto) : {total_bruto}")
print(f"id_pedido distintos     : {ids_distintos}")
print(f"Duplicados              : {duplicados} ({duplicados/total_bruto:.1%})")

w_dedup = Window.partitionBy("id_pedido").orderBy(F.col("dt_ultima_atualizacao_status").desc())
df_pedidos = (
    df_pedidos_raw
    .withColumn("_rn", F.row_number().over(w_dedup))
    .filter(F.col("_rn") == 1)
    .drop("_rn")
)

print(f"Pedidos após dedup      : {df_pedidos.count()}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Seleção e tratamento de colunas
# MAGIC
# MAGIC Reaproveita `construir_features_pedidos` — as mesmas 18 colunas, calculadas do mesmo jeito, já
# MAGIC usadas no restante do projeto. Resumo do que é aplicado em cada grupo (útil para reportar de
# MAGIC forma objetiva, sem precisar mandar o código inteiro):
# MAGIC
# MAGIC | Grupo | Colunas | O que é aplicado |
# MAGIC |---|---|---|
# MAGIC | Tempo do pedido | `hora_do_pedido`, `dia_semana_pedido` | Extraído de `dt_pedido` |
# MAGIC | Histórico do cliente | `ticket_medio_historico_cliente`, `desvio_padrao_historico_cliente`, `qtd_pedidos_historico_cliente`, `dias_desde_ultima_compra`, `qtd_pedidos_ultimos_30_dias`, `desvio_valor_vs_historico`, `desvio_pct_vs_media_cliente` | Window Functions particionadas por cliente, olhando só pedidos anteriores (evita vazamento) |
# MAGIC | Composição do pedido | `qtd_linhas_itens`, `qtd_unidades_total`, `qtd_categorias_distintas`, `ticket_medio_por_unidade`, `valor_itens_calculado`, `diferenca_valor_itens_vs_pedido` | Agregação de `itens_pedido` + `produtos`, cruzado de volta com o pedido |
# MAGIC | Bruto | `valor_total`, `valor_frete`, `razao_frete_valor` | Direto da tabela de pedidos / calculado |

# COMMAND ----------

df_features = construir_features_pedidos(df_pedidos, df_itens, df_produtos)

total_features = df_features.count()
print(f"Pedidos após dedup : {df_pedidos.count()}")
print(f"Linhas de features : {total_features}")

if total_features == df_pedidos.count():
    print("✅ Volume consistente.")
else:
    print("⚠️ Volume divergente — revisar antes de seguir para o Notebook 2.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Gravação

# COMMAND ----------

sucesso = gravar_delta(df_features, "gold", "features_pedidos_squad3", mode="overwrite")

if sucesso:
    print("🚀 squad1/gold/features_pedidos_squad3 gravada com sucesso.")
else:
    print("⚠️ Falha na gravação — checar log acima.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Próximo passo
# MAGIC
# MAGIC Notebook 2/3: treinar o Isolation Forest sobre `squad1/gold/features_pedidos_squad3` e avaliar o
# MAGIC resultado (equivalente não supervisionado de "acurácia" — comparação estatística entre grupos,
# MAGIC já que não existe rótulo de anomalia real para comparar).
