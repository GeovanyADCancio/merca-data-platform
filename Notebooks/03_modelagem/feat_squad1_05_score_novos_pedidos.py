# Databricks notebook source
# MAGIC %md
# MAGIC # Score em Dados Novos — Isolation Forest (Squad 1)
# MAGIC **Card:** Treinamento e execução do Modelo
# MAGIC
# MAGIC Segunda metade do card — a primeira treinou o modelo; esta "acopla" o modelo já treinado para
# MAGIC avaliar pedidos novos, sem retreinar. Corresponde à fase de *Deployment* do CRISP-DM, na sua
# MAGIC forma mais simples: nenhum agendamento automático ainda, apenas a capacidade de rodar sob demanda
# MAGIC sobre o que houver de novo na Silver.
# MAGIC
# MAGIC **Lógica de "o que é novo":** este notebook não recebe um lote específico de pedidos — ele lê
# MAGIC toda a Silver de `pedidos` (necessário para as Window Functions de histórico funcionarem
# MAGIC corretamente, ver aviso na função `construir_features_pedidos`), calcula as features de todo
# MAGIC mundo, e então filtra apenas os `id_pedido` que **ainda não têm score** em
# MAGIC `squad1/gold/anomalias_pedidos`. Isso permite rodar este notebook repetidamente (ex: a cada nova
# MAGIC leva de pedidos) sem duplicar nem reprocessar o que já foi avaliado.

# COMMAND ----------

# MAGIC %pip install scikit-learn

# COMMAND ----------

# MAGIC %run ../99_utils/feat_squad1_99_helpers

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Features sobre o histórico completo
# MAGIC
# MAGIC Mesma função usada no notebook de Engenharia de Features (`construir_features_pedidos`) — nenhuma
# MAGIC lógica de cálculo é reescrita aqui, apenas reutilizada.

# COMMAND ----------

df_pedidos = ler_delta("silver", "ecommerce_pedidos")
df_itens = ler_delta("silver", "ecommerce_itens_pedido")
df_produtos = ler_delta("silver", "ecommerce_produtos")

df_features_completo = construir_features_pedidos(df_pedidos, df_itens, df_produtos)
print(f"Features calculadas sobre o histórico completo: {df_features_completo.count()} pedidos.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Identificar o que ainda não foi pontuado
# MAGIC
# MAGIC Lê os `id_pedido` já presentes em `squad1/gold/anomalias_pedidos` e filtra o que sobrou. Se a
# MAGIC tabela ainda não existir (cenário improvável, já que o notebook de treino a cria), o notebook
# MAGIC trata como "nenhum pedido pontuado ainda" em vez de falhar.

# COMMAND ----------

from pyspark.sql import functions as F

try:
    df_ja_pontuados = ler_delta("gold", "anomalias_pedidos")
    ids_ja_pontuados = {row["id_pedido"] for row in df_ja_pontuados.select("id_pedido").distinct().collect()}
except Exception:
    ids_ja_pontuados = set()

print(f"Pedidos já pontuados anteriormente: {len(ids_ja_pontuados)}")

df_features_novos = (
    df_features_completo.filter(~F.col("id_pedido").isin(ids_ja_pontuados))
    if ids_ja_pontuados
    else df_features_completo
)

qtd_novos = df_features_novos.count()
print(f"Pedidos novos a pontuar: {qtd_novos}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Encerramento antecipado se não houver pedido novo
# MAGIC
# MAGIC Cenário esperado ao rodar este notebook logo após o treino, sobre a mesma base — todos os
# MAGIC pedidos já foram pontuados ali, então não deveria sobrar nada aqui. Isso serve como teste de
# MAGIC sanidade da própria lógica de filtro antes de qualquer pedido novo existir de verdade.

# COMMAND ----------

if qtd_novos == 0:
    print("✅ Nenhum pedido novo para pontuar — notebook encerrado sem ação.")
    dbutils.notebook.exit("sem_pedidos_novos")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Preparação — mesmo tratamento de nulos e mesmas colunas do treino

# COMMAND ----------

pdf_novos = df_features_novos.toPandas()
pdf_novos = pdf_novos.fillna(VALORES_NULOS_PADRAO_FEATURES)

X_novos = pdf_novos[COLUNAS_MODELO_ANOMALIA]
print(f"Matriz de score: {X_novos.shape[0]} pedidos × {X_novos.shape[1]} features")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Carregar o modelo já treinado e pontuar
# MAGIC
# MAGIC Nenhum `fit()` acontece aqui — só `score_samples`/`predict` sobre um modelo já ajustado. A
# MAGIC contaminação usada é lida do próprio objeto do modelo (`modelo.contamination`), não redigitada,
# MAGIC para nunca divergir do que foi realmente usado no treino.

# COMMAND ----------

modelo = carregar_modelo("modelos/isolation_forest_pedidos.pkl")

pdf_novos["score_anomalia"] = -modelo.score_samples(X_novos)
pdf_novos["flag_anomalia"] = (modelo.predict(X_novos) == -1)

qtd_anomalias = pdf_novos["flag_anomalia"].sum()
print(f"Pedidos novos sinalizados como anômalos: {qtd_anomalias} de {len(pdf_novos)}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Gravação incremental
# MAGIC
# MAGIC Modo `append`, diferente do notebook de treino (`overwrite`) — aqui o objetivo é **acrescentar**
# MAGIC os pedidos novos ao que já existe em `squad1/gold/anomalias_pedidos`, não substituir o histórico
# MAGIC de scores já gravados.

# COMMAND ----------

from datetime import datetime, timezone
import pandas as pd

pdf_resultado = pdf_novos[["id_pedido", "id_cliente", "score_anomalia", "flag_anomalia"]].copy()
pdf_resultado["modelo_usado"] = "isolation_forest"
pdf_resultado["contaminacao_assumida"] = modelo.contamination
pdf_resultado["data_execucao"] = pd.Timestamp(datetime.now(timezone.utc))

df_resultado = spark.createDataFrame(pdf_resultado)
sucesso = gravar_delta(df_resultado, "gold", "anomalias_pedidos", mode="append")

if sucesso:
    print(f"🚀 {qtd_novos} pedido(s) novo(s) adicionado(s) a squad1/gold/anomalias_pedidos.")
else:
    print("⚠️ Falha na gravação — checar o log acima.")
