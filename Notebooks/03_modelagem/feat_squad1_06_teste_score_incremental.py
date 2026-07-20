# Databricks notebook source
# MAGIC %md
# MAGIC # Teste — Validação da Gravação Incremental do Score
# MAGIC
# MAGIC Este notebook não faz parte do pipeline oficial — registra o teste que validou a Seção 6
# MAGIC (`mode="append"`) do `feat_squad1_05_score_novos_pedidos`. Motivação: rodar o notebook de score
# MAGIC logo após o treino sempre resulta em "0 pedidos novos" (Seção 3 encerra antes de qualquer coisa
# MAGIC acontecer), então o caminho de gravação incremental nunca era realmente exercitado. Este teste
# MAGIC simula um cenário de "pedido novo" removendo temporariamente 3 pedidos do resultado já salvo,
# MAGIC forçando o notebook de score a de fato pontuá-los e regravá-los.
# MAGIC
# MAGIC **Resultado do teste (executado em 20/07):** os 3 pedidos removidos foram corretamente
# MAGIC detectados como novos, pontuados e gravados via `append` — a tabela voltou a ter exatamente 50
# MAGIC linhas e 50 `id_pedido` distintos, sem duplicidade.

# COMMAND ----------

# MAGIC %run ../99_utils/feat_squad1_99_helpers

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Simulação — remover 3 pedidos do resultado já salvo

# COMMAND ----------

df_atual = ler_delta("gold", "anomalias_pedidos")
total_antes = df_atual.count()

ids_removidos = [row["id_pedido"] for row in df_atual.limit(3).select("id_pedido").collect()]
df_reduzido = df_atual.filter(~df_atual.id_pedido.isin(ids_removidos))

gravar_delta(df_reduzido, "gold", "anomalias_pedidos", mode="overwrite")

print(f"Total antes  : {total_antes}")
print(f"Total depois : {df_reduzido.count()}")
print(f"IDs removidos (simulando 'pedidos novos'): {ids_removidos}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Neste ponto, `feat_squad1_05_score_novos_pedidos` foi rodado manualmente
# MAGIC
# MAGIC Resultado observado: "Pedidos novos a pontuar: 3", seguido da execução real das Seções 4–6
# MAGIC (antes nunca exercitadas), gravação via `append`, sem erro.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Verificação final — tabela consistente após o ciclo completo

# COMMAND ----------

df_final = ler_delta("gold", "anomalias_pedidos")

total_linhas = df_final.count()
total_ids_distintos = df_final.select("id_pedido").distinct().count()

print(f"Total de linhas      : {total_linhas}")
print(f"id_pedido distintos  : {total_ids_distintos}")

if total_linhas == 50 and total_ids_distintos == 50:
    print("✅ Tabela consistente — voltou aos 50 pedidos, sem duplicidade.")
else:
    print("⚠️ Algo não bate — revisar antes de considerar o card fechado.")

