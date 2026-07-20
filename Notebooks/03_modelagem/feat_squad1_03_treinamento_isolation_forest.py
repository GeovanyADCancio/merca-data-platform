# Databricks notebook source
# MAGIC %md
# MAGIC # Treinamento e Execução do Modelo — Isolation Forest (Squad 1)
# MAGIC **Card:** Treinamento e execução do Modelo
# MAGIC
# MAGIC Corresponde à fase de *Modeling* do CRISP-DM, com um primeiro passo de *Evaluation*. Lê a
# MAGIC tabela `squad1/gold/features_pedidos`, treina o Isolation Forest definido na proposta de modelo
# MAGIC da squad, gera o score de anomalia por pedido, e persiste tanto o modelo quanto o resultado.
# MAGIC
# MAGIC **Aviso importante sobre escopo:** com o volume atual (dezenas de pedidos, não milhares), este
# MAGIC treino deve ser tratado como uma prova de conceito de pipeline — valida que a esteira funciona de
# MAGIC ponta a ponta (ler features → treinar → pontuar → salvar), não que o modelo está calibrado para
# MAGIC uso em produção. Os resultados devem ser recalibrados conforme o volume real crescer.

# COMMAND ----------

# MAGIC %pip install scikit-learn

# COMMAND ----------

# MAGIC %run ../99_utils/feat_squad1_99_helpers

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Leitura das features
# MAGIC
# MAGIC Lê a tabela Gold já validada no card anterior. Nenhuma transformação de negócio acontece aqui —
# MAGIC essa etapa já foi resolvida na Engenharia de Features; este notebook consome o resultado pronto.

# COMMAND ----------

df_features = ler_delta("gold", "features_pedidos")
total_pedidos = df_features.count()
print(f"Pedidos carregados de squad1/gold/features_pedidos: {total_pedidos}")

pdf = df_features.toPandas()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Tratamento de nulos
# MAGIC
# MAGIC As colunas de histórico do cliente ficam nulas na primeira compra de cada cliente (não existe
# MAGIC pedido anterior para calcular média/desvio) — isso é esperado, não um erro de dado. A decisão
# MAGIC tomada aqui: preencher com `0`, que representa "sem desvio conhecido" — um valor neutro, não um
# MAGIC valor baixo. Como grande parte da base compartilha esse mesmo `0` (todo cliente passa por uma
# MAGIC primeira compra), o Isolation Forest não trata isso como raridade — é o comportamento correto
# MAGIC para essa situação.
# MAGIC
# MAGIC O dicionário de preenchimento (`VALORES_NULOS_PADRAO_FEATURES`) vive no helper compartilhado —
# MAGIC o notebook de score em dados novos precisa aplicar exatamente o mesmo tratamento.

# COMMAND ----------

nulos_antes = pdf.isnull().sum()
pdf = pdf.fillna(VALORES_NULOS_PADRAO_FEATURES)

print("Nulos preenchidos por coluna:")
for coluna, qtd in nulos_antes.items():
    if qtd > 0:
        print(f"  {coluna:40s} {qtd} nulo(s)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Seleção das colunas do modelo
# MAGIC
# MAGIC Identificadores (`id_pedido`, `id_cliente`) e a data (`dt_pedido`) ficam de fora — servem para
# MAGIC rastrear o resultado depois, mas não são sinal de comportamento para o modelo.
# MAGIC `COLUNAS_MODELO_ANOMALIA` também vive no helper compartilhado, pelo mesmo motivo do item acima.

# COMMAND ----------

X = pdf[COLUNAS_MODELO_ANOMALIA]
print(f"Matriz de treino: {X.shape[0]} pedidos × {X.shape[1]} features")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Treinamento do Isolation Forest
# MAGIC
# MAGIC * `contamination=0.05`: assume que ~5% dos pedidos são anômalos — a mesma faixa (1–5%) definida
# MAGIC   na proposta de modelo original da squad. Com o volume atual, isso equivale a esperar 2–3
# MAGIC   pedidos sinalizados.
# MAGIC * `random_state=42`: o algoritmo constrói árvores com cortes aleatórios — sem fixar essa semente,
# MAGIC   rodar o mesmo notebook duas vezes poderia gerar scores levemente diferentes a cada vez,
# MAGIC   dificultando comparar resultados ou depurar.

# COMMAND ----------

from sklearn.ensemble import IsolationForest

CONTAMINACAO = 0.05

modelo = IsolationForest(
    n_estimators=100,
    contamination=CONTAMINACAO,
    random_state=42,
)
modelo.fit(X)

print("Modelo treinado.")
print(f"Contaminação assumida : {CONTAMINACAO} (~{round(CONTAMINACAO * len(X))} pedido(s) esperado(s) como anômalo)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Geração do score e da flag de anomalia
# MAGIC
# MAGIC `score_samples` devolve um valor onde números mais altos significam "mais normal" — contraintuitivo
# MAGIC para leitura de negócio. Por isso o sinal é invertido aqui: `score_anomalia` mais **alto** passa a
# MAGIC significar mais anômalo, consistente com o que já foi apresentado ao Lead no one-pager.

# COMMAND ----------

pdf["score_anomalia"] = -modelo.score_samples(X)
pdf["flag_anomalia"] = (modelo.predict(X) == -1)

qtd_anomalias = pdf["flag_anomalia"].sum()
print(f"Pedidos sinalizados como anômalos: {qtd_anomalias} de {len(pdf)} ({qtd_anomalias/len(pdf):.1%})")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Inspeção qualitativa
# MAGIC
# MAGIC Mostra os pedidos com maior score de anomalia — validação manual antes de considerar o resultado
# MAGIC confiável. Com o volume atual, uma validação estatística formal (comparação de médias entre
# MAGIC grupo normal e anômalo) não é confiável — o grupo anômalo tem poucos pedidos demais para
# MAGIC qualquer teste de hipótese ter poder estatístico. Essa validação fica marcada como próximo passo
# MAGIC para quando o volume real crescer.

# COMMAND ----------

colunas_exibir = ["id_pedido", "id_cliente", "valor_total", "desvio_valor_vs_historico", "score_anomalia", "flag_anomalia"]
top_anomalos = pdf.sort_values("score_anomalia", ascending=False).head(10)

print("👀 Top 10 pedidos por score de anomalia:")
display(spark.createDataFrame(top_anomalos[colunas_exibir]))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Persistência — modelo e resultado
# MAGIC
# MAGIC O modelo é salvo separadamente do resultado: o modelo (`.pkl`) é o que o notebook de score em
# MAGIC dados novos vai reabrir; a tabela Delta é o que consome quem for analisar oportunidades de
# MAGIC negócio (dashboards, o time de negócio). Nenhum dos dois recalcula o outro.

# COMMAND ----------

from datetime import datetime, timezone
import pandas as pd

sucesso_modelo = salvar_modelo(modelo, "modelos/isolation_forest_pedidos.pkl")

pdf_resultado = pdf[["id_pedido", "id_cliente", "score_anomalia", "flag_anomalia"]].copy()
pdf_resultado["modelo_usado"] = "isolation_forest"
pdf_resultado["contaminacao_assumida"] = CONTAMINACAO
pdf_resultado["data_execucao"] = pd.Timestamp(datetime.now(timezone.utc))

df_resultado = spark.createDataFrame(pdf_resultado)
sucesso_gravacao = gravar_delta(df_resultado, "gold", "anomalias_pedidos", mode="overwrite")

if sucesso_modelo and sucesso_gravacao:
    print("🚀 Modelo salvo em squad1/modelos/isolation_forest_pedidos.pkl")
    print("🚀 Resultado salvo em squad1/gold/anomalias_pedidos")
else:
    print("⚠️ Algo falhou na persistência — checar os logs acima.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Próximo passo
# MAGIC
# MAGIC Notebook separado de *score*, que carrega o modelo salvo aqui (`carregar_modelo`) e aplica sobre
# MAGIC pedidos novos conforme chegam — sem retreinar. A lógica de construção das features (Seções 1–3
# MAGIC deste card de features) precisa ser encapsulada numa função reutilizável entre o treino e o score,
# MAGIC para garantir que os dois calculem exatamente as mesmas colunas, do mesmo jeito.
