# Databricks notebook source
# MAGIC %md
# MAGIC ## 1. Leitura das features

# COMMAND ----------

# MAGIC %run ../99_utils/feat_squad1_99_helpers

# COMMAND ----------

df_features = ler_delta("gold", "features_pedidos_squad3")
print(f"Linhas lidas: {df_features.count()}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Treino do Isolation Forest
# MAGIC
# MAGIC A conversão para pandas é a única ação pesada desta célula (Serverless não
# MAGIC suporta `.cache()`/`.persist()`). Nulos são preenchidos conforme
# MAGIC `VALORES_NULOS_PADRAO_FEATURES` antes do treino.

# COMMAND ----------

from sklearn.ensemble import IsolationForest

pdf = df_features.toPandas().fillna(value=VALORES_NULOS_PADRAO_FEATURES)

# Colunas decimal(...) do Spark chegam via toPandas() como object/Decimal, não
# float — o sklearn converte internamente ao treinar (por isso o fit funciona
# de qualquer forma), mas np.isnan/scipy exigem float nativo. Convertendo aqui
# garante que pdf inteiro (usado depois na avaliação estatística) já fica
# consistente, sem precisar repetir a conversão mais adiante.
for coluna in COLUNAS_MODELO_ANOMALIA:
    pdf[coluna] = pdf[coluna].astype(float)

X = pdf[COLUNAS_MODELO_ANOMALIA]

# Assumido: contamination próximo da taxa observada na Squad 3 (~4,95%) e
# random_state/n_estimators padrão do sklearn. Ajustar se
# feat_squad1_03_treinamento_isolation_forest usou valores diferentes.
modelo = IsolationForest(n_estimators=100, contamination=0.05, random_state=42)
modelo.fit(X)

pdf["anomaly_score"] = modelo.decision_function(X)
pdf["is_anomalia"] = modelo.predict(X) == -1

taxa = pdf["is_anomalia"].mean()
print(f"Pedidos sinalizados como anômalos: {pdf['is_anomalia'].sum()} ({taxa:.2%})")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Avaliação estatística (equivalente à acurácia)
# MAGIC
# MAGIC Sem rótulo real de anomalia, o equivalente à "acurácia" pedida pelo Geovany é a
# MAGIC comparação estatística entre o grupo anômalo e o grupo normal, feature a
# MAGIC feature — mesmo princípio da análise 3 da Lídia (Squad 2), adaptado para
# MAGIC comparação por feature em vez de por cliente.

# COMMAND ----------

import numpy as np
import pandas as pd
from scipy.stats import mannwhitneyu

grupo_anomalo = pdf[pdf["is_anomalia"]]
grupo_normal = pdf[~pdf["is_anomalia"]]

resultados = []
for coluna in COLUNAS_MODELO_ANOMALIA:
    _, p_valor = mannwhitneyu(grupo_anomalo[coluna], grupo_normal[coluna], alternative="two-sided")
    resultados.append({
        "feature": coluna,
        "media_anomalo": grupo_anomalo[coluna].mean(),
        "media_normal": grupo_normal[coluna].mean(),
        "p_valor": p_valor,
    })

df_avaliacao = pd.DataFrame(resultados).sort_values("p_valor")
df_avaliacao["neg_log10_p"] = -np.log10(df_avaliacao["p_valor"].clip(lower=1e-300))
df_avaliacao

# COMMAND ----------

# MAGIC %md
# MAGIC O grafico abaixo visualiza a tabela acima — indica, feature a feature, o
# MAGIC quanto o modelo separou o grupo anomalo do normal.

# COMMAND ----------

import matplotlib.pyplot as plt

df_avaliacao_plot = df_avaliacao.sort_values("neg_log10_p", ascending=True)
cores = ["#d62728" if p < 0.05 else "#999999" for p in df_avaliacao_plot["p_valor"]]

fig, ax = plt.subplots(figsize=(8, 6))
ax.barh(df_avaliacao_plot["feature"], df_avaliacao_plot["neg_log10_p"], color=cores)
ax.axvline(-np.log10(0.05), linestyle="--", color="black", linewidth=1, label="p = 0.05")
ax.set_xlabel("-log10(p-valor)")
ax.set_title("Significancia estatistica por feature (grupo anomalo vs normal)")
ax.legend()
plt.tight_layout()
plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Concentracao por cliente (Pareto)
# MAGIC
# MAGIC Segundo grafico de "o modelo esta correto": clientes ordenados por
# MAGIC quantidade de pedidos anomalos, com curva acumulada mostrando se a anomalia
# MAGIC se concentra em poucos clientes ou esta distribuida — util para checar se o
# MAGIC modelo nao esta so aprendendo o comportamento de alguns poucos clientes
# MAGIC especificos.
# MAGIC
# MAGIC **Assumido:** considera apenas clientes com pelo menos 1 pedido anomalo
# MAGIC (clientes sem anomalia nao entram na curva). Reaproveita o `pdf` desta mesma
# MAGIC sessao (ja tem `id_cliente` e `is_anomalia`), sem reler nada.

# COMMAND ----------

import numpy as np

por_cliente = (
    pdf.groupby("id_cliente")["is_anomalia"]
    .sum()
    .astype(int)
    .sort_values(ascending=False)
    .reset_index(name="qtd_pedidos_anomalos")
)
por_cliente = por_cliente[por_cliente["qtd_pedidos_anomalos"] > 0].reset_index(drop=True)

por_cliente["pct_clientes_acumulado"] = (np.arange(1, len(por_cliente) + 1) / len(por_cliente)) * 100
por_cliente["pct_anomalias_acumulado"] = (
    por_cliente["qtd_pedidos_anomalos"].cumsum() / por_cliente["qtd_pedidos_anomalos"].sum() * 100
)

top10_idx = int(np.ceil(len(por_cliente) * 0.10))
pct_concentrado_top10 = por_cliente.loc[top10_idx - 1, "pct_anomalias_acumulado"] if top10_idx > 0 else 0
idx_80 = (por_cliente["pct_anomalias_acumulado"].values >= 80).argmax()
pct_clientes_para_80pct = round(por_cliente.loc[idx_80, "pct_clientes_acumulado"], 2)

print(f"Clientes com ao menos 1 pedido anomalo: {len(por_cliente)}")
print(f"Top 10% desses clientes concentram {pct_concentrado_top10:.1f}% dos pedidos anomalos")
print(f"{pct_clientes_para_80pct}% desses clientes concentram 80% dos pedidos anomalos")

fig, ax = plt.subplots(figsize=(8, 4.5))
ax.plot(por_cliente["pct_clientes_acumulado"], por_cliente["pct_anomalias_acumulado"], color="#d62728", linewidth=2)
ax.plot([0, 100], [0, 100], linestyle="--", color="gray", linewidth=1, label="distribuicao uniforme")
ax.axhline(80, color="#2a78d6", linestyle=":", linewidth=1)
ax.axvline(pct_clientes_para_80pct, color="#2a78d6", linestyle=":", linewidth=1)
ax.plot(pct_clientes_para_80pct, 80, "o", color="#2a78d6", markersize=8)
ax.annotate(
    f"{pct_clientes_para_80pct}% dos clientes\n= 80% dos pedidos anomalos",
    xy=(pct_clientes_para_80pct, 80),
    xytext=(pct_clientes_para_80pct + 8, 55),
    fontsize=9,
    color="#2a78d6",
)
ax.set_xlabel("% acumulado de clientes (ordenados por qtd. anomalias)")
ax.set_ylabel("% acumulado de pedidos anomalos")
ax.set_title("Concentracao de pedidos anomalos por cliente (Pareto)")
ax.set_xlim(0, 100)
ax.set_ylim(0, 100)
ax.legend(loc="lower right", fontsize=9)
plt.tight_layout()
plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Distribuicao do score por grupo (normal vs anomalo)
# MAGIC
# MAGIC Terceiro grafico de "o modelo esta correto": se a separacao fizesse
# MAGIC sentido, o score dos pedidos anomalos deveria ficar visivelmente mais
# MAGIC baixo (mais negativo — no sklearn, quanto menor o `decision_function`,
# MAGIC mais anomalo) que o dos normais, com pouca sobreposicao. Inspirado no
# MAGIC grafico equivalente do Autoencoder do Jonathan (erro de reconstrucao por
# MAGIC grupo), adaptado para o `decision_function` do Isolation Forest.

# COMMAND ----------

fig, ax = plt.subplots(figsize=(7, 5))
dados_box = [
    grupo_normal["anomaly_score"],
    grupo_anomalo["anomaly_score"],
]
ax.boxplot(dados_box, tick_labels=["Normal", "Anomalo"])
ax.set_ylabel("anomaly_score (decision_function — menor = mais anomalo)")
ax.set_title("Distribuicao do Score: Normal x Anomalo")
plt.tight_layout()
plt.show()

print(
    f"Score medio — Normal: {grupo_normal['anomaly_score'].mean():.4f} | "
    f"Anomalo: {grupo_anomalo['anomaly_score'].mean():.4f}"
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Comparacao com baseline simples (Z-score em valor_total)
# MAGIC
# MAGIC Pergunta que qualquer revisor vai fazer: "por que nao uma regra simples
# MAGIC em vez de Isolation Forest?" Aqui comparamos contra um baseline ingenuo —
# MAGIC sinalizar como anomalo qualquer pedido com `|z-score| > 2` em
# MAGIC `valor_total` sozinho — e vemos quanto do resultado do modelo esse
# MAGIC baseline ja explicaria sozinho. Mesmo principio da comparacao que o
# MAGIC Jonathan fez para o Autoencoder dele.

# COMMAND ----------

media_valor = pdf["valor_total"].mean()
desvio_valor = pdf["valor_total"].std()
z_score_valor = (pdf["valor_total"] - media_valor) / desvio_valor
pdf["baseline_anomalo"] = z_score_valor.abs() > 2

sobreposicao = (pdf["baseline_anomalo"] & pdf["is_anomalia"]).sum()
so_baseline = (pdf["baseline_anomalo"] & ~pdf["is_anomalia"]).sum()
so_modelo = (~pdf["baseline_anomalo"] & pdf["is_anomalia"]).sum()

print(f"Baseline simples (so valor_total) encontrou: {pdf['baseline_anomalo'].sum()} anomalias")
print(f"Isolation Forest (18 features) encontrou:    {pdf['is_anomalia'].sum()} anomalias")
print(f"Sobreposicao (ambos concordam): {sobreposicao}")
print(f"So o baseline capturou: {so_baseline}")
print(f"So o Isolation Forest capturou: {so_modelo} "
      f"(evidencia de que o modelo enxerga padroes alem do valor bruto)")

fig, ax = plt.subplots(figsize=(7, 5))
categorias_baseline = ["So baseline", "Sobreposicao", "So Isolation Forest"]
valores_baseline = [so_baseline, sobreposicao, so_modelo]
barras = ax.bar(categorias_baseline, valores_baseline, color=["#DD8452", "#55A868", "#4C72B0"])
ax.set_ylabel("Qtd. de pedidos")
ax.set_title("Anomalias: Isolation Forest x Baseline (Z-score)")
for barra, v in zip(barras, valores_baseline):
    ax.text(barra.get_x() + barra.get_width() / 2, v, str(v), ha="center", va="bottom", fontweight="bold")
plt.tight_layout()
plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Gravação
# MAGIC
# MAGIC Persiste o modelo treinado, a base com score/rotulo de anomalia, a tabela de
# MAGIC significancia e a tabela de Pareto — as duas ultimas para reuso no Looker sem
# MAGIC precisar reabrir este notebook. Inclui tambem um score 0-100 (mais
# MAGIC intuitivo para dashboard) e sincronizacao opcional com SQL Server.

# COMMAND ----------

# Score 0-100, mais intuitivo que o decision_function cru para quem for
# consumir no Looker — mesma ideia usada pelo Jonathan no Autoencoder dele,
# invertendo o sinal (no sklearn, menor/mais negativo = mais anomalo, entao
# aqui 100 = mais anomalo, 0 = mais normal, mais intuitivo em dashboard).
score_min = pdf["anomaly_score"].min()
score_max = pdf["anomaly_score"].max()
pdf["anomaly_score_0_100"] = (100 * (score_max - pdf["anomaly_score"]) / (score_max - score_min)).round(2)

# Caminho de exemplo — ajustar para o mesmo padrão usado em salvar_modelo()
# no notebook 03 de treino, se for diferente.
salvar_modelo(modelo, "modelos/isolation_forest_batch_squad3.pkl")

df_scores = spark.createDataFrame(pdf)
sucesso_scores = gravar_delta(df_scores, "gold", "scores_pedidos_squad3", mode="overwrite")
sucesso_significancia = gravar_delta(
    spark.createDataFrame(df_avaliacao), "gold", "insights_significancia_squad3", mode="overwrite"
)
sucesso_pareto = gravar_delta(
    spark.createDataFrame(por_cliente), "gold", "insights_pareto_cliente_squad3", mode="overwrite"
)

for nome, sucesso in [
    ("scores_pedidos_squad3", sucesso_scores),
    ("insights_significancia_squad3", sucesso_significancia),
    ("insights_pareto_cliente_squad3", sucesso_pareto),
]:
    status = "gravada com sucesso." if sucesso else "FALHOU — checar log acima."
    print(f"squad1/gold/{nome} {status}")

# Sincronizacao opcional com SQL Server — sem efeito ate confirmar com quem
# administra o Looker se e necessaria (ver nota no helper).
escrever_sqlserver_gold(df_scores, schema="squad1", tabela="scores_pedidos_squad3", modo="overwrite")
escrever_sqlserver_gold(
    spark.createDataFrame(df_avaliacao), schema="squad1", tabela="insights_significancia_squad3", modo="overwrite"
)
escrever_sqlserver_gold(
    spark.createDataFrame(por_cliente), schema="squad1", tabela="insights_pareto_cliente_squad3", modo="overwrite"
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Próximo passo
# MAGIC
# MAGIC Notebook 3/3 (`feat_squad1_insights_batch`) fica só com analytics de negocio:
# MAGIC faixas de valor cruzadas com tempo e caracterizacao das anomalias (gasto
# MAGIC medio, produtos e marcas associados) — sem duplicar o que ja foi feito aqui.
