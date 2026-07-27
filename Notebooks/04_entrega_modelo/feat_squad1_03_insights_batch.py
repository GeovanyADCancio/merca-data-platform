# Databricks notebook source
# MAGIC %md
# MAGIC ## Insights — analytics para verificar os resultados
# MAGIC
# MAGIC Terceiro notebook da entrega: analytics de negocio sobre os pedidos ja
# MAGIC classificados (Pareto por cliente e o grafico de significancia ficaram no
# MAGIC Notebook 2, como graficos de "o modelo esta correto"). Aqui: (1) faixas de
# MAGIC valor cruzadas com tempo, (2) caracterizacao das anomalias — gasto medio,
# MAGIC produtos/marcas e combinacoes de categoria desproporcionais, pedido
# MAGIC explicito do Geovany —, (3) segmentacao de clientes, e (4) cruzamento com
# MAGIC status do pedido como validacao de negocio externa ao modelo. As tres
# MAGIC ultimas foram incorporadas depois de revisar o notebook do Autoencoder do
# MAGIC Jonathan (Squad1+Squad3) — adaptadas para o Isolation Forest onde o
# MAGIC conceito nao portava 1:1.

# COMMAND ----------

# MAGIC %run ../99_utils/feat_squad1_99_helpers

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Leitura
# MAGIC
# MAGIC `scores_pedidos_squad3` traz o rotulo de anomalia. `itens_pedido` e
# MAGIC `produtos` sao lidos direto da Squad 3 (nunca entraram no modelo — as 18
# MAGIC features so guardam contagens/agregados, nao a identidade do produto).

# COMMAND ----------

import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import pyspark.sql.functions as F

df_scores = ler_delta("gold", "scores_pedidos_squad3")
pdf = df_scores.toPandas()
print(f"Linhas lidas: {len(pdf)}")
print(f"Pedidos anomalos: {pdf['is_anomalia'].sum()} ({pdf['is_anomalia'].mean():.2%})")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Faixas de valor cruzadas com tempo
# MAGIC
# MAGIC Taxa de anomalia por faixa de `valor_total` cruzada com `dia_semana_pedido`.
# MAGIC
# MAGIC **Assumido:** faixas de valor em quartis (`qcut`) em vez de cortes fixos em
# MAGIC reais, para se adaptar a mudanca de escala se a base for reprocessada. Trocar
# MAGIC por cortes fixos se o dashboard exigir faixas de negocio especificas
# MAGIC (ex.: "ate R$100", "R$100-300"...).
# MAGIC
# MAGIC **Nota sobre "tempo":** no notebook de referencia (Lidia, Squad 2), "tempo"
# MAGIC significa horas entre o pedido e o cancelamento — um evento que so existe
# MAGIC porque cancelamento tem um "antes/depois" no tempo. Deteccao de anomalia nao
# MAGIC tem equivalente (o pedido ja nasce anomalo ou nao, no momento do score), entao
# MAGIC aqui "tempo" foi adaptado para dia da semana do pedido.

# COMMAND ----------

pdf["faixa_valor"] = pd.qcut(pdf["valor_total"], q=4, labels=["Q1 (mais baixo)", "Q2", "Q3", "Q4 (mais alto)"])

dias_semana = {1: "Dom", 2: "Seg", 3: "Ter", 4: "Qua", 5: "Qui", 6: "Sex", 7: "Sab"}
pdf["dia_semana_label"] = pdf["dia_semana_pedido"].map(dias_semana).fillna(pdf["dia_semana_pedido"].astype(str))

faixas_pivot = pdf.pivot_table(
    index="faixa_valor", columns="dia_semana_label", values="is_anomalia", aggfunc="mean", observed=True
) * 100

fig, ax = plt.subplots(figsize=(8, 4.5))
im = ax.imshow(faixas_pivot.values, cmap="Reds", aspect="auto")
ax.set_xticks(range(len(faixas_pivot.columns)))
ax.set_xticklabels(faixas_pivot.columns)
ax.set_yticks(range(len(faixas_pivot.index)))
ax.set_yticklabels(faixas_pivot.index)
ax.set_title("Taxa de anomalia (%) por faixa de valor x dia da semana")
for i in range(faixas_pivot.shape[0]):
    for j in range(faixas_pivot.shape[1]):
        ax.text(j, i, f"{faixas_pivot.values[i, j]:.1f}", ha="center", va="center", fontsize=8)
fig.colorbar(im, ax=ax, label="% anomalia")
plt.tight_layout()
plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Caracterizacao das anomalias — gasto medio e produtos
# MAGIC
# MAGIC Pedido do Geovany: nao e necessario revisar pedidos anomalos individualmente
# MAGIC nem pegar uma amostra percentual. A analise abaixo e agregada sobre todos os
# MAGIC pedidos anomalos, olhando (1) gasto medio e (2) quais produtos/marcas aparecem
# MAGIC desproporcionalmente nesses pedidos em relacao aos pedidos normais — para
# MAGIC conseguir descrever o grupo em termos de comportamento de compra, nao so de
# MAGIC metricas abstratas.
# MAGIC
# MAGIC **Assumido:** nao ha tabela de categorias com nome legivel disponivel — a
# MAGIC granularidade usada aqui e `nome_produto`/`nome_marca`. Se existir uma tabela
# MAGIC de categorias, vale incluir esse join tambem.

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3.1 Gasto medio — grupo anomalo vs normal

# COMMAND ----------

resumo_gasto = (
    pdf.groupby("is_anomalia")
    .agg(
        qtd_pedidos=("id_pedido", "count"),
        valor_total_medio=("valor_total", "mean"),
        qtd_linhas_itens_media=("qtd_linhas_itens", "mean"),
        qtd_unidades_media=("qtd_unidades_total", "mean"),
    )
    .reset_index()
)
resumo_gasto["grupo"] = resumo_gasto["is_anomalia"].map({True: "Anomalo", False: "Normal"})
print(resumo_gasto[["grupo", "qtd_pedidos", "valor_total_medio", "qtd_linhas_itens_media", "qtd_unidades_media"]].round(2).to_string(index=False))

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3.2 Produtos e marcas mais associados aos pedidos anomalos
# MAGIC
# MAGIC Para cada produto/marca, compara a frequencia relativa dentro dos pedidos
# MAGIC anomalos com a frequencia dentro dos pedidos normais — o "lift". Lift > 1
# MAGIC significa que o produto aparece proporcionalmente mais nos pedidos anomalos.
# MAGIC Um filtro de volume minimo evita que produtos muito raros dominem o ranking
# MAGIC so por acaso estatistico.

# COMMAND ----------

df_itens = ler_delta_squad3("silver", "ecommerce_itens_pedido")
df_produtos = ler_delta_squad3("silver", "ecommerce_produtos")

df_itens_produtos = df_itens.join(
    df_produtos.select("sku", "nome_produto", "nome_marca", "id_categoria"), on="sku", how="left"
)
df_itens_scored = df_itens_produtos.join(
    df_scores.select("id_pedido", "is_anomalia"), on="id_pedido", how="inner"
)

pdf_itens = df_itens_scored.select("is_anomalia", "nome_produto", "nome_marca", "quantidade").toPandas()
print(f"Linhas de item cruzadas com score: {len(pdf_itens)}")

# COMMAND ----------

MIN_OCORRENCIAS_ANOMALO = 20  # filtro de volume minimo, ajustar se o ranking vier ruidoso

total_linhas_anomalo = len(pdf_itens[pdf_itens["is_anomalia"]])
total_linhas_normal = len(pdf_itens[~pdf_itens["is_anomalia"]])


def ranking_por_coluna(coluna):
    contagem_anomalo = pdf_itens[pdf_itens["is_anomalia"]][coluna].value_counts()
    contagem_normal = pdf_itens[~pdf_itens["is_anomalia"]][coluna].value_counts()

    tabela = (
        contagem_anomalo.rename("qtd_em_anomalos")
        .to_frame()
        .join(contagem_normal.rename("qtd_em_normais"), how="left")
        .fillna(0)
    )
    tabela = tabela[tabela["qtd_em_anomalos"] >= MIN_OCORRENCIAS_ANOMALO]

    tabela["freq_em_anomalos"] = tabela["qtd_em_anomalos"] / total_linhas_anomalo
    tabela["freq_em_normais"] = tabela["qtd_em_normais"] / total_linhas_normal
    tabela["lift"] = tabela["freq_em_anomalos"] / tabela["freq_em_normais"].replace(0, float("nan"))
    return tabela.sort_values("lift", ascending=False)


ranking_produtos = ranking_por_coluna("nome_produto")
ranking_marcas = ranking_por_coluna("nome_marca")

print("Top 10 produtos mais associados a pedidos anomalos (lift):")
print(ranking_produtos.head(10)[["qtd_em_anomalos", "lift"]].round(2).to_string())
print("\nTop 10 marcas mais associadas a pedidos anomalos (lift):")
print(ranking_marcas.head(10)[["qtd_em_anomalos", "lift"]].round(2).to_string())

# COMMAND ----------

top_produtos = ranking_produtos.head(12).sort_values("lift")
fig, ax = plt.subplots(figsize=(8, 6))
ax.barh(top_produtos.index.astype(str), top_produtos["lift"], color="#d62728")
ax.axvline(1, color="black", linestyle="--", linewidth=1, label="lift = 1 (sem associacao)")
ax.set_xlabel("Lift (frequencia em anomalos / frequencia em normais)")
ax.set_title("Produtos mais associados a pedidos anomalos")
ax.legend()
plt.tight_layout()
plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3.3 Feature dominante por pedido anomalo
# MAGIC
# MAGIC Pra cada pedido anomalo, aponta qual das 18 features mais se desviou do
# MAGIC padrao do grupo normal (maior `|z-score|` contra media/desvio do grupo
# MAGIC normal) — adaptacao do conceito de "feature dominante" do Autoencoder do
# MAGIC Jonathan (la, e a feature com maior erro de reconstrucao; aqui, e a de
# MAGIC maior desvio estatistico), respondendo a mesma pergunta: "por que ESSE
# MAGIC pedido especifico foi sinalizado", nao so o grupo como um todo.
# MAGIC
# MAGIC **Atencao:** nao conferimos isso contra `feat_squad1_04_interpretabilidade_modelo`
# MAGIC (o notebook exploratorio da Squad 1 sozinha) — se ja existir logica
# MAGIC parecida la, vale comparar antes de finalizar, pra nao duplicar/divergir.

# COMMAND ----------

grupo_normal_ins = pdf[~pdf["is_anomalia"]]
media_normal_ins = grupo_normal_ins[COLUNAS_MODELO_ANOMALIA].mean()
desvio_normal_ins = grupo_normal_ins[COLUNAS_MODELO_ANOMALIA].std().replace(0, np.nan)

z_scores_anomalos = (
    pdf.loc[pdf["is_anomalia"], COLUNAS_MODELO_ANOMALIA] - media_normal_ins
) / desvio_normal_ins
pdf.loc[pdf["is_anomalia"], "feature_dominante"] = z_scores_anomalos.abs().idxmax(axis=1)

contagem_dominante = pdf.loc[pdf["is_anomalia"], "feature_dominante"].value_counts().reset_index()
contagem_dominante.columns = ["feature_dominante", "qtd_anomalias"]
contagem_dominante["pct"] = (
    100 * contagem_dominante["qtd_anomalias"] / contagem_dominante["qtd_anomalias"].sum()
).round(1)

print("Feature dominante entre os pedidos anomalos:")
print(contagem_dominante.to_string(index=False))

fig, ax = plt.subplots(figsize=(8, max(4, 0.4 * len(contagem_dominante))))
ax.barh(contagem_dominante["feature_dominante"], contagem_dominante["pct"], color="#2a78d6")
ax.set_xlabel("% das anomalias")
ax.set_title("Feature dominante por pedido anomalo")
for i, v in enumerate(contagem_dominante["pct"]):
    ax.text(v, i, f" {v}%", va="center", fontweight="bold")
plt.tight_layout()
plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3.4 Combinacoes de categoria nos pedidos anomalos
# MAGIC
# MAGIC Regra de associacao simplificada (sem lib externa): para cada par de
# MAGIC categorias que aparece junto num mesmo pedido anomalo, calcula suporte,
# MAGIC confianca e lift — mesma logica do achado "cerveja + fralda", aplicada a
# MAGIC pares de categoria em vez de produto isolado. Complementa o ranking de
# MAGIC produto/marca da secao 3.2 com uma resposta mais direta tipo "essa
# MAGIC combinacao especifica e rara".
# MAGIC
# MAGIC **Assumido:** tenta ler uma tabela `ecommerce_categorias` (com
# MAGIC `nome_categoria` legivel) da Squad 3, seguindo o mesmo padrao usado pelo
# MAGIC Jonathan. Se essa tabela nao existir nesse ambiente, cai para usar o
# MAGIC `id_categoria` bruto, sem quebrar o notebook.

# COMMAND ----------

try:
    df_categorias = ler_delta_squad3("silver", "ecommerce_categorias")
    coluna_nome = "nome_categoria" if "nome_categoria" in df_categorias.columns else df_categorias.columns[-1]
    df_itens_categoria = (
        df_itens_produtos
        .join(
            df_categorias.select("id_categoria", F.col(coluna_nome).alias("nome_categoria")),
            on="id_categoria", how="left",
        )
        .join(df_scores.select("id_pedido", "is_anomalia"), on="id_pedido", how="inner")
    )
    print("ecommerce_categorias encontrada — usando nome_categoria legivel.")
except Exception as e:
    print(f"[Aviso] Nao foi possivel ler ecommerce_categorias ({str(e)[:120]}) — usando id_categoria bruto.")
    df_itens_categoria = (
        df_itens_produtos
        .join(df_scores.select("id_pedido", "is_anomalia"), on="id_pedido", how="inner")
        .withColumnRenamed("id_categoria", "nome_categoria")
    )

pdf_pedido_categoria = (
    df_itens_categoria
    .select("id_pedido", "is_anomalia", "nome_categoria")
    .dropna(subset=["nome_categoria"])
    .distinct()
    .toPandas()
)

from itertools import combinations

cestas_anomalia = (
    pdf_pedido_categoria[pdf_pedido_categoria["is_anomalia"]]
    .groupby("id_pedido")["nome_categoria"]
    .apply(set)
)
total_cestas = len(cestas_anomalia)
contagem_individual_cat = (
    pdf_pedido_categoria[pdf_pedido_categoria["is_anomalia"]]["nome_categoria"].value_counts().to_dict()
)

contagem_pares = {}
for cesta in cestas_anomalia:
    for par in combinations(sorted(cesta), 2):
        contagem_pares[par] = contagem_pares.get(par, 0) + 1

linhas_regras = []
for (cat_a, cat_b), qtd_junto in contagem_pares.items():
    suporte = qtd_junto / total_cestas if total_cestas else 0
    confianca_a_b = qtd_junto / contagem_individual_cat[cat_a]
    prob_b = contagem_individual_cat[cat_b] / total_cestas if total_cestas else 0
    lift = confianca_a_b / prob_b if prob_b > 0 else 0
    linhas_regras.append({
        "categoria_a": cat_a, "categoria_b": cat_b,
        "qtd_pedidos_juntos": qtd_junto,
        "suporte_pct": round(100 * suporte, 2),
        "confianca_pct": round(100 * confianca_a_b, 2),
        "lift": round(lift, 2),
    })

if linhas_regras:
    df_regras_categoria = pd.DataFrame(linhas_regras).sort_values("lift", ascending=False)
    print("Top 10 combinacoes de categoria mais fortes nos pedidos anomalos:")
    print(df_regras_categoria.head(10).to_string(index=False))
else:
    df_regras_categoria = pd.DataFrame(
        columns=["categoria_a", "categoria_b", "qtd_pedidos_juntos", "suporte_pct", "confianca_pct", "lift"]
    )
    print("Nenhum pedido anomalo tem 2+ categorias distintas para formar combinacoes.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Segmentacao de clientes
# MAGIC
# MAGIC Segmentacao simples de negocio, direto do historico ja calculado nas
# MAGIC features (sem coluna nova): Cliente Novo (sem pedido anterior),
# MAGIC Recorrente - Alto Valor / Baixo Valor (ticket historico acima/abaixo da
# MAGIC mediana). Mesmo padrao usado pelo Jonathan no notebook de insights dele.

# COMMAND ----------

mediana_ticket = pdf.loc[
    pdf["qtd_pedidos_historico_cliente"] > 0, "ticket_medio_historico_cliente"
].median()


def classificar_segmento(row):
    if row["qtd_pedidos_historico_cliente"] == 0:
        return "Cliente Novo"
    elif row["ticket_medio_historico_cliente"] >= mediana_ticket:
        return "Recorrente - Alto Valor"
    else:
        return "Recorrente - Baixo Valor"


pdf["segmento_cliente"] = pdf.apply(classificar_segmento, axis=1)

resumo_segmento = (
    pdf.groupby("segmento_cliente")
    .agg(qtd_pedidos=("id_pedido", "count"), qtd_anomalias=("is_anomalia", "sum"))
    .assign(taxa_anomalia_pct=lambda d: round(100 * d["qtd_anomalias"] / d["qtd_pedidos"], 2))
    .reset_index()
)
print(resumo_segmento.to_string(index=False))

fig, ax = plt.subplots(figsize=(7, 4.5))
ax.bar(resumo_segmento["segmento_cliente"], resumo_segmento["taxa_anomalia_pct"], color="#d62728")
ax.set_ylabel("% de anomalia")
ax.set_title("Taxa de Anomalia por Segmento de Cliente")
plt.xticks(rotation=15)
for i, v in enumerate(resumo_segmento["taxa_anomalia_pct"]):
    ax.text(i, v, f"{v}%", ha="center", va="bottom", fontweight="bold")
plt.tight_layout()
plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Anomalias x status do pedido
# MAGIC
# MAGIC `status_pedido` nunca foi feature do modelo (evita vazar informacao
# MAGIC pos-fato) — mas serve como validacao de negocio externa: se pedidos
# MAGIC cancelados tem taxa de anomalia mais alta que a media geral, e evidencia
# MAGIC de que o modelo capta sinal real, vindo de fora do proprio treino. Le
# MAGIC direto da Squad 3, ja que essa coluna nao esta em `scores_pedidos_squad3`.
# MAGIC
# MAGIC **Nota:** normalizamos com `.str.upper()` porque vimos, nos dados do
# MAGIC Jonathan, `status_pedido` gravado com caixa inconsistente entre origens
# MAGIC (ex.: "Pagamento Aprovado" e "PAGAMENTO APROVADO" como valores
# MAGIC diferentes) — mais vale prevenir aqui tambem.

# COMMAND ----------

df_status = ler_delta_squad3("silver", "ecommerce_pedidos").select("id_pedido", "status_pedido")
pdf_status = df_status.toPandas()
pdf_status["status_pedido"] = pdf_status["status_pedido"].astype(str).str.strip().str.upper()
pdf_com_status = pdf.merge(pdf_status, on="id_pedido", how="left")

resumo_status = (
    pdf_com_status.groupby("status_pedido")
    .agg(qtd_pedidos=("id_pedido", "count"), qtd_anomalias=("is_anomalia", "sum"))
    .assign(taxa_anomalia_pct=lambda d: round(100 * d["qtd_anomalias"] / d["qtd_pedidos"], 2))
    .reset_index()
    .sort_values("taxa_anomalia_pct", ascending=False)
)
print(resumo_status.to_string(index=False))

fig, ax = plt.subplots(figsize=(8, 5))
ax.barh(resumo_status["status_pedido"], resumo_status["taxa_anomalia_pct"], color="#2a78d6")
ax.set_xlabel("% de anomalia")
ax.set_title("Taxa de Anomalia por Status do Pedido")
for i, v in enumerate(resumo_status["taxa_anomalia_pct"]):
    ax.text(v, i, f" {v}%", va="center", fontweight="bold")
plt.tight_layout()
plt.show()

taxa_geral_status = round(100 * pdf["is_anomalia"].sum() / len(pdf), 2)
if "CANCELADO" in resumo_status["status_pedido"].values:
    linha_cancelado = resumo_status[resumo_status["status_pedido"] == "CANCELADO"].iloc[0]
    print(
        f"\nPedidos cancelados tem taxa de anomalia de {linha_cancelado['taxa_anomalia_pct']}%, "
        f"contra {taxa_geral_status}% da base geral "
        f"({'ACIMA' if linha_cancelado['taxa_anomalia_pct'] > taxa_geral_status else 'ABAIXO'} da media)."
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Gravacao (para consumo no Looker)

# COMMAND ----------

sucesso_faixas = gravar_delta(
    spark.createDataFrame(faixas_pivot.reset_index()), "gold", "insights_faixas_valor_tempo_squad3", mode="overwrite"
)
sucesso_gasto = gravar_delta(
    spark.createDataFrame(resumo_gasto.drop(columns=["is_anomalia"])), "gold", "insights_gasto_medio_squad3", mode="overwrite"
)
sucesso_produtos = gravar_delta(
    spark.createDataFrame(ranking_produtos.reset_index().rename(columns={"nome_produto": "produto"})),
    "gold", "insights_produtos_anomalias_squad3", mode="overwrite"
)
sucesso_marcas = gravar_delta(
    spark.createDataFrame(ranking_marcas.reset_index().rename(columns={"nome_marca": "marca"})),
    "gold", "insights_marcas_anomalias_squad3", mode="overwrite"
)

# Caracterizacao por pedido (feature dominante + segmento), tabela separada —
# nao sobrescreve scores_pedidos_squad3 (que e responsabilidade do Notebook 2),
# so complementa via join por id_pedido no Looker/analises futuras.
df_caracterizacao_pedido = spark.createDataFrame(
    pdf.loc[pdf["is_anomalia"], ["id_pedido", "feature_dominante", "segmento_cliente"]]
)
sucesso_caracterizacao = gravar_delta(
    df_caracterizacao_pedido, "gold", "insights_caracterizacao_pedido_squad3", mode="overwrite"
)

sucesso_categorias = gravar_delta(
    spark.createDataFrame(df_regras_categoria), "gold", "insights_combinacoes_categoria_squad3", mode="overwrite"
)
sucesso_segmento = gravar_delta(
    spark.createDataFrame(resumo_segmento), "gold", "insights_segmento_cliente_squad3", mode="overwrite"
)
sucesso_status = gravar_delta(
    spark.createDataFrame(resumo_status), "gold", "insights_status_pedido_squad3", mode="overwrite"
)

for nome, sucesso in [
    ("insights_faixas_valor_tempo_squad3", sucesso_faixas),
    ("insights_gasto_medio_squad3", sucesso_gasto),
    ("insights_produtos_anomalias_squad3", sucesso_produtos),
    ("insights_marcas_anomalias_squad3", sucesso_marcas),
    ("insights_caracterizacao_pedido_squad3", sucesso_caracterizacao),
    ("insights_combinacoes_categoria_squad3", sucesso_categorias),
    ("insights_segmento_cliente_squad3", sucesso_segmento),
    ("insights_status_pedido_squad3", sucesso_status),
]:
    status = "gravada com sucesso." if sucesso else "FALHOU — checar log acima."
    print(f"squad1/gold/{nome} {status}")

# Sincronizacao opcional com SQL Server — sem efeito ate confirmar com quem
# administra o Looker se e necessaria (ver nota no helper).
for df_pandas, tabela in [
    (faixas_pivot.reset_index(), "insights_faixas_valor_tempo_squad3"),
    (resumo_gasto.drop(columns=["is_anomalia"]), "insights_gasto_medio_squad3"),
    (ranking_produtos.reset_index().rename(columns={"nome_produto": "produto"}), "insights_produtos_anomalias_squad3"),
    (ranking_marcas.reset_index().rename(columns={"nome_marca": "marca"}), "insights_marcas_anomalias_squad3"),
    (resumo_segmento, "insights_segmento_cliente_squad3"),
    (resumo_status, "insights_status_pedido_squad3"),
]:
    escrever_sqlserver_gold(spark.createDataFrame(df_pandas), schema="squad1", tabela=tabela, modo="overwrite")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Proximo passo
# MAGIC
# MAGIC Com os 3 notebooks de entrega fechados nesse formato (features, modelo com
# MAGIC graficos de correcao, analytics), falta: (1) revisar se os graficos e faixas
# MAGIC fazem sentido para o formato de dashboard que o Looker vai consumir,
# MAGIC (2) levar o ranking de produtos/marcas e as combinacoes de categoria de
# MAGIC volta para o Geovany, e (3) confirmar com quem administra o Looker se a
# MAGIC sincronizacao com SQL Server (`escrever_sqlserver_gold`) e realmente
# MAGIC necessaria ou se o Looker ja consegue ler direto do Delta/gold.
