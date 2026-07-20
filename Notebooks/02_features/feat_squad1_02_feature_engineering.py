# Databricks notebook source
# MAGIC %md
# MAGIC # Engenharia de Features — Pedidos (Squad 1)
# MAGIC **Card:** Engenharia de Features (Feature Engineering)
# MAGIC
# MAGIC **Objetivo do card:** preparar os dados para IA, criando colunas numéricas que ajudem a
# MAGIC identificar padrões de compra atípicos — insumo direto para o modelo de detecção de anomalias
# MAGIC (Isolation Forest) definido na proposta de modelo da squad.
# MAGIC
# MAGIC Este notebook corresponde à fase de *Data Preparation* do CRISP-DM. Parte das tabelas já
# MAGIC validadas na camada Silver e produz, ao final, uma tabela de features numéricas na camada Gold.
# MAGIC
# MAGIC **Decisão de privacidade:** as features de histórico do cliente são calculadas inteiramente
# MAGIC dentro da própria tabela `pedidos` (via `id_cliente`), sem necessidade de join com a tabela
# MAGIC `clientes`. Isso evita que dados pessoais (nome, e-mail, hash de senha) cheguem à camada de
# MAGIC features, que deve conter apenas sinais numéricos de comportamento.

# COMMAND ----------

# MAGIC %pip install azure-storage-file-datalake azure-identity python-dotenv deltalake pyarrow

# COMMAND ----------

# MAGIC %run ../99_utils/feat_squad1_99_helpers

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Leitura das tabelas Silver
# MAGIC
# MAGIC Apenas três das cinco tabelas mapeadas no diagnóstico são necessárias para este conjunto de
# MAGIC features: `pedidos`, `itens_pedido` e `produtos` (+ `categorias`, para nomear a dimensão de
# MAGIC categoria). A tabela `clientes` não é lida, pelo motivo explicado acima.

# COMMAND ----------

df_pedidos = ler_delta("silver", "ecommerce_pedidos")
df_itens = ler_delta("silver", "ecommerce_itens_pedido")
df_produtos = ler_delta("silver", "ecommerce_produtos")
df_categorias = ler_delta("silver", "ecommerce_categorias")

total_pedidos_original = df_pedidos.count()
print(f"✅ Tabelas carregadas. Baseline de pedidos: {total_pedidos_original} linhas.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Features derivadas do tempo do pedido
# MAGIC
# MAGIC Extraídas diretamente de `dt_pedido` — não das colunas `ano`/`mes`/`dia`/`hora` já existentes,
# MAGIC pois essas últimas referem-se ao momento de ingestão no Data Lake, não ao momento real da compra.
# MAGIC
# MAGIC * `hora_do_pedido`: hora do dia (0–23) em que o pedido foi feito.
# MAGIC * `dia_semana_pedido`: dia da semana (1 = domingo ... 7 = sábado, padrão do Spark).

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.window import Window

df_pedidos_tempo = (
    df_pedidos
    .withColumn("hora_do_pedido", F.hour("dt_pedido"))
    .withColumn("dia_semana_pedido", F.dayofweek("dt_pedido"))
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Features de histórico do cliente (Window Functions)
# MAGIC
# MAGIC Cada janela abaixo é particionada por `id_cliente` e ordenada por `dt_pedido`. O ponto crítico
# MAGIC aqui é **olhar só para pedidos anteriores ao atual** (`rowsBetween(unboundedPreceding, -1)`) —
# MAGIC incluir o próprio pedido no cálculo da média histórica seria vazamento de informação
# MAGIC (*data leakage*): o modelo estaria usando o valor que ele mesmo precisa avaliar como anômalo
# MAGIC para compor a média de referência.
# MAGIC
# MAGIC * `ticket_medio_historico_cliente` / `desvio_padrao_historico_cliente`: média e desvio padrão
# MAGIC   dos pedidos anteriores desse cliente.
# MAGIC * `desvio_valor_vs_historico`: quantos desvios padrão o valor do pedido atual está distante da
# MAGIC   própria média do cliente — a feature mais importante do conjunto, pois transforma "valor alto"
# MAGIC   em "valor alto para aquele cliente específico".
# MAGIC * `desvio_pct_vs_media_cliente`: mesma ideia, mas em variação percentual simples em vez de desvios
# MAGIC   padrão. É uma métrica complementar, não substituta — o desvio em desvios-padrão pondera pela
# MAGIC   volatilidade histórica do próprio cliente (um cliente com gasto muito instável precisa de um
# MAGIC   desvio maior para ser considerado anômalo); o percentual não pondera por isso, mas é mais direto
# MAGIC   de interpretar. Manter as duas dá ao modelo dois ângulos diferentes do mesmo sinal.
# MAGIC * `dias_desde_ultima_compra`: intervalo, em dias, desde o pedido anterior do mesmo cliente.
# MAGIC * `qtd_pedidos_ultimos_30_dias`: quantidade de pedidos desse cliente nos 30 dias anteriores ao
# MAGIC   pedido atual (janela baseada em tempo, não em número de linhas).
# MAGIC
# MAGIC **Nota sobre nulos:** o primeiro pedido de cada cliente não tem histórico anterior, então essas
# MAGIC colunas ficam nulas nesse caso — isso é esperado, não um erro. A decisão de como tratar esses
# MAGIC nulos (zerar, imputar ou manter) fica para a etapa de modelagem, pois zerar precipitadamente
# MAGIC aqui poderia distorcer o sinal de anomalia.

# COMMAND ----------

janela_historico = (
    Window.partitionBy("id_cliente")
    .orderBy("dt_pedido")
    .rowsBetween(Window.unboundedPreceding, -1)
)

janela_pedido_anterior = Window.partitionBy("id_cliente").orderBy("dt_pedido")

df_pedidos_ts = df_pedidos_tempo.withColumn("dt_pedido_unix", F.col("dt_pedido").cast("long"))

janela_30_dias = (
    Window.partitionBy("id_cliente")
    .orderBy("dt_pedido_unix")
    .rangeBetween(-30 * 86400, -1)
)

df_pedidos_features = (
    df_pedidos_ts
    .withColumn("ticket_medio_historico_cliente", F.avg("valor_total").over(janela_historico))
    .withColumn("desvio_padrao_historico_cliente", F.stddev("valor_total").over(janela_historico))
    .withColumn("qtd_pedidos_historico_cliente", F.count("id_pedido").over(janela_historico))
    .withColumn("dt_pedido_anterior", F.lag("dt_pedido").over(janela_pedido_anterior))
    .withColumn("dias_desde_ultima_compra", F.datediff(F.col("dt_pedido"), F.col("dt_pedido_anterior")))
    .withColumn("qtd_pedidos_ultimos_30_dias", F.count("id_pedido").over(janela_30_dias))
    .withColumn(
        "desvio_valor_vs_historico",
        F.when(
            F.col("desvio_padrao_historico_cliente") > 0,
            (F.col("valor_total") - F.col("ticket_medio_historico_cliente"))
            / F.col("desvio_padrao_historico_cliente"),
        ).otherwise(F.lit(None).cast("double")),
    )
    .withColumn(
        "desvio_pct_vs_media_cliente",
        F.when(
            F.col("ticket_medio_historico_cliente") > 0,
            (F.col("valor_total") - F.col("ticket_medio_historico_cliente"))
            / F.col("ticket_medio_historico_cliente"),
        ).otherwise(F.lit(None).cast("double")),
    )
    .withColumn(
        "razao_frete_valor",
        F.when(F.col("valor_total") > 0, F.col("valor_frete") / F.col("valor_total")),
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Features de composição do pedido (itens, produtos, categorias)
# MAGIC
# MAGIC `itens_pedido` é cruzada com `produtos` (via `sku`) e `categorias` (via `id_categoria`), depois
# MAGIC agregada por `id_pedido` — o resultado é **uma linha por pedido**, pronta para juntar de volta
# MAGIC com `df_pedidos_features` sem duplicar nenhum pedido.
# MAGIC
# MAGIC * `qtd_linhas_itens`: quantidade de itens (SKUs) distintos no pedido.
# MAGIC * `qtd_unidades_total`: soma das unidades compradas (considerando quantidade de cada item).
# MAGIC * `qtd_categorias_distintas`: diversidade de categorias dentro do mesmo pedido — um pedido que
# MAGIC   mistura muitas categorias diferentes foge do padrão típico de compra concentrada.
# MAGIC * `valor_itens_calculado`: soma de `quantidade × preco_unitario − desconto_aplicado` de todos os
# MAGIC   itens — recalculado de forma independente do `valor_total` que já vem em `pedidos`.
# MAGIC * `diferenca_valor_itens_vs_pedido`: `valor_total` menos `valor_itens_calculado`. Esta é uma
# MAGIC   feature extra em relação ao combinado originalmente — surgiu naturalmente ao cruzar as tabelas,
# MAGIC   e tem valor duplo: além de sinalizar comportamento anômalo, uma diferença grande e sistemática
# MAGIC   aqui também apontaria um problema de qualidade de dados no pipeline (ex: frete somado errado),
# MAGIC   o que é diretamente relevante para a missão da squad.

# COMMAND ----------

df_itens_produtos = (
    df_itens
    .join(df_produtos.select("sku", "id_categoria"), on="sku", how="left")
)

df_composicao_pedido = (
    df_itens_produtos
    .groupBy("id_pedido")
    .agg(
        F.count("id_item_pedido").alias("qtd_linhas_itens"),
        F.sum("quantidade").alias("qtd_unidades_total"),
        F.countDistinct("id_categoria").alias("qtd_categorias_distintas"),
        F.sum(
            F.col("quantidade") * F.col("preco_unitario") - F.col("desconto_aplicado")
        ).alias("valor_itens_calculado"),
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. União final e seleção de colunas
# MAGIC
# MAGIC O join com `df_composicao_pedido` é `left`, mantendo todos os pedidos mesmo que, por algum
# MAGIC motivo, não tenham itens associados (o que também seria, em si, um sinal de anomalia a observar).
# MAGIC A seleção final de colunas exclui deliberadamente qualquer coluna de auditoria técnica
# MAGIC (`bronze_run_id`, `silver_processed_at`, etc.) — a tabela Gold de features deve conter apenas o
# MAGIC necessário para o modelo.

# COMMAND ----------

df_features = (
    df_pedidos_features
    .join(df_composicao_pedido, on="id_pedido", how="left")
    .withColumn(
        "ticket_medio_por_unidade",
        F.when(F.col("qtd_unidades_total") > 0, F.col("valor_total") / F.col("qtd_unidades_total")),
    )
    .withColumn(
        "diferenca_valor_itens_vs_pedido",
        F.col("valor_total") - F.col("valor_itens_calculado"),
    )
)

COLUNAS_FEATURES = [
    "id_pedido", "id_cliente", "dt_pedido", "valor_total", "valor_frete", "razao_frete_valor",
    "hora_do_pedido", "dia_semana_pedido",
    "ticket_medio_historico_cliente", "desvio_padrao_historico_cliente",
    "qtd_pedidos_historico_cliente", "dias_desde_ultima_compra",
    "qtd_pedidos_ultimos_30_dias", "desvio_valor_vs_historico", "desvio_pct_vs_media_cliente",
    "qtd_linhas_itens", "qtd_unidades_total", "qtd_categorias_distintas",
    "ticket_medio_por_unidade", "valor_itens_calculado", "diferenca_valor_itens_vs_pedido",
]

df_features_final = df_features.select(*COLUNAS_FEATURES)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Validação pós-cruzamento
# MAGIC
# MAGIC Como cada pedido deve gerar exatamente uma linha na tabela final, o total de linhas aqui precisa
# MAGIC bater com o baseline lido na Seção 1. Qualquer diferença indicaria um join duplicando ou
# MAGIC descartando pedidos — sinal de que algo na lógica precisa ser revisto antes de gravar.

# COMMAND ----------

total_features = df_features_final.count()
print(f"Pedidos originais : {total_pedidos_original}")
print(f"Linhas no resultado: {total_features}")

if total_features == total_pedidos_original:
    print("✅ Volume consistente — nenhum pedido duplicado ou perdido no cruzamento.")
else:
    print("⚠️ Volume divergente — revisar a lógica de join antes de gravar na Gold.")

print("\n👀 Amostra do resultado:")
display(df_features_final.limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Gravação na camada Gold
# MAGIC
# MAGIC Gravação em modo `overwrite`: diferente das camadas Bronze/Silver (que acumulam histórico via
# MAGIC `append` e checkpoint), a tabela de features representa um recálculo completo a partir do estado
# MAGIC atual da Silver — por isso substitui integralmente o conteúdo anterior a cada execução.

# COMMAND ----------

sucesso = gravar_delta(df_features_final, "gold", "features_pedidos", mode="overwrite")

if sucesso:
    print("🚀 Tabela squad1/gold/features_pedidos gravada com sucesso.")
else:
    print("❌ Falha na gravação — checar o log acima para detalhes.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Próximo passo
# MAGIC
# MAGIC Com `squad1/gold/features_pedidos` disponível, a etapa seguinte do CRISP-DM é a *Modeling*: treinar
# MAGIC o Isolation Forest sobre essas colunas numéricas e gerar o score de anomalia por pedido — o próximo
# MAGIC card da squad.
