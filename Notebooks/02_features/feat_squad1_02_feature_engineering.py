# Databricks notebook source
# MAGIC %md
# MAGIC # Engenharia de Features — Pedidos (Squad 1)
# MAGIC **Card:** Engenharia de Features (Feature Engineering)
# MAGIC
# MAGIC **Objetivo do card:** preparar os dados para IA, criando colunas numéricas que ajudem a
# MAGIC identificar padrões de compra atípicos — insumo direto para o modelo de detecção de anomalias
# MAGIC (Isolation Forest) definido na proposta de modelo da squad.
# MAGIC
# MAGIC Este notebook corresponde à fase de *Data Preparation* do CRISP-DM. A lógica de cálculo das
# MAGIC features vive em `construir_features_pedidos`, no helper compartilhado — este notebook apenas
# MAGIC lê as tabelas Silver, chama essa função e grava o resultado na Gold. A mesma função é reutilizada
# MAGIC pelo notebook de score em dados novos, garantindo que treino e score calculem exatamente as
# MAGIC mesmas colunas, do mesmo jeito.
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
# MAGIC ## 1. Leitura das tabelas Silver e construção das features
# MAGIC
# MAGIC Apenas três das cinco tabelas mapeadas no diagnóstico são necessárias: `pedidos`, `itens_pedido`
# MAGIC e `produtos`. A tabela `clientes` não é lida, pelo motivo explicado acima; `categorias` também
# MAGIC não é necessária — a diversidade de categorias é medida via `id_categoria`, já presente em
# MAGIC `produtos`.

# COMMAND ----------

df_pedidos = ler_delta("silver", "ecommerce_pedidos")
df_itens = ler_delta("silver", "ecommerce_itens_pedido")
df_produtos = ler_delta("silver", "ecommerce_produtos")

total_pedidos_original = df_pedidos.count()
print(f"✅ Tabelas carregadas. Baseline de pedidos: {total_pedidos_original} linhas.")

df_features_final = construir_features_pedidos(df_pedidos, df_itens, df_produtos)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Validação pós-cruzamento
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
# MAGIC ## 3. Gravação na camada Gold
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
