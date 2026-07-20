# Databricks notebook source
# MAGIC %md
# MAGIC # Interpretabilidade — Por que o modelo sinalizou (ou não) cada pedido
# MAGIC
# MAGIC Este notebook não retreina nem grava nada — é uma extensão de leitura sobre o resultado já
# MAGIC persistido pelo `feat_squad1_03_treinamento_isolation_forest`. Motivação: dois pedidos com
# MAGIC `desvio_valor_vs_historico` parecido (1,97 e 0,73) receberam decisões opostas (não-anômalo e
# MAGIC anômalo, respectivamente) — o Isolation Forest decide olhando as 18 features de uma vez, não uma
# MAGIC isoladamente, então a explicação não aparece numa tabela resumida de 6 colunas.
# MAGIC
# MAGIC O Isolation Forest não expõe uma importância de feature nativa (diferente de florestas
# MAGIC supervisionadas). Em vez de introduzir uma biblioteca nova (ex: SHAP) só para uma checagem
# MAGIC pontual, este notebook usa uma aproximação simples e transparente: o desvio-padrão de cada
# MAGIC feature em relação à população inteira de 50 pedidos — quanto maior o `|z-score|`, mais aquele
# MAGIC valor específico se destaca do restante da base nessa dimensão.

# COMMAND ----------

# MAGIC %pip install scikit-learn

# COMMAND ----------

# MAGIC %run ../99_utils/feat_squad1_99_helpers

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Reconstrução do X (mesmo processo do treino)
# MAGIC
# MAGIC Repete exatamente as Seções 1–3 do notebook de treino, para garantir que os valores aqui
# MAGIC batem com o que o modelo realmente viu — nenhuma transformação nova é introduzida.

# COMMAND ----------

df_features = ler_delta("gold", "features_pedidos")
pdf = df_features.toPandas()

VALORES_NULOS_PADRAO = {
    "ticket_medio_historico_cliente": 0.0,
    "desvio_padrao_historico_cliente": 0.0,
    "dias_desde_ultima_compra": 0,
    "desvio_valor_vs_historico": 0.0,
    "desvio_pct_vs_media_cliente": 0.0,
    "qtd_linhas_itens": 0,
    "qtd_unidades_total": 0,
    "qtd_categorias_distintas": 0,
    "ticket_medio_por_unidade": 0.0,
    "valor_itens_calculado": 0.0,
    "diferenca_valor_itens_vs_pedido": 0.0,
}
pdf = pdf.fillna(VALORES_NULOS_PADRAO)

COLUNAS_MODELO = [
    "valor_total", "valor_frete", "razao_frete_valor",
    "hora_do_pedido", "dia_semana_pedido",
    "ticket_medio_historico_cliente", "desvio_padrao_historico_cliente",
    "qtd_pedidos_historico_cliente", "dias_desde_ultima_compra",
    "qtd_pedidos_ultimos_30_dias", "desvio_valor_vs_historico", "desvio_pct_vs_media_cliente",
    "qtd_linhas_itens", "qtd_unidades_total", "qtd_categorias_distintas",
    "ticket_medio_por_unidade", "valor_itens_calculado", "diferenca_valor_itens_vs_pedido",
]

X = pdf[COLUNAS_MODELO]
pdf = pdf.set_index("id_pedido")
X.index = pdf.index

print(f"X reconstruído: {X.shape[0]} pedidos × {X.shape[1]} features — confira se bate com o treino.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Carregar modelo e resultado já persistidos
# MAGIC
# MAGIC `carregar_modelo` é a mesma função que o notebook de score em dados novos vai usar — testá-la
# MAGIC aqui já valida que ela funciona antes desse próximo card depender dela.

# COMMAND ----------

modelo = carregar_modelo("modelos/isolation_forest_pedidos.pkl")
df_resultado = ler_delta("gold", "anomalias_pedidos").toPandas().set_index("id_pedido")

print("Modelo e resultado carregados com sucesso.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Comparação lado a lado: os 3 flagados + 1 contraponto
# MAGIC
# MAGIC O pedido `1784211728` não foi sinalizado apesar de ter `desvio_valor_vs_historico` (1,97) maior
# MAGIC que o do `1784209685` (0,73, sinalizado) — comparar os vetores completos mostra o que mais pesou.

# COMMAND ----------

PEDIDOS_INSPECIONAR = [1784231645, 1783780201, 1784209685, 1784211728]

comparacao = X.loc[PEDIDOS_INSPECIONAR].T
comparacao.columns = [f"{pid} ({'ANÔMALO' if df_resultado.loc[pid, 'flag_anomalia'] else 'normal'})" for pid in PEDIDOS_INSPECIONAR]

print("📊 Vetor de features completo dos 4 pedidos:")
display(comparacao)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Quais features mais se destacam em cada pedido
# MAGIC
# MAGIC Para cada uma das 18 features, calcula-se o z-score do pedido em relação à média e ao
# MAGIC desvio-padrão da população inteira (50 pedidos) — não em relação ao histórico do próprio
# MAGIC cliente (isso já é outra feature, `desvio_valor_vs_historico`). Aqui o que se mede é
# MAGIC "o quão fora do comum geral" cada valor está, feature por feature.

# COMMAND ----------

medias = X.mean()
desvios = X.std().replace(0, 1)  # evita divisão por zero em colunas constantes

z_scores = (X.loc[PEDIDOS_INSPECIONAR] - medias) / desvios

for pid in PEDIDOS_INSPECIONAR:
    flag = "🔴 ANÔMALO" if df_resultado.loc[pid, "flag_anomalia"] else "⚪ normal"
    top3 = z_scores.loc[pid].abs().sort_values(ascending=False).head(3)
    print(f"\nPedido {pid} — {flag} (score: {df_resultado.loc[pid, 'score_anomalia']:.3f})")
    print("  Features que mais se destacam da população (|z-score|):")
    for feature, valor_z in top3.items():
        valor_real = X.loc[pid, feature]
        print(f"    {feature:35s} valor={valor_real:>10.2f}  z-score={z_scores.loc[pid, feature]:+.2f}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Leitura dos resultados
# MAGIC
# MAGIC Compare o que aparece no topo de cada pedido: se os 3 anômalos consistentemente têm pelo menos
# MAGIC uma feature com `|z-score|` bem alto (acima de ~2) e o pedido normal (`1784211728`) não, isso
# MAGIC confirma que o modelo está reagindo a sinais legítimos, não a ruído. Se o padrão não for claro,
# MAGIC é sinal de que o volume atual (50 pedidos) ainda é pequeno demais para o modelo diferenciar bem
# MAGIC entre "fora do comum" e "coincidência" — reforça o aviso já registrado no notebook de treino.
