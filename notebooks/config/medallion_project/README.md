# Projeto ecommerce_rastreamento - Bronze, Silver e Gold

Este entregavel reorganiza os notebooks do projeto em arquitetura medalhao:

- `00.config`: configuracao de credenciais, caminhos e schemas.
- `01.bronze`: ingestao dos arquivos parquet brutos para Delta, preservando os dados originais e metadados de auditoria.
- `02.silver`: limpeza, tipagem, padronizacao e deduplicacao da tabela de rastreamento.
- `03.gold`: tabelas agregadas para consumo analitico.
- `99.control`: notebook simples para executar as camadas em ordem.

Tambem foi adicionado controle por camada em JSON, no padrao:

```text
control/{camada}/{tabela}/checkpoint.json
```

Exemplos:

```text
control/bronze/ecommerce_rastreamento/checkpoint.json
control/silver/ecommerce_rastreamento/checkpoint.json
control/gold/ecommerce_rastreamento_status_diario/checkpoint.json
control/gold/ecommerce_rastreamento_pedido_ultima_posicao/checkpoint.json
```

## Ordem de execucao

1. `notebooks/00.config/feat_squad2_00_setup_config.py`
2. `notebooks/01.bronze/feat_squad2_ecommerce_rastreamento_bronze.py`
3. `notebooks/02.silver/feat_squad2_ecommerce_rastreamento_silver.py`
4. `notebooks/03.gold/feat_squad2_ecommerce_rastreamento_gold.py`

Ou execute:

```python
%run ./notebooks/99.control/run_ecommerce_rastreamento_medallion
```

## Variaveis esperadas

Configure no `.env`, secret scope ou variaveis do cluster:

- `ADLS_CLIENT_ID`
- `ADLS_TENANT_ID`
- `ADLS_CLIENT_SECRET`
- `STORAGE_ACCOUNT_NAME`

Valores padrao usados:

- container: `raw`
- raw: `real-time-data/`
- bronze: `squad2/bronze/ecommerce_rastreamento`
- silver: `squad2/silver/ecommerce_rastreamento`
- gold: `squad2/gold/ecommerce_rastreamento`
- quarantine: `squad2/quarantine/ecommerce_rastreamento_entregas`
- control: `control/{camada}/{tabela}/checkpoint.json`

## Regras da Kalita

Tabela: `ecommerce_rastreamento_entregas`

1. Schema completo em cada micro-lote.
2. `status_entrega` dentro do fluxo logistico permitido.
3. `dt_evento` nao pode ser nula nem futura.
4. Deduplicacao por `id_rastreamento`.
5. `id_pedido_ecommerce` deve existir na Silver de pedidos.
6. KPI de pedidos que entraram em `saiu para entrega` nas ultimas 2 horas.
7. KPI de percentual de pedidos entregues no prazo, com SLA de 7 dias.
8. Alerta para pedido com evento `entregue` mais de uma vez.
9. Top 3 transportadoras com mais eventos no micro-lote atual.
10. Alerta para pedido sem evento por mais de 3 dias apos `coletado`.
