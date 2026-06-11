# Regras da Kalita - ecommerce_rastreamento_entregas

Arquivo base: `regras_estagio_engdados - squad2.xlsx`

## Tecnicas

1. Schema completo em cada micro-lote
   - Implementado na Bronze.
   - O notebook valida as colunas obrigatorias antes de gravar Delta.

2. `status_entrega` dentro do fluxo logistico
   - Implementado na Silver.
   - Status fora da lista permitida vai para quarentena.

3. `dt_evento` nao nula nem futura
   - Implementado na Silver.
   - Registros invalidos vao para quarentena.

4. Deduplicacao por `id_rastreamento`
   - Implementado na Silver.
   - Mantem o registro mais recente por `dt_evento` e `bronze_ingested_at`.

5. `id_pedido_ecommerce` com correspondencia na Silver de pedidos
   - Implementado na Silver.
   - Valida contra `silver.ecommerce_pedidos`.
   - Pedido sem correspondencia vai para quarentena.

## Negocio

6. Quantidade de pedidos que entraram em `saiu para entrega` nas ultimas 2 horas
   - Implementado na Gold.
   - Tabela: `gold.ecommerce_rastreamento_saiu_para_entrega_2h`.

7. Percentual de pedidos entregues no prazo
   - Implementado na Gold.
   - SLA configurado: 7 dias.
   - Tabela: `gold.ecommerce_rastreamento_sla_entrega`.

8. Alerta para pedido com evento `entregue` mais de uma vez
   - Implementado na Gold.
   - Tabela: `gold.ecommerce_rastreamento_alerta_entrega_duplicada`.

9. Top 3 transportadoras com mais eventos no micro-lote atual
   - Implementado na Gold.
   - Tabela: `gold.ecommerce_rastreamento_top3_transportadoras_micro_lote`.

10. Alerta se pedido ficar mais de 3 dias sem evento apos `coletado`
    - Implementado na Gold.
    - Tabela: `gold.ecommerce_rastreamento_alerta_sem_evento_apos_coleta`.

## Controle

Cada etapa salva checkpoint JSON no padrao combinado pelo grupo:

```text
control/{camada}/{tabela}/checkpoint.json
```

Exemplos:

```text
control/bronze/ecommerce_rastreamento/checkpoint.json
control/silver/ecommerce_rastreamento/checkpoint.json
control/gold/ecommerce_rastreamento_sla_entrega/checkpoint.json
```

