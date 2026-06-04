# Ingestão Batch — Ecommerce Itens Pedido

## Objetivo

Este notebook tem como objetivo realizar a ingestão batch do arquivo `ecommerce_itens_pedido.csv`, disponível no Azure Data Lake, validando sua estrutura antes da gravação no banco de dados SQL Server.

O processo garante que apenas arquivos com o contrato de colunas correto sejam carregados na tabela destino, evitando propagação de dados inconsistentes, colunas extras, colunas ausentes ou alteração indevida na ordem do schema.

---

## Contexto do Projeto

Este desenvolvimento faz parte do **Projeto de Engenharia de Dados — Programa de Estágio**.

O projeto está organizado em três squads:

| Squad | Responsabilidade |
|---|---|
| Squad 1 | Data Quality em tempo real |
| Squad 2 | Processamento em tempo real para negócio |
| Squad 3 | Processamento batch para negócio |

Este notebook pertence à:


Squad 3 — Batch para Negócio