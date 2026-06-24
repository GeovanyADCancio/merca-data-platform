# Ingestao Batch - Ecommerce Pedidos

## Objetivo

Este notebook realiza a ingestao batch do arquivo `ecommerce_pedidos.csv`, disponivel no Azure Data Lake, validando sua estrutura antes da gravacao no banco de dados SQL Server.

O processo garante que apenas arquivos com o contrato de colunas correto sejam carregados na tabela destino, evitando propagacao de dados inconsistentes, colunas extras, colunas ausentes ou alteracao indevida na ordem do schema.

---

## Contexto do Projeto

Este desenvolvimento faz parte do **Projeto de Engenharia de Dados - Programa de Estagio**.

O projeto esta organizado em tres squads:

| Squad | Responsabilidade |
|---|---|
| Squad 1 | Data Quality em tempo real |
| Squad 2 | Processamento em tempo real para negocio |
| Squad 3 | Processamento batch para negocio |

Este notebook pertence a:

Squad 3 - Batch para Negocio
