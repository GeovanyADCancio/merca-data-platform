# Projeto de Engenharia de Dados — Programa de Estágio

## Estrutura das Squads

O projeto será dividido em 3 squads:

### Squad 1 — Data Quality em Tempo Real

Responsável por validações, monitoramento, consistência e qualidade de dados do e-commerce em tempo real.

### Squad 2 — Tempo Real para Negócio

Responsável por processamento e análises em tempo real para o negócio utilizando dados do e-commerce.

### Squad 3 — Batch para Negócio

Responsável por processamento batch utilizando:

* dados do e-commerce
* dados das lojas físicas

---

# Bases de Dados

## E-commerce

```text
ecommerce_categorias.csv
ecommerce_clientes.csv
ecommerce_enderecos.csv
ecommerce_itens_pedido.csv
ecommerce_pedidos.csv
ecommerce_produtos.csv
ecommerce_rastreamento_entregas.csv
```

## Lojas Físicas

```text
physical_itens_venda_caixa.csv
physical_lojas.csv
physical_vendas_caixa.csv
```

---

# Regras Importantes

## Squads 1 e 2

As squads de tempo real NÃO devem utilizar dados físicos (`physical_*`).

Utilizar apenas dados do e-commerce.

---

## Squad 3

Pode utilizar:

* dados do e-commerce
* dados físicos (`physical_*`)

---

# Fluxo Git Obrigatório

## Regras Gerais

* NÃO realizar commits diretamente na `main`
* NÃO realizar commits diretamente na `dev`
* TODO desenvolvimento deve ocorrer em branch própria
* TODO merge deve ocorrer via Pull Request
* TODOS os Pull Requests devem ser aprovados por `@GeovanyADCancio`

---

# Fluxo Correto

## 1. Atualizar a branch dev

```bash
git checkout dev
git pull origin dev
```

---

## 2. Criar uma branch da feature

Padrão obrigatório:

```text
feat/<squad>/<tabela>/<descricao>
```

Exemplos:

```text
feat/squad1/ecommerce_pedidos/data-quality-pedidos
feat/squad1/ecommerce_clientes/validacao-clientes
feat/squad2/ecommerce_itens_pedido/streaming-itens
feat/squad3/physical_vendas_caixa/analise-batch-vendas
feat/squad3/ecommerce_produtos/consolidacao-produtos
```

Criando a branch:

```bash
git checkout -b feat/squad1/ecommerce_pedidos/data-quality-pedidos
```

---

# Padrão de Notebooks

Os notebooks devem seguir o mesmo padrão da branch.

Exemplos:

```text
feat_squad1_ecommerce_pedidos_data_quality.ipynb
feat_squad2_streaming_clientes.ipynb
feat_squad3_batch_vendas_fisicas.ipynb
```

---

# Realizando Commits

```bash
git add .
git commit -m "feat: implementa validacao de pedidos em tempo real"
```

---

# Enviando para o GitHub

```bash
git push origin feat/squad1/ecommerce_pedidos/data-quality-pedidos
```

---

# Abrindo Pull Request

O Pull Request deve ser aberto:

```text
feature branch -> dev
```

Exemplo:

```text
feat/squad1/ecommerce_pedidos/data-quality-pedidos -> dev
```

---

# Regras do Pull Request

* Adicionar `@GeovanyADCancio` como reviewer
* Explicar claramente e objetivamente:

  * objetivo
  * tabelas utilizadas
  * transformações realizadas
  * validações implementadas
* Resolver conflitos antes da aprovação

---

# Fluxo Final

```text
feature/* -> dev -> main
```

* Estagiários realizam merge apenas para `dev`
* Apenas mantenedores realizam merge de `dev` para `main`

---

# Boas Práticas

## Sempre antes de começar

```bash
git checkout dev
git pull origin dev
```

---

## Nunca utilizar

```bash
git push --force
```

---

## Commits devem ser pequenos e objetivos

Bom exemplo:

```text
feat: adiciona validacao de duplicidade em pedidos
```

Evitar:

```text
ajustes
teste
mudancas
```

---

# Organização Esperada

Cada squad deve manter:

* organização de pastas
* padronização de notebooks
* commits claros
* PRs pequenos e objetivos
* documentação mínima das transformações

---
