# Databricks notebook source
# MAGIC %md
# MAGIC ### Diagnóstico do Data Lake — Squad 1 (Data Quality)
# MAGIC
# MAGIC > Este notebook NÃO cria, altera nem grava nada. Ele só **lista** o que já existe no Data Lake,
# MAGIC > pra decidirmos com segurança de onde partir na Engenharia de Features.
# MAGIC
# MAGIC **Por que isso é necessário antes de qualquer código de feature:**
# MAGIC 1. Não sabemos ainda se a Squad 1 já tem as camadas Bronze/Silver estruturadas para todas as
# MAGIC    tabelas (pedidos, itens_pedidos, produtos, categorias, clientes), ou só para algumas.
# MAGIC 2. Escrever código de feature engineering assumindo uma tabela Silver que não existe geraria erro
# MAGIC    ou, pior, nos faria recalcular do zero algo que um colega da squad já fez.
# MAGIC 3. Este notebook segue o mesmo padrão de conexão usado pela Squad 2 (`azure-storage-file-datalake`
# MAGIC    + `azure-identity`), então não é necessário instalar nada novo.
# MAGIC
# MAGIC **Como usar:** rode célula por célula e leia os prints. Ao final, você terá clareza sobre qual dos
# MAGIC cenários se aplica: (a) tudo já pronto, (b) só pedidos pronto, (c) nada estruturado ainda.

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1. Carregamento de credenciais (.env)
# MAGIC
# MAGIC > Reaproveita exatamente o mesmo `.env` que vocês já configuraram para a Squad 2 — as credenciais
# MAGIC > do ADLS são as mesmas para todas as squads, o que muda é apenas o *container* de cada squad.

# COMMAND ----------

import os
from dotenv import load_dotenv

caminho_env = None
for tentativa in [".env", "../.env", "../../.env"]:
    if os.path.exists(tentativa):
        caminho_env = tentativa
        break

if caminho_env:
    load_dotenv(dotenv_path=caminho_env)
    print(f"✅ .env carregado a partir de: '{caminho_env}'")
else:
    raise FileNotFoundError("⚠️ Arquivo .env não localizado. Confirme se ele está na raiz do projeto.")

ADLS_CLIENT_ID = os.getenv("ADLS_CLIENT_ID")
ADLS_TENANT_ID = os.getenv("ADLS_TENANT_ID")
ADLS_CLIENT_SECRET = os.getenv("ADLS_CLIENT_SECRET")
STORAGE_ACCOUNT = "internshipdatalake"  # mesmo valor usado no setup da Squad 2

if not all([ADLS_CLIENT_ID, ADLS_TENANT_ID, ADLS_CLIENT_SECRET]):
    raise ValueError("⚠️ Credenciais ausentes no .env. Verifique ADLS_CLIENT_ID / ADLS_TENANT_ID / ADLS_CLIENT_SECRET.")

print("🔒 Credenciais mapeadas com sucesso (valores não são exibidos por segurança).")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2. Quais containers (áreas) existem nessa Storage Account?
# MAGIC
# MAGIC > Cada squad tem, em tese, seu próprio container (ex: `squad2`, análogo ao que os helpers da Squad 2
# MAGIC > usam). Aqui listamos TODOS os containers disponíveis, pra confirmar se um `squad1` (ou nome parecido)
# MAGIC > já foi criado por alguém da sua squad.

# COMMAND ----------

from azure.identity import ClientSecretCredential
from azure.storage.filedatalake import DataLakeServiceClient

credential = ClientSecretCredential(
    tenant_id=ADLS_TENANT_ID,
    client_id=ADLS_CLIENT_ID,
    client_secret=ADLS_CLIENT_SECRET,
)

service_client = DataLakeServiceClient(
    account_url=f"https://{STORAGE_ACCOUNT}.dfs.core.windows.net",
    credential=credential,
)

print("📦 Containers disponíveis nesta Storage Account:\n")
containers_encontrados = []
for fs in service_client.list_file_systems():
    containers_encontrados.append(fs.name)
    print(f" - {fs.name}")

print("\n👉 Procure por algo como 'squad1' na lista acima. Se não aparecer nada parecido,")
print("   isso já responde a pergunta: ainda não existe uma área dedicada da Squad 1.")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3. O que existe dentro do container da Squad 1?
# MAGIC
# MAGIC > Ajuste `CONTAINER_SQUAD1` abaixo com o nome exato que apareceu na célula anterior
# MAGIC > (o padrão mais provável, seguindo a Squad 2, é `"squad1"`).

# COMMAND ----------

CONTAINER_SQUAD1 = "squad1"  # <-- ajuste aqui se o nome real for diferente

if CONTAINER_SQUAD1 not in containers_encontrados:
    print(f"⚠️ O container '{CONTAINER_SQUAD1}' não foi encontrado na lista da célula anterior.")
    print("   Isso indica que a estruturação em Delta (Bronze/Silver) para a Squad 1 provavelmente")
    print("   ainda não foi iniciada por ninguém — o que é uma resposta válida e importante de se ter.")
else:
    fs_client = service_client.get_file_system_client(CONTAINER_SQUAD1)
    print(f"📂 Conteúdo de nível superior do container '{CONTAINER_SQUAD1}':\n")
    for path in fs_client.get_paths(recursive=False):
        tipo = "📁 pasta" if path.is_directory else "📄 arquivo"
        print(f" - {tipo}: {path.name}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4. Quais tabelas já existem em cada camada (Bronze / Silver)?
# MAGIC
# MAGIC > Aqui verificamos, tabela por tabela, se já existe uma pasta correspondente em `bronze/` e `silver/`.
# MAGIC > Isso nos dá o mapa exato de onde a Engenharia de Features pode (ou não pode) começar.

# COMMAND ----------

TABELAS_ALVO = [
    "ecommerce_pedidos",
    "ecommerce_itens_pedido",
    "ecommerce_produtos",
    "ecommerce_categorias",
    "ecommerce_clientes",
]

if CONTAINER_SQUAD1 in containers_encontrados:
    for camada in ["bronze", "silver"]:
        print(f"\n🔍 Camada '{camada}':")
        try:
            pastas_camada = {
                p.name.split("/")[-1] for p in fs_client.get_paths(path=camada, recursive=False)
            }
        except Exception:
            pastas_camada = set()

        if not pastas_camada:
            print(f"   (vazio ou inexistente — nenhuma tabela estruturada em '{camada}/' ainda)")
            continue

        for tabela in TABELAS_ALVO:
            status = "✅ existe" if tabela in pastas_camada else "❌ não existe"
            print(f"   {status:12s} — {tabela}")
else:
    print("⏩ Pulado: o container da Squad 1 ainda não existe (ver célula anterior).")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 5. Resumo — o que fazer com esse resultado
# MAGIC
# MAGIC * Se **tudo apareceu como "✅ existe" na Silver** → a Engenharia de Features pode começar direto,
# MAGIC   lendo essas tabelas Silver e cruzando-as.
# MAGIC * Se **só `ecommerce_pedidos` existe** → precisamos primeiro estruturar Bronze/Silver das outras
# MAGIC   tabelas (replicando o padrão que a Squad 2 já usou), antes de calcular features que dependem delas.
# MAGIC * Se **nada existe (container nem apareceu)** → a Squad 1 ainda está no ponto de partida da
# MAGIC   engenharia de dados, e o card de Feature Engineering precisa esperar essa base ser montada —
# MAGIC   vale alinhar isso com o Lead, porque pode mudar a ordem de prioridade das tarefas.
