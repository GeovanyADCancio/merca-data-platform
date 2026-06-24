# Databricks notebook source
################################################
### Instalando bibliotecas para autenticacao ###
################################################
%pip install azure-identity==1.20.0 azure-storage-file-datalake==12.22.0 python-dotenv==1.2.1


##########################
### Reiniciando Python ###
##########################
dbutils.library.restartPython()


#############################
### Importando Bibliotecas ###
#############################
from pyspark.sql.functions import col
from azure.identity import ClientSecretCredential
from azure.storage.filedatalake import DataLakeServiceClient
from dotenv import load_dotenv

import os
import io
import pandas as pd
import re


################################################################
### Configuracoes da Ingestao                                ###
################################################################
nome_arquivo_csv = "ecommerce_pedidos.csv"

schema_destino = "squad3"
tabela_destino = "ecommerce_pedidos"
tabela_completa = f"{schema_destino}.{tabela_destino}"

colunas_esperadas = [
    "id_pedido",
    "id_cliente",
    "dt_pedido",
    "status_pedido",
    "canal_venda",
    "forma_pagamento",
    "valor_frete"
]

ddl_colunas_sqlserver = """
    id_pedido VARCHAR(255),
    id_cliente VARCHAR(255),
    dt_pedido VARCHAR(255),
    status_pedido VARCHAR(255),
    canal_venda VARCHAR(255),
    forma_pagamento VARCHAR(255),
    valor_frete VARCHAR(255),
    dt_carga DATETIME2 NOT NULL DEFAULT SYSDATETIME()
"""


#########################################
### Carregando Variaveis de Ambiente  ###
#########################################
load_dotenv("env")
load_dotenv("../env")
load_dotenv(".env")


################################
### Instanciando Credenciais ###
################################
# Azure
client_id = os.getenv("CLIENT_ID")
tenant_id = os.getenv("TENANT_ID")
client_secret = os.getenv("CLIENT_SECRET")
storage_account_name = os.getenv("STORAGE_ACCOUNT_NAME")
container_name = os.getenv("CONTAINER_NAME")

# SQL Server
sql_host = os.getenv("SQL_HOST")
sql_database = os.getenv("SQL_DATABASE")
sql_username = os.getenv("SQL_USERNAME")
sql_password = os.getenv("SQL_PASSWORD")

variaveis_obrigatorias = {
    "CLIENT_ID": client_id,
    "TENANT_ID": tenant_id,
    "CLIENT_SECRET": client_secret,
    "STORAGE_ACCOUNT_NAME": storage_account_name,
    "CONTAINER_NAME": container_name,
    "SQL_HOST": sql_host,
    "SQL_DATABASE": sql_database,
    "SQL_USERNAME": sql_username,
    "SQL_PASSWORD": sql_password
}

variaveis_ausentes = [
    nome for nome, valor in variaveis_obrigatorias.items()
    if valor is None or str(valor).strip() == ""
]

if variaveis_ausentes:
    raise ValueError(f"Variaveis de ambiente ausentes no .env: {variaveis_ausentes}")

print("Variaveis de ambiente carregadas com sucesso.")


###################################
### Conexao com Azure Data Lake ###
###################################
credential = ClientSecretCredential(
    tenant_id=tenant_id,
    client_id=client_id,
    client_secret=client_secret
)

service_client = DataLakeServiceClient(
    account_url=f"https://{storage_account_name}.dfs.core.windows.net",
    credential=credential
)

file_system_client = service_client.get_file_system_client(
    file_system=container_name
)

print("Conexao com Azure Data Lake criada com sucesso.")


################################################################
### Funcoes Auxiliares                                       ###
################################################################
def normalizar_nome_coluna(nome_coluna):
    return re.sub(
        r"\W+",
        "",
        nome_coluna.lower().strip().replace(" ", "_")
    )


def ler_csv_datalake_como_spark_df(file_system_client, nome_arquivo_csv):
    print(f"Lendo arquivo do Data Lake: {nome_arquivo_csv}")

    file_client = file_system_client.get_file_client(nome_arquivo_csv)
    dados = file_client.download_file().readall()

    pdf = pd.read_csv(io.BytesIO(dados))

    pdf.columns = [
        normalizar_nome_coluna(c)
        for c in pdf.columns
    ]

    df = spark.createDataFrame(pdf)

    df = df.select([
        col(c).cast("string").alias(c)
        for c in df.columns
    ])

    return df


def validar_contrato_colunas(df, colunas_esperadas):
    colunas_atuais = df.columns

    if len(colunas_atuais) != len(colunas_esperadas):
        raise ValueError(
            f"ERRO CRITICO: Quantidade de colunas incorreta. "
            f"O arquivo possui {len(colunas_atuais)} colunas, mas o esperado eram {len(colunas_esperadas)}. "
            f"Colunas atuais: {colunas_atuais}"
        )

    colunas_faltantes = set(colunas_esperadas) - set(colunas_atuais)
    colunas_extras = set(colunas_atuais) - set(colunas_esperadas)

    if colunas_faltantes or colunas_extras:
        mensagem_erro = "ERRO CRITICO: As colunas do arquivo nao correspondem ao esquema esperado.\n"

        if colunas_faltantes:
            mensagem_erro += f" -> Colunas FALTANDO: {list(colunas_faltantes)}\n"

        if colunas_extras:
            mensagem_erro += f" -> Colunas EXTRAS: {list(colunas_extras)}\n"

        raise ValueError(mensagem_erro)

    if colunas_atuais != colunas_esperadas:
        raise ValueError(
            f"ERRO CRITICO: A ordem das colunas esta incorreta.\n"
            f"Esperado: {colunas_esperadas}\n"
            f"Recebido: {colunas_atuais}"
        )

    print("Validacao estrutural OK: quantidade, nomes e ordem das colunas estao corretos.")


def executar_sqlserver_ddl(jdbc_url, user, password, sql):
    try:
        (
            spark.read
            .format("jdbc")
            .option("url", jdbc_url)
            .option("user", user)
            .option("password", password)
            .option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver")
            .option("sessionInitStatement", sql)
            .option("query", "SELECT 1 AS comando_executado")
            .load()
            .collect()
        )

    except Exception as e:
        print(f"Erro ao executar comando no SQL Server: {e}")
        raise e


def criar_schema_se_nao_existir(schema_destino):
    print(f"Verificando a existencia do schema '{schema_destino}' no SQL Server...")

    query_schema = f"""
    IF NOT EXISTS (
        SELECT 1
        FROM sys.schemas
        WHERE name = '{schema_destino}'
    )
    BEGIN
        EXEC('CREATE SCHEMA [{schema_destino}]');
    END;
    """

    executar_sqlserver_ddl(
        jdbc_url=jdbc_url,
        user=sql_username,
        password=sql_password,
        sql=query_schema
    )

    print(f"Schema '{schema_destino}' validado/criado com sucesso.")


def criar_tabela_se_nao_existir(tabela_completa, ddl_colunas_sqlserver):
    print(f"Verificando a existencia da tabela '{tabela_completa}'...")

    query_create_table = f"""
    IF OBJECT_ID('{tabela_completa}', 'U') IS NULL
    BEGIN
        CREATE TABLE {tabela_completa} (
            {ddl_colunas_sqlserver}
        );
    END;
    """

    executar_sqlserver_ddl(
        jdbc_url=jdbc_url,
        user=sql_username,
        password=sql_password,
        sql=query_create_table
    )

    print(f"Tabela {tabela_completa} validada/criada com sucesso.")


def limpar_tabela(tabela_completa):
    print(f"Limpando a tabela {tabela_completa}...")

    query_truncate = f"""
    TRUNCATE TABLE {tabela_completa};
    """

    executar_sqlserver_ddl(
        jdbc_url=jdbc_url,
        user=sql_username,
        password=sql_password,
        sql=query_truncate
    )

    print(f"Tabela {tabela_completa} limpa com sucesso.")


def gravar_sqlserver(df, tabela_completa):
    print(f"Escrevendo a tabela {tabela_completa}...")

    df.write \
        .format("sqlserver") \
        .mode("append") \
        .option("host", sql_host) \
        .option("port", "1433") \
        .option("database", sql_database) \
        .option("dbtable", tabela_completa) \
        .option("user", sql_username) \
        .option("password", sql_password) \
        .option("encrypt", "true") \
        .option("trustServerCertificate", "false") \
        .option("batchsize", "5000") \
        .save()

    print(f"Tabela {tabela_completa} finalizada com sucesso.")


def ler_tabela_sqlserver(tabela_completa):
    df_sql = spark.read.jdbc(
        url=jdbc_url,
        table=tabela_completa,
        properties=connection_properties
    )

    return df_sql


def validar_volumetria(df_origem, df_destino, nome_tabela):
    qtd_origem = df_origem.count()
    qtd_destino = df_destino.count()

    print("--- VOLUMETRIA: DATA LAKE / ORIGEM ---")
    print(f"{nome_tabela}: {qtd_origem}")

    print("\n--- VOLUMETRIA: SQL SERVER / DESTINO ---")
    print(f"{nome_tabela}: {qtd_destino}")

    print("\n--- STATUS DA RECONCILIACAO ---")

    if qtd_origem == qtd_destino:
        print(f"{nome_tabela}: SUCESSO - Quantidade de linhas reconciliada.")
    else:
        raise ValueError(
            f"ERRO DE RECONCILIACAO: Quantidade divergente. "
            f"Origem: {qtd_origem} | Destino: {qtd_destino}"
        )


################################################################
### Configuracao de Conexao JDBC com SQL Server               ###
################################################################
jdbc_url = (
    f"jdbc:sqlserver://{sql_host}:1433;"
    f"databaseName={sql_database};"
    "encrypt=true;"
    "trustServerCertificate=false;"
    "loginTimeout=30;"
)

connection_properties = {
    "user": sql_username,
    "password": sql_password,
    "driver": "com.microsoft.sqlserver.jdbc.SQLServerDriver"
}

print("JDBC URL configurada.")


################################################################
### Teste de Conexao com SQL Server                          ###
################################################################
query_teste = "(SELECT 1 AS conexao_ok) AS teste"

df_teste_conexao = spark.read.jdbc(
    url=jdbc_url,
    table=query_teste,
    properties=connection_properties
)

display(df_teste_conexao)

print("Conexao Spark -> SQL Server bem-sucedida.")


################################################################
### Execucao da Ingestao                                     ###
################################################################
df_origem = ler_csv_datalake_como_spark_df(
    file_system_client=file_system_client,
    nome_arquivo_csv=nome_arquivo_csv
)

display(df_origem)

validar_contrato_colunas(
    df=df_origem,
    colunas_esperadas=colunas_esperadas
)

df_write = df_origem.select(*colunas_esperadas)

criar_schema_se_nao_existir(schema_destino)

criar_tabela_se_nao_existir(
    tabela_completa=tabela_completa,
    ddl_colunas_sqlserver=ddl_colunas_sqlserver
)

limpar_tabela(tabela_completa)

gravar_sqlserver(
    df=df_write,
    tabela_completa=tabela_completa
)

df_destino = ler_tabela_sqlserver(tabela_completa)

display(df_destino)

validar_volumetria(
    df_origem=df_write,
    df_destino=df_destino,
    nome_tabela=tabela_destino
)

