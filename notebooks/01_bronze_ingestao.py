# Databricks notebook source
# MAGIC %md
# MAGIC # 01 - Bronze: Ingestão dos dados brutos
# MAGIC
# MAGIC **Objetivo:** trazer o arquivo `autism_screening_adult.csv` (dataset "Autism Screening Adult",
# MAGIC UCI ML Repository / Fadi Thabtah, 2017, licença CC BY 4.0) para dentro do Databricks
# MAGIC exatamente como ele chegou, sem nenhuma limpeza de conteúdo, apenas com metadados de
# MAGIC controle (data de ingestão e arquivo de origem). Essa é a camada Bronze da Arquitetura
# MAGIC Medalhão: o "cofre de evidências" que preserva o dado original para rastreabilidade.
# MAGIC
# MAGIC **Antes de rodar:**
# MAGIC 1. Baixe o CSV do Kaggle (ex: `andrewmvd/autism-screening-on-adults` ou
# MAGIC    `faizunnabi/autism-screening`) ou do UCI ML Repository (dataset 426).
# MAGIC 2. Faça upload do arquivo para um Volume do Unity Catalog (recomendado) ou para
# MAGIC    `/Volumes/workspace/default/raw_files/` — ajuste o caminho abaixo conforme onde
# MAGIC    você salvou o arquivo.

# COMMAND ----------

from pyspark.sql import functions as F

# Ajuste este caminho para onde você fez upload do arquivo no seu workspace
CAMINHO_ARQUIVO_ORIGEM = "/Volumes/workspace/default/raw_files/autism_screening_adult.csv"
NOME_ARQUIVO_ORIGEM = "autism_screening_adult.csv"

# Catálogo/esquema de destino (crie previamente via UI ou SQL: CREATE SCHEMA bronze)
CATALOGO = "workspace"
ESQUEMA_BRONZE = "bronze"
TABELA_BRONZE = f"{CATALOGO}.{ESQUEMA_BRONZE}.triagem_asd_raw"

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE SCHEMA IF NOT EXISTS workspace.bronze;

# COMMAND ----------

# Leitura do CSV bruto, mantendo os dados exatamente como vieram (inferSchema apenas
# para não perder tipos óbvios, mas sem qualquer tratamento de conteúdo)
df_bruto = (
    spark.read
    .option("header", True)
    .option("inferSchema", True)
    .csv(CAMINHO_ARQUIVO_ORIGEM)
)

print(f"Linhas lidas: {df_bruto.count()}")
print(f"Colunas: {df_bruto.columns}")
df_bruto.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Ajuste técnico mínimo (não é limpeza de conteúdo)
# MAGIC A coluna original `Class/ASD` contém uma barra `/`, caractere não permitido em nomes
# MAGIC de coluna Delta/Unity Catalog. Renomeamos apenas o **nome** da coluna para
# MAGIC `Class_ASD`, sem alterar nenhum valor. Todas as demais colunas permanecem com o
# MAGIC nome e os valores exatamente como recebidos (incluindo os erros de digitação
# MAGIC originais do dataset, como `austim` e `contry_of_res`, e os valores `?` usados
# MAGIC como marcador de dado faltante).

# COMMAND ----------

if "Class/ASD" in df_bruto.columns:
    df_bruto = df_bruto.withColumnRenamed("Class/ASD", "Class_ASD")

# Metadados de controle da camada Bronze
df_bronze = (
    df_bruto
    .withColumn("_dt_ingestao", F.current_timestamp())
    .withColumn("_arquivo_origem", F.lit(NOME_ARQUIVO_ORIGEM))
)

df_bronze.display()

# COMMAND ----------

(
    df_bronze.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(TABELA_BRONZE)
)

print(f"Tabela Bronze salva em: {TABELA_BRONZE}")
print(f"Total de registros: {spark.table(TABELA_BRONZE).count()}")

# COMMAND ----------

# MAGIC %md
# MAGIC **Evidência para o README:** tire um print do resultado do `display()` acima e
# MAGIC outro da tabela `workspace.bronze.triagem_asd_raw` aparecendo no Catalog Explorer
# MAGIC (menu lateral "Catalog"), comprovando que os dados foram persistidos na nuvem.
