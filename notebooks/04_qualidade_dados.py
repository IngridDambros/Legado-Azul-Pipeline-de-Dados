# Databricks notebook source
# MAGIC %md
# MAGIC # 04 - Qualidade de Dados
# MAGIC
# MAGIC Verificação sistemática de completude, consistência, unicidade, acurácia e
# MAGIC outliers, comparando a tabela Bronze (bruta) com a Silver (limpa), para
# MAGIC evidenciar os problemas encontrados e como foram tratados na etapa anterior.

# COMMAND ----------

from pyspark.sql import functions as F

CATALOGO = "workspace"
df_bronze = spark.table(f"{CATALOGO}.bronze.triagem_asd_raw")
df_silver = spark.table(f"{CATALOGO}.silver.triagem_asd_limpo")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1) Completude
# MAGIC Percentual de valores nulos (ou `"?"`) por coluna, antes e depois da limpeza.

# COMMAND ----------

total_bronze = df_bronze.count()

completude_bronze = df_bronze.select(
    [
        (
            F.count(F.when(F.col(c).isNull() | (F.trim(F.col(c).cast("string")) == "?"), c))
            / total_bronze * 100
        ).alias(c)
        for c in df_bronze.columns
        if c not in ("_dt_ingestao", "_arquivo_origem")
    ]
)
print("Percentual de valores ausentes por coluna (Bronze):")
completude_bronze.display()

total_silver = df_silver.count()
completude_silver = df_silver.select(
    [
        (F.count(F.when(F.col(c).isNull(), c)) / total_silver * 100).alias(c)
        for c in ["idade", "genero", "etnia", "quem_respondeu"]
    ]
)
print("Percentual de valores ausentes por coluna (Silver, apenas colunas que tinham '?'):")
completude_silver.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2) Consistência
# MAGIC Valores distintos de colunas categóricas — usado para identificar grafias
# MAGIC divergentes para a mesma categoria (ex.: "others" vs "Others").

# COMMAND ----------

print("Valores distintos de etnia na Bronze (antes da padronização):")
df_bronze.select("ethnicity").distinct().orderBy("ethnicity").display()

print("Valores distintos de etnia na Silver (depois da padronização):")
df_silver.select("etnia").distinct().orderBy("etnia").display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3) Unicidade
# MAGIC Verificação de linhas duplicadas (considerando todas as colunas originais).

# COMMAND ----------

colunas_originais = [c for c in df_bronze.columns if c not in ("_dt_ingestao", "_arquivo_origem")]
duplicatas = (
    df_bronze.groupBy(colunas_originais)
    .count()
    .filter("count > 1")
)
print(f"Grupos de linhas duplicadas encontrados na Bronze: {duplicatas.count()}")
duplicatas.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4) Acurácia
# MAGIC (a) Faixa plausível de idade; (b) consistência entre a soma das respostas
# MAGIC A1-A10 e a pontuação total informada.

# COMMAND ----------

print("Estatísticas da idade na Bronze (antes do tratamento):")
# A coluna `age` na Bronze ainda contém o valor "?" (dado faltante) misturado com
# números, então o Spark a trata como texto. Usamos `try_cast` para converter o
# que for possível para número e ignorar (retornar NULL) o que não for — assim
# evitamos o erro CAST_INVALID_INPUT que o modo ANSI SQL do Databricks lançaria
# com um cast comum.
df_bronze.selectExpr(
    "min(try_cast(age as double)) as idade_min",
    "max(try_cast(age as double)) as idade_max",
    "avg(try_cast(age as double)) as idade_media",
).display()

print("Estatísticas da idade na Silver (depois do tratamento):")
df_silver.select(F.min("idade").alias("idade_min"), F.max("idade").alias("idade_max"), F.avg("idade").alias("idade_media")).display()

print("Linhas em que a soma das respostas A1-A10 diverge da pontuação informada:")
df_silver.filter(F.col("pontuacao_inconsistente")).select(
    "soma_respostas_calculada", "pontuacao_teste"
).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5) Outliers
# MAGIC Distribuição da idade (Silver) para inspeção visual de valores extremos.

# COMMAND ----------

import matplotlib.pyplot as plt

pdf_idade = df_silver.select("idade").dropna().toPandas()
plt.figure(figsize=(6, 4))
plt.boxplot(pdf_idade["idade"])
plt.title("Distribuição de idade (Silver) - boxplot para detecção de outliers")
plt.ylabel("Idade")
plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Resumo dos problemas encontrados e tratamento aplicado
# MAGIC
# MAGIC | Problema encontrado | Dimensão de qualidade | Tratamento aplicado (Silver) |
# MAGIC |---|---|---|
# MAGIC | Valores `"?"` em `age`, `ethnicity`, `relation` | Completude | Convertidos para `NULL` |
# MAGIC | Grafias divergentes em `ethnicity` (ex.: "others"/"Others") | Consistência | Padronização de capitalização |
# MAGIC | Idades fora da faixa plausível (ex. valores > 100) | Acurácia | Convertidas para `NULL` |
# MAGIC | Nomes de coluna com erro de digitação (`austim`, `contry_of_res`) | Consistência/usabilidade | Renomeados na Silver |
# MAGIC | Coluna `Class/ASD` com caractere inválido para nome de coluna Delta | Estrutural | Renomeada para `Class_ASD` já na Bronze |
# MAGIC | Linhas duplicadas | Unicidade | Removidas com `dropDuplicates()` |
# MAGIC | Divergência entre soma das respostas e pontuação informada | Acurácia | Sinalizada em coluna `pontuacao_inconsistente` para análise |
# MAGIC
# MAGIC **Preencha após executar:** atualize esta tabela com os números reais obtidos
# MAGIC no seu ambiente (quantas linhas tinham cada problema) e inclua os prints dos
# MAGIC resultados acima no README.
