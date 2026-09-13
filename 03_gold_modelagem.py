# Databricks notebook source
# MAGIC %md
# MAGIC # 03 - Gold: Modelagem em Esquema Estrela
# MAGIC
# MAGIC **Objetivo:** transformar a tabela Silver (uma única tabela larga) em um
# MAGIC Esquema Estrela: uma tabela fato (`Fato_Triagem`) cercada de tabelas dimensão
# MAGIC (`Dimensao_Demografia`, `Dimensao_Saude_Familiar`, `Dimensao_Localizacao`,
# MAGIC `Dimensao_Contexto_Teste`). Isso organiza os dados de forma que consultas
# MAGIC analíticas (Etapa 4.5) fiquem simples e eficientes.
# MAGIC
# MAGIC **Grão da tabela fato:** uma linha = uma triagem (um respondente que fez o teste).
# MAGIC
# MAGIC **Nota sobre escala:** para gerar as chaves substitutas (surrogate keys) das
# MAGIC dimensões, usamos `row_number()` sobre uma janela sem partição, o que é adequado
# MAGIC para o volume deste MVP (704 registros, dimensões com poucas dezenas de valores
# MAGIC distintos). Em um cenário de produção com dimensões muito maiores, essa
# MAGIC abordagem não seria escalável (todo o cálculo roda em uma única partição); o
# MAGIC caminho mais robusto seria usar `IDENTITY` columns do Delta Lake ou
# MAGIC `monotonically_increasing_id()` combinado a uma tabela de-para persistida.

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.window import Window

CATALOGO = "workspace"
TABELA_SILVER = f"{CATALOGO}.silver.triagem_asd_limpo"
ESQUEMA_GOLD = f"{CATALOGO}.gold"

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {ESQUEMA_GOLD}")

df = spark.table(TABELA_SILVER)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Dimensão: Demografia (idade e gênero)

# COMMAND ----------

dim_demografia = (
    df.select("idade", "genero").distinct()
    .withColumn(
        "faixa_etaria",
        F.when(F.col("idade").isNull(), "Não informado")
         .when(F.col("idade") < 25, "18-24")
         .when(F.col("idade") < 35, "25-34")
         .when(F.col("idade") < 45, "35-44")
         .when(F.col("idade") < 55, "45-54")
         .otherwise("55+"),
    )
    .withColumn("id_demografia", F.row_number().over(Window.orderBy("idade", "genero")))
    .select("id_demografia", "idade", "genero", "faixa_etaria")
)

dim_demografia.write.format("delta").mode("overwrite").option("overwriteSchema", "true") \
    .saveAsTable(f"{ESQUEMA_GOLD}.Dimensao_Demografia")

# Relemos a tabela recém-salva (em vez de seguir usando o DataFrame em memória).
# Isso "quebra" a linhagem compartilhada com `df`: como dim_demografia foi criada
# a partir de transformações sobre o próprio `df`, o Spark pode não conseguir
# distinguir a coluna `idade` de um lado e de outro quando os dois forem juntados
# mais abaixo (erro AMBIGUOUS_COLUMN_REFERENCE). Ler de volta do Delta resolve isso.
dim_demografia = spark.table(f"{ESQUEMA_GOLD}.Dimensao_Demografia")
dim_demografia.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Dimensão: Saúde familiar (icterícia ao nascer e histórico familiar de autismo)

# COMMAND ----------

dim_saude = (
    df.select("nasceu_com_icterica", "possui_parente_autista").distinct()
    .withColumn("id_saude_familiar", F.row_number().over(Window.orderBy("nasceu_com_icterica", "possui_parente_autista")))
    .select("id_saude_familiar", "nasceu_com_icterica", "possui_parente_autista")
)

dim_saude.write.format("delta").mode("overwrite").option("overwriteSchema", "true") \
    .saveAsTable(f"{ESQUEMA_GOLD}.Dimensao_Saude_Familiar")

dim_saude = spark.table(f"{ESQUEMA_GOLD}.Dimensao_Saude_Familiar")
dim_saude.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Dimensão: Localização (etnia e país de residência)

# COMMAND ----------

dim_localizacao = (
    df.select("etnia", "pais_residencia").distinct()
    .withColumn("id_localizacao", F.row_number().over(Window.orderBy("pais_residencia", "etnia")))
    .select("id_localizacao", "etnia", "pais_residencia")
)

dim_localizacao.write.format("delta").mode("overwrite").option("overwriteSchema", "true") \
    .saveAsTable(f"{ESQUEMA_GOLD}.Dimensao_Localizacao")

dim_localizacao = spark.table(f"{ESQUEMA_GOLD}.Dimensao_Localizacao")
dim_localizacao.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Dimensão: Contexto do teste (quem respondeu, se já usou app antes)

# COMMAND ----------

dim_contexto = (
    df.select("quem_respondeu", "usou_app_antes", "faixa_etaria_desc").distinct()
    .withColumn("id_contexto_teste", F.row_number().over(Window.orderBy("quem_respondeu", "usou_app_antes")))
    .select("id_contexto_teste", "quem_respondeu", "usou_app_antes", "faixa_etaria_desc")
)

dim_contexto.write.format("delta").mode("overwrite").option("overwriteSchema", "true") \
    .saveAsTable(f"{ESQUEMA_GOLD}.Dimensao_Contexto_Teste")

dim_contexto = spark.table(f"{ESQUEMA_GOLD}.Dimensao_Contexto_Teste")
dim_contexto.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Fato: Triagem
# MAGIC Junta a Silver com cada dimensão para trazer as respectivas chaves substitutas
# MAGIC (surrogate keys), e mantém como medidas as 10 respostas do questionário AQ-10,
# MAGIC a pontuação total e o resultado da triagem.

# COMMAND ----------

colunas_score = [f"A{i}_Score" for i in range(1, 11)]

# Observação importante: usamos eqNullSafe ("<=>") em vez de um join simples por
# nome de coluna, porque colunas como `idade` podem conter NULL (idades fora da
# faixa plausível foram zeradas na Silver). Em SQL padrão, NULL = NULL nunca é
# verdadeiro, então um join comum descartaria silenciosamente essas linhas. O
# eqNullSafe trata NULL = NULL como verdadeiro, preservando todas as triagens.
fato = (
    df.alias("s")
    .join(
        dim_demografia.alias("dd"),
        on=(df["idade"].eqNullSafe(dim_demografia["idade"])) & (df["genero"].eqNullSafe(dim_demografia["genero"])),
        how="left",
    )
    .join(
        dim_saude.alias("dsf"),
        on=(df["nasceu_com_icterica"].eqNullSafe(dim_saude["nasceu_com_icterica"]))
        & (df["possui_parente_autista"].eqNullSafe(dim_saude["possui_parente_autista"])),
        how="left",
    )
    .join(
        dim_localizacao.alias("dl"),
        on=(df["etnia"].eqNullSafe(dim_localizacao["etnia"])) & (df["pais_residencia"].eqNullSafe(dim_localizacao["pais_residencia"])),
        how="left",
    )
    .join(
        dim_contexto.alias("dc"),
        on=(df["quem_respondeu"].eqNullSafe(dim_contexto["quem_respondeu"]))
        & (df["usou_app_antes"].eqNullSafe(dim_contexto["usou_app_antes"]))
        & (df["faixa_etaria_desc"].eqNullSafe(dim_contexto["faixa_etaria_desc"])),
        how="left",
    )
    .withColumn("id_triagem", F.monotonically_increasing_id())
    .select(
        "id_triagem",
        "id_demografia",
        "id_saude_familiar",
        "id_localizacao",
        "id_contexto_teste",
        *colunas_score,
        "pontuacao_teste",
        "resultado_triagem_positivo",
    )
)

fato.write.format("delta").mode("overwrite").option("overwriteSchema", "true") \
    .saveAsTable(f"{ESQUEMA_GOLD}.Fato_Triagem")

print(f"Fato_Triagem: {fato.count()} linhas")
fato.display()

# COMMAND ----------

# MAGIC %md
# MAGIC **Evidência para o README:** prints do `display()` de cada dimensão e da fato,
# MAGIC além de um print do Catalog Explorer mostrando o schema `gold` com as 5 tabelas
# MAGIC (idealmente com o diagrama de linhagem — aba "Lineage" do Unity Catalog — que o
# MAGIC Databricks gera automaticamente mostrando Bronze → Silver → Gold).
