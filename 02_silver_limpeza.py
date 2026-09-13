# Databricks notebook source
# MAGIC %md
# MAGIC # 02 - Silver: Limpeza e padronização
# MAGIC
# MAGIC **Objetivo:** partir da tabela Bronze e produzir uma versão limpa, tipada e
# MAGIC padronizada, tratando os problemas de qualidade encontrados (nulos disfarçados
# MAGIC de `?`, nomes de colunas com erro de digitação, valores categóricos com grafias
# MAGIC inconsistentes, tipos incorretos, duplicatas).
# MAGIC
# MAGIC Cada transformação abaixo está comentada explicando **o que** foi feito e **por quê**,
# MAGIC conforme pedido na documentação do MVP (Etapa 4.4).

# COMMAND ----------

from pyspark.sql import functions as F

CATALOGO = "workspace"
TABELA_BRONZE = f"{CATALOGO}.bronze.triagem_asd_raw"
TABELA_SILVER = f"{CATALOGO}.silver.triagem_asd_limpo"

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOGO}.silver")

df = spark.table(TABELA_BRONZE)
print(f"Linhas na Bronze: {df.count()}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1) Tratamento de nulos disfarçados
# MAGIC O dataset original usa a string `"?"` para representar valores ausentes nas
# MAGIC colunas `age`, `ethnicity` e `relation`. Substituímos por `NULL` de verdade, para
# MAGIC que o Spark reconheça corretamente como dado faltante (isso impacta contagens de
# MAGIC completude e evita que "?" seja tratado como uma categoria válida).

# COMMAND ----------

colunas_texto = [c for c, t in df.dtypes if t == "string"]
for c in colunas_texto:
    df = df.withColumn(c, F.when(F.trim(F.col(c)) == "?", None).otherwise(F.col(c)))

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2) Padronização de texto (consistência)
# MAGIC Removemos espaços extras e padronizamos a capitalização de colunas categóricas.
# MAGIC Isso resolve o problema comum, já relatado por quem trabalhou com este dataset,
# MAGIC de categorias como `"others"` e `"Others"` sendo tratadas como valores distintos
# MAGIC quando na verdade representam a mesma categoria.
# MAGIC
# MAGIC Também removemos aspas simples que sobraram de algumas células (ex.:
# MAGIC `'United States'`, `'Health care professional'`, `'Middle Eastern '`) — um
# MAGIC resíduo do formato de exportação original do arquivo, que ficaria misturado ao
# MAGIC valor de texto se não fosse tratado (ex.: `'United States'` e `United States`
# MAGIC seriam vistos como países diferentes numa contagem).

# COMMAND ----------

def limpar_aspas_e_espacos(col):
    # remove aspas simples nas bordas e espaços extras (inclusive os que ficaram
    # *dentro* das aspas, como em "'Middle Eastern '")
    return F.trim(F.regexp_replace(F.col(col), r"^'|'$", ""))

for c in ["ethnicity", "relation", "contry_of_res", "gender", "age_desc"]:
    df = df.withColumn(c, limpar_aspas_e_espacos(c))
    df = df.withColumn(c, F.trim(F.col(c)))  # segunda passada: espaço que ficou após remover a aspa

df = df.withColumn(
    "ethnicity",
    F.when(F.lower(F.col("ethnicity")) == "others", F.lit("Others")).otherwise(F.col("ethnicity")),
)
df = df.withColumn("gender", F.lower(F.col("gender")))

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3) Tipagem e correção de acurácia (idade)
# MAGIC A coluna `age` deveria ser um inteiro plausível para um adulto. Convertendo para
# MAGIC inteiro e marcando como nula qualquer idade fora de uma faixa plausível
# MAGIC (< 15 ou > 100 anos) — esse é um problema de acurácia conhecido neste dataset,
# MAGIC que já teve registros de idade claramente impossíveis (ex: valores acima de 300).
# MAGIC Optamos por marcar como nulo em vez de excluir a linha inteira, preservando as
# MAGIC demais informações do respondente.

# COMMAND ----------

df = df.withColumn("age", F.col("age").cast("int"))
df = df.withColumn(
    "age",
    F.when((F.col("age") < 15) | (F.col("age") > 100), None).otherwise(F.col("age")),
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4) Conversão Sim/Não para booleano
# MAGIC As colunas `jundice`, `austim`, `used_app_before` e `Class_ASD` vêm como texto
# MAGIC `"yes"`/`"no"`. Convertemos para booleano, o tipo correto para uma resposta binária,
# MAGIC o que também facilita filtros e agregações nas etapas seguintes.

# COMMAND ----------

def para_booleano(col):
    return F.when(F.lower(F.col(col)).isin("yes", "y", "1"), True) \
            .when(F.lower(F.col(col)).isin("no", "n", "0"), False) \
            .otherwise(None)

for c in ["jundice", "austim", "used_app_before", "Class_ASD"]:
    df = df.withColumn(c, para_booleano(c))

# COMMAND ----------

# MAGIC %md
# MAGIC ### 5) Renomeação para nomes claros e sem erros de digitação
# MAGIC O dataset original tem três colunas com erro de digitação (`jundice` em vez de
# MAGIC `jaundice`, `austim` em vez de `autism`, `contry_of_res` em vez de
# MAGIC `country_of_res`). Aqui, na Silver, já corrigimos e traduzimos os nomes para
# MAGIC deixar o modelo autoexplicativo — diferente da Bronze, onde o nome original é
# MAGIC preservado por rastreabilidade.

# COMMAND ----------

df = (
    df
    .withColumnRenamed("age", "idade")
    .withColumnRenamed("gender", "genero")
    .withColumnRenamed("ethnicity", "etnia")
    .withColumnRenamed("jundice", "nasceu_com_icterica")
    .withColumnRenamed("austim", "possui_parente_autista")
    .withColumnRenamed("contry_of_res", "pais_residencia")
    .withColumnRenamed("used_app_before", "usou_app_antes")
    .withColumnRenamed("result", "pontuacao_teste")
    .withColumnRenamed("age_desc", "faixa_etaria_desc")
    .withColumnRenamed("relation", "quem_respondeu")
    .withColumnRenamed("Class_ASD", "resultado_triagem_positivo")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 6) Verificação de acurácia cruzada (soma das respostas x pontuação)
# MAGIC A coluna `pontuacao_teste` deveria ser igual (ou muito próxima) à soma das 10
# MAGIC respostas `A1_Score` a `A10_Score`. Criamos uma coluna de verificação para uso na
# MAGIC etapa de Qualidade de Dados (04_qualidade_dados).

# COMMAND ----------

colunas_score = [f"A{i}_Score" for i in range(1, 11)]
df = df.withColumn("soma_respostas_calculada", sum(F.col(c) for c in colunas_score))
df = df.withColumn(
    "pontuacao_inconsistente",
    F.col("soma_respostas_calculada") != F.col("pontuacao_teste"),
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 7) Remoção de duplicatas exatas (unicidade)

# COMMAND ----------

linhas_antes = df.count()
df = df.dropDuplicates()
linhas_depois = df.count()
print(f"Duplicatas exatas removidas: {linhas_antes - linhas_depois}")

# COMMAND ----------

df_silver = df.withColumn("_dt_processamento", F.current_timestamp())

(
    df_silver.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(TABELA_SILVER)
)

print(f"Tabela Silver salva em: {TABELA_SILVER}")
print(f"Total de registros: {spark.table(TABELA_SILVER).count()}")
df_silver.display()

# COMMAND ----------

# MAGIC %md
# MAGIC **Evidência para o README:** print do `display()` acima e da tabela
# MAGIC `workspace.silver.triagem_asd_limpo` no Catalog Explorer.
