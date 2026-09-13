# Databricks notebook source
# MAGIC %md
# MAGIC # 05 - Análise: Respondendo às perguntas de negócio
# MAGIC
# MAGIC Cada seção abaixo corresponde a uma das perguntas definidas na etapa de Objetivo
# MAGIC (ver README, seção "Contexto de Negócios e Perguntas"). Depois de rodar cada
# MAGIC célula, escreva na célula de markdown seguinte (`# PREENCHER APÓS EXECUÇÃO`) a
# MAGIC discussão do resultado obtido, e copie o resultado para o README com um print.

# COMMAND ----------

CATALOGO = "workspace"
spark.sql(f"USE {CATALOGO}.gold")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Pergunta 1 — Resultado positivo por faixa etária e gênero
# MAGIC Qual a proporção de resultados positivos (traço de TEA) por faixa etária e gênero?

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   dd.faixa_etaria,
# MAGIC   dd.genero,
# MAGIC   COUNT(*) AS total_triagens,
# MAGIC   SUM(CASE WHEN f.resultado_triagem_positivo THEN 1 ELSE 0 END) AS positivos,
# MAGIC   ROUND(100.0 * SUM(CASE WHEN f.resultado_triagem_positivo THEN 1 ELSE 0 END) / COUNT(*), 1) AS pct_positivo
# MAGIC FROM Fato_Triagem f
# MAGIC JOIN Dimensao_Demografia dd ON f.id_demografia = dd.id_demografia
# MAGIC GROUP BY dd.faixa_etaria, dd.genero
# MAGIC ORDER BY dd.faixa_etaria, dd.genero;

# COMMAND ----------

# MAGIC %md
# MAGIC **PREENCHER APÓS EXECUÇÃO:** discuta aqui o que os números mostram (ex.: alguma
# MAGIC faixa etária ou gênero concentra mais resultados positivos?).

# COMMAND ----------

# MAGIC %md
# MAGIC ## Pergunta 2 — Histórico familiar de autismo
# MAGIC Pessoas com parente com autismo têm maior propensão a resultado positivo?

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   dsf.possui_parente_autista,
# MAGIC   COUNT(*) AS total_triagens,
# MAGIC   ROUND(100.0 * SUM(CASE WHEN f.resultado_triagem_positivo THEN 1 ELSE 0 END) / COUNT(*), 1) AS pct_positivo
# MAGIC FROM Fato_Triagem f
# MAGIC JOIN Dimensao_Saude_Familiar dsf ON f.id_saude_familiar = dsf.id_saude_familiar
# MAGIC GROUP BY dsf.possui_parente_autista;

# COMMAND ----------

# MAGIC %md
# MAGIC **PREENCHER APÓS EXECUÇÃO:** discuta a diferença percentual encontrada entre os
# MAGIC dois grupos.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Pergunta 3 — Icterícia ao nascer
# MAGIC Nascer com icterícia está associado a maior taxa de resultado positivo?

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   dsf.nasceu_com_icterica,
# MAGIC   COUNT(*) AS total_triagens,
# MAGIC   ROUND(100.0 * SUM(CASE WHEN f.resultado_triagem_positivo THEN 1 ELSE 0 END) / COUNT(*), 1) AS pct_positivo
# MAGIC FROM Fato_Triagem f
# MAGIC JOIN Dimensao_Saude_Familiar dsf ON f.id_saude_familiar = dsf.id_saude_familiar
# MAGIC GROUP BY dsf.nasceu_com_icterica;

# COMMAND ----------

# MAGIC %md
# MAGIC **PREENCHER APÓS EXECUÇÃO:** discuta o achado — lembre-se de que associação
# MAGIC estatística não implica causalidade, e a amostra pode ser pequena em um dos grupos.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Pergunta 4 — Quem respondeu o teste
# MAGIC O resultado varia conforme quem respondeu (o próprio indivíduo, um familiar, um
# MAGIC profissional de saúde)?

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   dc.quem_respondeu,
# MAGIC   COUNT(*) AS total_triagens,
# MAGIC   ROUND(AVG(f.pontuacao_teste), 2) AS pontuacao_media,
# MAGIC   ROUND(100.0 * SUM(CASE WHEN f.resultado_triagem_positivo THEN 1 ELSE 0 END) / COUNT(*), 1) AS pct_positivo
# MAGIC FROM Fato_Triagem f
# MAGIC JOIN Dimensao_Contexto_Teste dc ON f.id_contexto_teste = dc.id_contexto_teste
# MAGIC GROUP BY dc.quem_respondeu
# MAGIC ORDER BY total_triagens DESC;

# COMMAND ----------

# MAGIC %md
# MAGIC **PREENCHER APÓS EXECUÇÃO:** comente se algum grupo (ex.: "self" vs "parent")
# MAGIC tem pontuação média ou taxa de positivos notavelmente diferente.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Pergunta 5 — País de residência e etnia
# MAGIC Quais países/regiões têm mais triagens e maior proporção de resultados positivos?

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   dl.pais_residencia,
# MAGIC   COUNT(*) AS total_triagens,
# MAGIC   ROUND(100.0 * SUM(CASE WHEN f.resultado_triagem_positivo THEN 1 ELSE 0 END) / COUNT(*), 1) AS pct_positivo
# MAGIC FROM Fato_Triagem f
# MAGIC JOIN Dimensao_Localizacao dl ON f.id_localizacao = dl.id_localizacao
# MAGIC GROUP BY dl.pais_residencia
# MAGIC ORDER BY total_triagens DESC
# MAGIC LIMIT 15;

# COMMAND ----------

# MAGIC %md
# MAGIC **PREENCHER APÓS EXECUÇÃO:** cuidado ao interpretar países com poucas triagens
# MAGIC (percentuais podem ser enganosos com amostras pequenas — cite o total antes de
# MAGIC comentar o percentual).

# COMMAND ----------

# MAGIC %md
# MAGIC ## Pergunta 6 — Uso prévio de aplicativo de triagem

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   dc.usou_app_antes,
# MAGIC   COUNT(*) AS total_triagens,
# MAGIC   ROUND(AVG(f.pontuacao_teste), 2) AS pontuacao_media,
# MAGIC   ROUND(100.0 * SUM(CASE WHEN f.resultado_triagem_positivo THEN 1 ELSE 0 END) / COUNT(*), 1) AS pct_positivo
# MAGIC FROM Fato_Triagem f
# MAGIC JOIN Dimensao_Contexto_Teste dc ON f.id_contexto_teste = dc.id_contexto_teste
# MAGIC GROUP BY dc.usou_app_antes;

# COMMAND ----------

# MAGIC %md
# MAGIC **PREENCHER APÓS EXECUÇÃO:** discuta se quem já usou um app de triagem antes
# MAGIC tende a ter resultado diferente (pode sugerir busca por confirmação diagnóstica).

# COMMAND ----------

# MAGIC %md
# MAGIC ## Visualização de apoio (exemplo)
# MAGIC Gráfico de barras da pergunta 1, para incluir no README.

# COMMAND ----------

import matplotlib.pyplot as plt

pdf = spark.sql("""
    SELECT dd.faixa_etaria, ROUND(100.0 * SUM(CASE WHEN f.resultado_triagem_positivo THEN 1 ELSE 0 END) / COUNT(*), 1) AS pct_positivo
    FROM Fato_Triagem f
    JOIN Dimensao_Demografia dd ON f.id_demografia = dd.id_demografia
    GROUP BY dd.faixa_etaria
    ORDER BY dd.faixa_etaria
""").toPandas()

plt.figure(figsize=(7, 4))
plt.bar(pdf["faixa_etaria"], pdf["pct_positivo"])
plt.title("% de triagens positivas por faixa etária")
plt.ylabel("% positivo")
plt.xlabel("Faixa etária")
plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Discussão geral (conectando as respostas ao problema original)
# MAGIC **PREENCHER APÓS EXECUÇÃO:** amarre as 6 respostas de volta ao problema de negócio
# MAGIC descrito no README — quais perfis concentram mais resultados positivos e como
# MAGIC isso poderia orientar a priorização de encaminhamento para avaliação especializada.
