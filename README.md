# MVP — Pipeline de Dados na Nuvem: Triagem de Traços de TEA em Adultos

**Autora:** Ingrid Dambros
**Plataforma:** Databricks Free Edition (Lakehouse / Arquitetura Medalhão)
**Dataset:** Autism Screening Adult (UCI ML Repository / Fadi Thabtah, 2017)

> Este README segue a estrutura de tópicos exigida na especificação do MVP, com
> todas as evidências (screenshots) e resultados reais extraídos da execução do
> pipeline no meu workspace Databricks Free Edition.

---

## Contexto de Negócios e Perguntas (Etapa 2 e 4.1)

### Problema de negócio

Serviços e plataformas voltados a saúde neurodivergente — como o Legado Azul, projeto
que venho desenvolvendo com foco em indivíduos neurodivergentes e suas famílias —
frequentemente precisam entender **quais perfis de pessoas apresentam maior
probabilidade de um resultado positivo em uma triagem inicial de traços de Transtorno
do Espectro Autista (TEA)**, para apoiar a priorização de encaminhamento a uma
avaliação diagnóstica especializada (que é mais demorada e custosa que a triagem).

O problema a ser resolvido neste MVP é: **"Quais fatores demográficos, de saúde
familiar e de contexto do teste estão associados a um resultado positivo na triagem
de traços de TEA em adultos?"**

### Perguntas de negócio

1. Qual a proporção de resultados positivos (traço de TEA) entre os respondentes, e
   como essa proporção varia por faixa etária e gênero?
2. Pessoas com histórico familiar de autismo (um parente com autismo) apresentam
   maior propensão a um resultado positivo na triagem?
3. Nascer com icterícia está associado a uma maior taxa de resultado positivo?
4. O resultado do teste varia conforme quem o respondeu (o próprio indivíduo, um dos
   pais, um profissional de saúde etc.)?
5. Quais países/regiões concentram mais triagens realizadas, e como se compara a
   proporção de resultados positivos entre eles?
6. Pessoas que já usaram um aplicativo de triagem antes apresentam resultado
   diferente das que nunca usaram?

> Conforme orientado na especificação, estas perguntas permanecem registradas
> integralmente mesmo que nem todas tenham sido plenamente respondidas — a discussão
> sobre o que foi ou não alcançado está na seção "Autoavaliação".

### Contexto e estrutura dos dados brutos

O dataset **"Autism Screening Adult"** foi criado por Fadi Thabtah (2017) e está
publicado no [UCI Machine Learning Repository (dataset nº 426)](https://archive.ics.uci.edu/dataset/426/autism+screening+adult),
com espelhos no Kaggle (ex.: [andrewmvd/autism-screening-on-adults](https://www.kaggle.com/datasets/andrewmvd/autism-screening-on-adults),
[faizunnabi/autism-screening](https://www.kaggle.com/datasets/faizunnabi/autism-screening)).

**Licença:** Creative Commons Attribution 4.0 International (CC BY 4.0) — permite uso,
compartilhamento e adaptação para qualquer finalidade, desde que os créditos ao autor
original (Fadi Thabtah) sejam mantidos. Essa licença consta explicitamente na página
do UCI ML Repository.

**Tamanho:** 704 registros (linhas), 21 colunas.

**O que representa cada registro:** uma pessoa adulta que respondeu ao questionário de
triagem AQ-10 (Autism Spectrum Quotient, versão de 10 perguntas), um instrumento
validado de triagem (não diagnóstico) para traços de TEA.

**Colunas originais (camada bruta):**

| Coluna original | Descrição |
|---|---|
| `A1_Score` a `A10_Score` | Resposta (0 ou 1) para cada uma das 10 perguntas do questionário AQ-10 |
| `age` | Idade do respondente, em anos |
| `gender` | Gênero (`m`/`f`) |
| `ethnicity` | Etnia autodeclarada |
| `jundice` | Se a pessoa nasceu com icterícia (`yes`/`no`) — *nome com erro de digitação no dataset original (o correto seria "jaundice")* |
| `austim` | Se algum parente próximo tem autismo (`yes`/`no`) — *nome com erro de digitação no dataset original* |
| `contry_of_res` | País de residência — *nome com erro de digitação no dataset original* |
| `used_app_before` | Se a pessoa já usou um app de triagem antes (`yes`/`no`) |
| `result` | Pontuação total calculada pelo algoritmo de triagem (soma das respostas) |
| `age_desc` | Faixa etária descritiva (neste dataset de adultos, valor constante "18 and more") |
| `relation` | Quem respondeu o teste (o próprio indivíduo, pai/mãe, profissional de saúde, outro parente) |
| `Class/ASD` | Resultado da triagem: traço de TEA identificado (`YES`/`NO`) |

---

## Carga dos Dados (Etapa 4.2)

**Como foi feita:** a coleta é do tipo "caso simples" — o dataset foi baixado do
Kaggle (mirror do UCI ML Repository) e enviado para um **Volume do Unity Catalog**
no Databricks (`/Volumes/workspace/default/raw_files/`), de onde é lido pelo
notebook de ingestão. Não houve necessidade de web scraping ou consumo de API, pois
o dataset já está disponível em arquivo pronto.

**Observação sobre o formato do arquivo:** o arquivo disponibilizado no Kaggle
(`Autism_Data.arff`) tem extensão `.arff` (formato originalmente usado pela
ferramenta Weka), mas seu conteúdo já está em texto separado por vírgulas, com uma
linha de cabeçalho idêntica à de um CSV comum — não contém as seções `@RELATION`/
`@ATTRIBUTE`/`@DATA` de um ARFF "de verdade". Por isso, o arquivo foi apenas salvo
com a extensão `.csv` (nenhum conteúdo foi alterado), o que já é suficiente para o
`spark.read.csv(...)` interpretá-lo corretamente.

**Referência ao script:** [`notebooks/01_bronze_ingestao.py`](notebooks/01_bronze_ingestao.py).

O notebook lê o CSV com `spark.read.csv(...)`, faz apenas um ajuste técnico
estrutural (renomear a coluna `Class/ASD` para `Class_ASD`, pois `/` não é permitido
em nomes de coluna Delta), adiciona metadados de controle (`_dt_ingestao`,
`_arquivo_origem`) e grava como tabela Delta `workspace.bronze.triagem_asd_raw` —
preservando os dados exatamente como vieram, incluindo os erros de digitação
originais e os valores `?` de dado faltante, seguindo o princípio da camada Bronze da
Arquitetura Medalhão.

**Evidências (prints reais do workspace):**

![Display da camada Bronze](evidencias/01_display_bronze.png)
*Resultado do `display()` do notebook 01 — os dados brutos, incluindo os erros de
digitação originais (`jundice`, `austim`, `contry_of_res`) e os valores `?` de dado
faltante, preservados como vieram da fonte.*

![Tabela Bronze salva](evidencias/02_tabela_salva.png)
*Confirmação de persistência: tabela `workspace.bronze.triagem_asd_raw` salva com
704 registros.*

---

## Modelagem e Catálogo de Dados (Etapa 4.3)

### Visão geral da arquitetura

O pipeline segue a Arquitetura Medalhão dentro do Lakehouse do Databricks:

```
Bronze (workspace.bronze.triagem_asd_raw)
   -> dado bruto, como recebido, + metadados de ingestão
Silver (workspace.silver.triagem_asd_limpo)
   -> dado limpo, tipado, padronizado, sem duplicatas
Gold (workspace.gold.*)
   -> Esquema Estrela: Fato_Triagem + 4 tabelas dimensão
```

### Modelo dimensional (Esquema Estrela)

**Grão da tabela fato:** uma linha = uma triagem realizada por um respondente.

```
Fato_Triagem (idTriagem, idDemografia, idSaudeFamiliar, idLocalizacao, idContextoTeste,
              A1_Score...A10_Score, pontuacaoTeste, resultadoTriagemPositivo)

Dimensao_Demografia (idDemografia, idade, genero, faixaEtaria)
Dimensao_Saude_Familiar (idSaudeFamiliar, nasceuComIctericia, possuiParenteAutista)
Dimensao_Localizacao (idLocalizacao, etnia, paisResidencia)
Dimensao_Contexto_Teste (idContextoTeste, quemRespondeu, usouAppAntes, faixaEtariaDesc)
```

Optamos por um Esquema Estrela (não Snowflake) porque as dimensões têm baixa
cardinalidade e não há hierarquias claras que justifiquem normalização adicional
(ex.: país não se desdobra em uma hierarquia região→país→continente relevante para
este MVP). Não há dimensão de tempo porque o dataset não registra datas de aplicação
do teste — cada linha é um evento único de triagem sem componente temporal.

Referência ao script: [`notebooks/03_gold_modelagem.py`](notebooks/03_gold_modelagem.py).

### Catálogo de Dados

#### Fato_Triagem
| Campo | Tipo | Descrição | Domínio |
|---|---|---|---|
| id_triagem | long | Identificador único da triagem (chave primária, gerado) | Sequencial |
| id_demografia | int | FK → Dimensao_Demografia | — |
| id_saude_familiar | int | FK → Dimensao_Saude_Familiar | — |
| id_localizacao | int | FK → Dimensao_Localizacao | — |
| id_contexto_teste | int | FK → Dimensao_Contexto_Teste | — |
| A1_Score...A10_Score | int | Resposta a cada pergunta do questionário AQ-10 | 0 ou 1 |
| pontuacao_teste | int | Pontuação total da triagem | 0 a 10 |
| resultado_triagem_positivo | boolean | Resultado da triagem: traço de TEA identificado | true/false |

**Linhagem:** originada da tabela Silver `triagem_asd_limpo`, após junção (`join`) com
as 4 dimensões geradas a partir dela mesma.

#### Dimensao_Demografia
| Campo | Tipo | Descrição | Domínio |
|---|---|---|---|
| id_demografia | int | Chave primária (gerada) | Sequencial |
| idade | int | Idade em anos | 15–100 (ou nulo) |
| genero | string | Gênero autodeclarado | "m", "f" |
| faixa_etaria | string | Faixa etária derivada da idade | "18-24", "25-34", "35-44", "45-54", "55+", "Não informado" |

**Linhagem:** derivada da coluna `age`/`gender` da tabela Bronze (`age`, `gender`),
renomeadas e limpas na Silver.

#### Dimensao_Saude_Familiar
| Campo | Tipo | Descrição | Domínio |
|---|---|---|---|
| id_saude_familiar | int | Chave primária (gerada) | Sequencial |
| nasceu_com_icterica | boolean | Se nasceu com icterícia | true/false |
| possui_parente_autista | boolean | Se possui parente próximo com autismo | true/false |

**Linhagem:** derivada das colunas originais `jundice` e `austim` (Bronze),
convertidas de texto ("yes"/"no") para booleano na Silver.

#### Dimensao_Localizacao
| Campo | Tipo | Descrição | Domínio |
|---|---|---|---|
| id_localizacao | int | Chave primária (gerada) | Sequencial |
| etnia | string | Etnia autodeclarada | Ex.: White-European, Asian, Black, Latino, Others, ... |
| pais_residencia | string | País de residência | Nome do país (texto livre no dataset original) |

**Linhagem:** derivada das colunas originais `ethnicity` e `contry_of_res` (Bronze),
padronizadas na Silver (correção de grafias divergentes e renomeação do campo de país).

#### Dimensao_Contexto_Teste
| Campo | Tipo | Descrição | Domínio |
|---|---|---|---|
| id_contexto_teste | int | Chave primária (gerada) | Sequencial |
| quem_respondeu | string | Quem preencheu o teste | "Self", "Parent", "Health care professional", "Relative", "Others" |
| usou_app_antes | boolean | Se já usou um app de triagem antes | true/false |
| faixa_etaria_desc | string | Faixa etária descritiva original do dataset | "18 and more" (constante nesta base de adultos) |

**Linhagem:** derivada das colunas originais `relation`, `used_app_before` e
`age_desc` (Bronze).

**Evidência (print real do workspace):**

![Catalog Explorer - schemas bronze, silver e gold](evidencias/schemas_bronze_silver_gold.png)
*Catalog Explorer mostrando o schema `gold` com as 5 tabelas do Esquema Estrela
(`fato_triagem`, `dimensao_demografia`, `dimensao_saude_familiar`,
`dimensao_localizacao`, `dimensao_contexto_teste`), ao lado dos schemas `bronze`
(`triagem_asd_raw`) e `silver` (`triagem_asd_limpo`), confirmando a progressão
completa da Arquitetura Medalhão.

---

## Pipeline de Dados (Etapa 4.4)

O pipeline foi organizado em **5 notebooks separados**, um por responsabilidade,
para facilitar manutenção e depuração (em vez de um único notebook monolítico):

1. [`notebooks/01_bronze_ingestao.py`](notebooks/01_bronze_ingestao.py) — extração
   (Extract) do CSV e carga (Load) na camada Bronze.
2. [`notebooks/02_silver_limpeza.py`](notebooks/02_silver_limpeza.py) — transformação
   (Transform): tratamento de nulos, padronização de texto, correção de tipos,
   conversão para booleano, renomeação de colunas, deduplicação.
3. [`notebooks/03_gold_modelagem.py`](notebooks/03_gold_modelagem.py) — transformação
   final (Transform) e carga (Load): construção das dimensões e da fato do Esquema
   Estrela.
4. [`notebooks/04_qualidade_dados.py`](notebooks/04_qualidade_dados.py) — verificações
   de qualidade sobre Bronze e Silver.
5. [`notebooks/05_analise.py`](notebooks/05_analise.py) — consultas SQL respondendo
   às 6 perguntas de negócio sobre a camada Gold.

Cada transformação de conteúdo (não apenas estrutural) está comentada em markdown
diretamente no notebook correspondente, explicando o que foi feito, por que foi
feito e qual o impacto esperado nos dados — por exemplo, a conversão de `"?"` para
`NULL`, a padronização de categorias de etnia, e a marcação de idades fora da faixa
plausível.

**Evidência (print real do workspace):**

![Catalog Explorer - tabelas persistidas](evidencias/schemas_bronze_silver_gold.png)
*Catalog Explorer confirmando que as tabelas das 3 camadas foram efetivamente
persistidas no workspace: `bronze.triagem_asd_raw` (1 tabela), `silver.triagem_asd_limpo`
(1 tabela) e `gold` com as 5 tabelas do Esquema Estrela.*

---

## Qualidade de Dados (Etapa 4.5)

A análise de qualidade (script completo em
[`notebooks/04_qualidade_dados.py`](notebooks/04_qualidade_dados.py)) cobriu as
cinco dimensões pedidas na especificação:

- **Completude:** as colunas `age`, `ethnicity` e `relation` continham o marcador
  `"?"` para valores ausentes na fonte original — convertidos para `NULL` na Silver
  para serem corretamente reconhecidos como dado faltante.
- **Consistência:** a coluna `ethnicity` apresentava grafias divergentes para a
  mesma categoria (ex.: `"others"` e `"Others"`) — padronizadas na Silver. Além
  disso, várias colunas de texto (`ethnicity`, `relation`, `contry_of_res`,
  `gender`, `age_desc`) tinham aspas simples residuais em torno do valor (ex.:
  `'United States'`, `'Health care professional'`), resíduo do formato de
  exportação original do arquivo — removidas na Silver para que, por exemplo,
  `'United States'` e `United States` não fossem tratados como países diferentes.
- **Unicidade:** verificação de linhas totalmente duplicadas na Bronze, removidas
  com `dropDuplicates()` na Silver.
- **Acurácia:** (a) idades fora de uma faixa plausível para um adulto (< 15 ou >
  100 anos) foram marcadas como nulas; (b) foi criada uma verificação cruzada entre
  a soma das 10 respostas do questionário (`A1_Score`...`A10_Score`) e a pontuação
  total informada (`result`/`pontuacao_teste`), sinalizando eventuais divergências
  na coluna `pontuacao_inconsistente`.
- **Outliers:** um boxplot da distribuição de idade foi gerado para inspeção visual
  de valores extremos remanescentes após o tratamento de acurácia.

**Resultados obtidos na execução real** (Bronze: 704 linhas; Silver, após remoção de
duplicatas: 699 linhas):

| Problema encontrado | Quantas linhas afetadas | Tratamento aplicado |
|---|---|---|
| Valores `?` em `age` | 3 linhas (0,43%) | Convertido para NULL |
| Valores `?` em `ethnicity` | 95 linhas (13,59%) | Convertido para NULL |
| Valores `?` em `relation` | 95 linhas (13,59%) | Convertido para NULL |
| Grafias divergentes em `ethnicity` | 12 valores distintos na Bronze → 11 categorias reais na Silver (ex.: `"others"`/`"Others"` e variações com aspas eram a mesma categoria) | Padronização de capitalização |
| Aspas simples residuais em colunas de texto (ex.: país, quem respondeu) | Presentes em vários valores de `ethnicity`, `relation` e `contry_of_res` (ex.: `'Middle Eastern '`, `'South Asian'`) | Removidas com `regexp_replace` |
| Idades fora da faixa plausível | 1 registro com valor 383 (idade mínima 17 e máxima 383 na Bronze; após tratamento, máxima passou a 64) | Convertidas para NULL |
| Linhas duplicadas | 5 grupos de duplicatas exatas identificados na Bronze (5 linhas removidas na deduplicação) | Removidas com `dropDuplicates()` |
| Divergência soma A1-A10 vs. pontuação | 0 linhas — a soma das respostas bateu com a pontuação informada em 100% dos casos | Sinalizada para análise (nenhuma inconsistência encontrada) |

**Evidências (prints reais do workspace):**

![Completude - Bronze](evidencias/01_completude_bronze.png)
*Percentual de valores ausentes por coluna na Bronze (colunas A1-A9_Score, sem
valores ausentes).*

![Completude - Silver](evidencias/02_completude_silver.png)
*Percentual de valores ausentes nas colunas que continham `"?"`: idade (0,43%),
gênero (0%), etnia (13,59%) e quem_respondeu (13,59%).*

![Consistência - Bronze](evidencias/03_consistencia_bronze.png)
*Valores distintos de etnia na Bronze — nota-se `"?"`, aspas residuais
(`'Middle Eastern '`, `'South Asian'`) e a duplicidade `"others"`/`"Others"`.*

![Consistência - Silver](evidencias/04_consistencia_silver.png)
*Valores distintos de etnia na Silver, já padronizados — de 12 valores brutos para
11 categorias reais (os `"?"` viraram `NULL`).*

![Unicidade](evidencias/05_unicidade.png)
*5 grupos de linhas duplicadas identificados na Bronze.*

![Acurácia - idade](evidencias/06_acuracia_idade.png)
*Estatísticas de idade antes (mínimo 17, máximo 383 — outlier claramente inválido,
média 29,70) e depois do tratamento (mínimo 17, máximo 64, média 29,19).*

![Acurácia - pontuação](evidencias/07_acuracia_pontuacao.png)
*Verificação cruzada entre a soma das respostas A1-A10 e a pontuação informada:
nenhuma linha divergente (`No rows returned`), confirmando consistência interna do
questionário AQ-10 nesta base.*

![Outliers](evidencias/08_outliers_boxplot.png)
*Boxplot da idade (Silver) — poucos respondentes acima de ~56 anos aparecem como
outliers visuais, mas dentro de uma faixa plausível após o tratamento de acurácia.*

---

## Análise de Dados (Etapa 4.5)

Consultas completas em [`notebooks/05_analise.py`](notebooks/05_analise.py), uma
seção por pergunta de negócio. Resultados reais obtidos na execução no workspace
(base Gold: 699 triagens):

### 1. Resultado positivo por faixa etária e gênero

![Resultado por faixa etária e gênero](evidencias/01_faixa_etaria_genero.png)

*Discussão:* a proporção de resultado positivo cresce de forma consistente com a
idade: de ~20-23% na faixa 18-24 anos para 45-50% na faixa 55+. Em quase todas as
faixas etárias, mulheres apresentam percentual de resultado positivo maior que
homens da mesma faixa (ex.: 35-44: 40,6% f vs. 30,4% m; 25-34: 31,4% f vs. 21,4% m) —
um achado que contraria a expectativa comum de que TEA é mais identificado em
homens, mas que pode refletir quem procura fazer a triagem (viés de amostragem) mais
do que a prevalência real. As faixas 55+ e "Não informado" têm poucos registros (4,
11, 2 e 1 pessoas) e devem ser lidas com cautela.

### 2. Histórico familiar de autismo

![Histórico familiar de autismo](evidencias/02_historico_familiar.png)

*Discussão:* quem tem um parente próximo com autismo apresenta quase o dobro da
taxa de resultado positivo (46,7%, n=90) em comparação a quem não tem (23,8%,
n=609). É a associação mais forte entre as seis perguntas, consistente com a
literatura sobre componente hereditário do TEA — mas o dataset não permite afirmar
causalidade, apenas associação.

### 3. Icterícia ao nascer

![Icterícia ao nascer](evidencias/03_ictericia.png)

*Discussão:* nascer com icterícia também está associado a uma taxa maior de
resultado positivo (40,6%, n=69) frente a quem não nasceu (25,2%, n=630). O efeito é
menor que o do histórico familiar e a amostra do grupo "com icterícia" é pequena
(69 pessoas), então essa associação deve ser tratada como um indício e não uma
conclusão — como em qualquer triagem, associação estatística não implica
causalidade.

### 4. Quem respondeu o teste

![Quem respondeu o teste](evidencias/04_quem_respondeu.png)

*Discussão:* a maioria respondeu por si mesma (Self, 519 triagens, 29,9%
positivo). Chama atenção o grupo com `quem_respondeu` nulo (95 triagens, os mesmos
registros que já tinham "?" nessa e na coluna de etnia, ver Qualidade de Dados):
tem a menor pontuação média (3,58) e a menor taxa de positivo (9,5%) de todos os
grupos — sugerindo que esses respondentes deixaram o campo em branco por terem
abandonado o questionário mais cedo, o que pode ter enviesado suas respostas para
baixo. Entre as categorias preenchidas, "Relative" (parente) teve a maior taxa
(34,6%, mas com apenas 26 casos).

### 5. País de residência

![País de residência](evidencias/05_pais_residencia.png)

*Discussão:* os países com mais triagens são Estados Unidos (111), Índia (81),
Nova Zelândia (81), Emirados Árabes Unidos (80) e Reino Unido (76). A taxa de
resultado positivo varia enormemente entre eles — de 3,8% (EAU) e 7,4% (Índia) até
36,8% (Reino Unido) e 46,8% (Estados Unidos) — uma diferença grande demais para
atribuir apenas à prevalência real de TEA entre países; é mais provável que reflita
diferenças no canal de recrutamento/quem teve acesso à triagem em cada país. Países
com poucas triagens (ex.: Canadá, 66,7% em apenas 15 casos, ou Sri Lanka, 0% em 14
casos) têm percentuais pouco confiáveis por causa do tamanho pequeno da amostra.

### 6. Uso prévio de aplicativo de triagem

![Uso prévio de aplicativo de triagem](evidencias/06_uso_app_antes.png)

*Discussão:* quem já usou um app de triagem antes teve taxa de positivo maior
(41,7%) que quem nunca usou (26,5%), mas o grupo "já usou" tem apenas 12 pessoas —
amostra pequena demais para generalizar. Uma hipótese razoável é a de
autosseleção: pessoas que já suspeitavam de um traço de TEA (por exemplo, por já
terem usado outro app) tendem a buscar uma nova triagem.

### Visualização de apoio

![Percentual de triagens positivas por faixa etária](evidencias/07_grafico_faixa_etaria.png)

### Discussão geral
Conectando as seis respostas ao problema de negócio original ("quais fatores
demográficos, de saúde familiar e de contexto do teste estão associados a um
resultado positivo na triagem de traços de TEA em adultos?"): o fator com
associação mais forte e mais confiável (maior amostra) é o **histórico familiar de
autismo** (quase o dobro da taxa de positivo), seguido por **idade mais avançada**
e, em menor grau, **icterícia ao nascer**. O **país de residência** mostra grandes
diferenças, mas provavelmente refletindo quem teve acesso à triagem em cada lugar,
não uma diferença real de prevalência. Já **quem respondeu o teste** e **uso
prévio de app** têm amostras pequenas em alguns grupos e servem mais como alerta de
qualidade de dados (grupo com resposta em branco) do que como fator de
priorização. Para o problema de negócio do Legado Azul, isso sugere que um
critério inicial de priorização de encaminhamento poderia dar peso maior a
histórico familiar de autismo e idade, usando os demais fatores como sinais
complementares, e não isoladamente — sempre reforçando que a triagem AQ-10 é um
instrumento de rastreio, não um diagnóstico.

---

## Autoavaliação

### Objetivos atingidos

Consegui percorrer o ciclo completo proposto pelo MVP e responder às 6 perguntas de
negócio definidas no início do trabalho, todas com evidência real extraída do
workspace Databricks. Os achados com maior confiança foram os de **histórico
familiar de autismo** (quase o dobro da taxa de resultado positivo, com amostra
robusta dos dois lados: 90 vs. 609 registros) e de **idade** (crescimento
consistente da taxa de positivo com a idade, em todas as faixas). Considero também
bem-sucedida a etapa de qualidade de dados: encontrei e tratei problemas reais no
dataset (valores `?`, aspas residuais, um outlier de idade de 383 anos, grafias
divergentes em etnia) e documentei cada um com números antes/depois, em vez de
apenas descrever o processo de forma abstrata.

### Objetivos não atingidos (e por quê)

Nem todas as perguntas puderam ser respondidas com o mesmo grau de confiança. A
pergunta sobre **país de residência** mostrou diferenças muito grandes entre países
(de ~4% a ~47% de resultado positivo), mas várias dessas nacionalidades têm poucas
triagens (14, 15 registros), o que torna o percentual pouco confiável — a limitação
aqui não foi de execução, mas do próprio tamanho da amostra (704 registros no
total, pequena para recortes tão granulares). O mesmo vale para **uso prévio de
app de triagem**, onde o grupo "já usou" tem apenas 12 pessoas. Além disso, o
dataset não traz diagnóstico clínico confirmado (apenas o resultado da triagem
AQ-10) nem componente temporal (não há data de aplicação do teste), então não foi
possível validar as associações encontradas contra um "gabarito" externo, nem
analisar tendências ao longo do tempo.

### Dificuldades encontradas

A maior dificuldade não foi conceitual, foi de execução real do pipeline na
nuvem — algo que só aparece quando se roda o código de verdade em vez de apenas
escrevê-lo. Ao longo da execução no Databricks Free Edition, apareceram 3 bugs
reais no código que só se manifestaram com os dados de produção: (1) o nome da
coluna original do dataset é `jundice`, não `jaundice` como o nome sugeriria
(erro de digitação do próprio dataset), o que quebrou uma junção até eu corrigir
o nome da coluna; (2) o Spark Connect (motor usado pelo Databricks Free Edition)
não tolera juntar um DataFrame com outro derivado dele mesmo sem antes persistir
e reler a tabela, o que gerava um erro de "coluna ambígua" na montagem da tabela
fato; (3) o modo ANSI SQL do Databricks rejeita converter o texto `"?"`
diretamente para número, exigindo `try_cast` em vez de um cast comum. Também
enfrentei uma instabilidade pontual da plataforma (mensagem de "Databricks
unresponsive"), resolvida recarregando a página. Do lado conceitual, a principal
dificuldade foi decidir como modelar um dataset sem componente temporal em um
Esquema Estrela — cada linha representa um evento único de triagem, sem uma
dimensão de tempo natural.

### Trabalhos futuros

Como próximos passos para evoluir este MVP, penso em: (1) buscar uma base
complementar com diagnóstico clínico confirmado, para validar a triagem AQ-10
contra um resultado externo e não apenas analisar a triagem em si; (2) se
disponível, incorporar uma dimensão de tempo (data de aplicação do teste), o que
permitiria acompanhar tendências e não apenas uma fotografia estática; (3) treinar
um modelo preditivo simples (classificação) sobre a camada Gold para estimar a
probabilidade de resultado positivo a partir do perfil do respondente, indo além
da análise descritiva feita aqui; e (4) conectar esse pipeline de triagem à minha
plataforma Legado Azul, usando os fatores identificados (histórico familiar,
idade) como critérios de apoio à priorização de encaminhamento para avaliação
diagnóstica especializada.

---

## Estrutura do repositório

```
.
├── README.md
├── notebooks/
│   ├── 01_bronze_ingestao.py
│   ├── 02_silver_limpeza.py
│   ├── 03_gold_modelagem.py
│   ├── 04_qualidade_dados.py
│   └── 05_analise.py
└── docs/
    └── setup_databricks.md   # passo a passo de configuração do Databricks Free Edition
```

## Como reproduzir

Veja o passo a passo completo em [`docs/setup_databricks.md`](docs/setup_databricks.md):
criação da conta Databricks Free Edition, upload do CSV, conexão do repositório via
Databricks Repos e ordem de execução dos notebooks.
