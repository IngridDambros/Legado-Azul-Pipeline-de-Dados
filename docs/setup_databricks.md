# Passo a passo: Databricks Free Edition + GitHub Repos

## 1. Criar a conta
1. Acesse https://www.databricks.com/learn/free-edition e clique em "Get started" / "Try Free Edition".
2. Cadastre-se com e-mail pessoal (não precisa de cartão de crédito).
3. Confirme o e-mail e finalize a criação do workspace (pode levar 1-2 minutos).

## 2. Criar o schema/catálogo de trabalho
1. No menu lateral, clique em **SQL Editor** (ou abra um Notebook novo).
2. Rode: `CREATE SCHEMA IF NOT EXISTS workspace.bronze;` (e repita para `silver` e `gold`,
   ou deixe que os próprios notebooks criem — eles já têm `CREATE SCHEMA IF NOT EXISTS`).

## 3. Subir o arquivo de dados (CSV)
Opção mais simples:
1. Baixe o CSV do Kaggle (`andrewmvd/autism-screening-on-adults` ou
   `faizunnabi/autism-screening`) para o seu computador.
2. No Databricks, menu lateral **Catalog** → escolha (ou crie) um Volume, ex.:
   `workspace.default.raw_files`.
3. Clique em **Upload to volume** e envie o CSV.
4. Copie o caminho completo do arquivo (algo como
   `/Volumes/workspace/default/raw_files/autism_screening_adult.csv`) e cole na
   variável `CAMINHO_ARQUIVO_ORIGEM` do notebook `01_bronze_ingestao.py`.

Referência oficial: https://docs.databricks.com/aws/pt/volumes/volume-files

## 4. Conectar o repositório GitHub (Databricks Repos)
1. Crie um repositório público no GitHub e suba esta pasta inteira (`README.md`,
   `notebooks/`, `docs/`).
2. No Databricks, menu lateral **Workspace** → **Repos** → **Add Repo**.
3. Cole a URL HTTPS do seu repositório GitHub e clone.
4. Os notebooks `.py` neste projeto já estão no formato "Databricks notebook source",
   então abrem diretamente como notebooks ao serem importados via Repos.

Referência oficial: https://www.databricks.com/resources/demos/videos/developer-experience/getting-started-with-databricks-repos

## 5. Ordem de execução
Rode os notebooks nesta ordem, um de cada vez, conferindo a saída de cada célula:
1. `notebooks/01_bronze_ingestao.py`
2. `notebooks/02_silver_limpeza.py`
3. `notebooks/03_gold_modelagem.py`
4. `notebooks/04_qualidade_dados.py`
5. `notebooks/05_analise.py`

## 6. Capturando as evidências (prints)
Para cada notebook, tire print de pelo menos:
- O resultado de um `display()` de uma tabela relevante.
- A tabela aparecendo no **Catalog Explorer** (menu lateral "Catalog"), confirmando
  que foi persistida.
Cole essas imagens no README, nas seções indicadas.
