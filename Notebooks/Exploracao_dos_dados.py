# Databricks notebook source
# MAGIC %md
# MAGIC # Exploração de Dados
# MAGIC
# MAGIC O objetivo deste notebook é realizar a exploração de dados para entender melhor o conjunto de dados, identificar padrões, características e potenciais problemas. A exploração de dados é uma etapa fundamental para garantir que as análises subsequentes sejam precisas e relevantes.
# MAGIC
# MAGIC ### Passos principais:
# MAGIC 1. **Carregamento dos Dados**: O primeiro passo é carregar os dados no ambiente de análise para visualizar sua estrutura e conteúdo.
# MAGIC 2. **Análise das Colunas e Tipos de Dados**: Verificaremos os tipos de dados, se existem valores nulos e outros aspectos importantes das colunas.
# MAGIC 3. **Estatísticas Descritivas**: Calcularemos estatísticas descritivas para entender a distribuição e os principais parâmetros das variáveis numéricas.
# MAGIC 4. **Identificação de Outliers e Anomalias**: Identificaremos possíveis valores atípicos que possam interferir nas análises.
# MAGIC 5. **Conversão de Tipos de Dados**: Em alguns casos, será necessário converter os tipos de dados para que as análises sejam realizadas de forma eficiente.
# MAGIC
# MAGIC Vamos iniciar carregando os dados e explorando seu conteúdo.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Carregar Bibliotecas
# MAGIC
# MAGIC Para este notebook de exploração de dados, começamos importando a biblioteca **Pandas**, que é fundamental para manipulação e análise de dados em Python. O Pandas oferece estruturas de dados rápidas, poderosas e flexíveis, que são essenciais para o processamento de dados em formato tabular.
# MAGIC

# COMMAND ----------

import pandas as pd

# COMMAND ----------

# MAGIC %md
# MAGIC ## Carregamento dos Dados
# MAGIC
# MAGIC Nesta etapa, estamos carregando os dados de um arquivo CSV localizado no caminho especificado. Utilizamos o **Apache Spark** para fazer isso, o que nos permite processar grandes volumes de dados de forma distribuída e eficiente.
# MAGIC
# MAGIC Primeiro, definimos o caminho do arquivo CSV e, em seguida, carregamos os dados em um DataFrame Spark. O parâmetro `header=True` garante que a primeira linha do arquivo seja usada como nomes das colunas, e o `inferSchema=True` permite que o Spark detecte automaticamente os tipos de dados das colunas.
# MAGIC
# MAGIC Após o carregamento dos dados, exibimos o esquema para verificar os tipos de cada coluna e garantir que os dados foram interpretados corretamente.
# MAGIC

# COMMAND ----------

# Definir o caminho do arquivo
file_path = "dbfs:/FileStore/tables/Walmart.csv"

# Carregar os dados em um DataFrame Spark
walmart_data = spark.read.csv(file_path, header=True, inferSchema=True)

# Exibir o esquema dos dados (tipos de cada coluna)
walmart_data.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Conversão para Pandas
# MAGIC
# MAGIC Para facilitar a visualização e manipulação dos dados, convertemos o DataFrame do Spark para um DataFrame do **Pandas**. O Pandas oferece uma interface mais familiar e poderosa para análise de dados em pequenos conjuntos de dados, permitindo visualizar e realizar operações como filtrar, agrupar e transformar os dados de forma mais intuitiva.
# MAGIC
# MAGIC Após a conversão, usamos o método `head()` do Pandas para exibir as primeiras linhas do DataFrame e termos uma ideia do conteúdo dos dados.
# MAGIC

# COMMAND ----------

# Converter o DataFrame Spark para Pandas para melhor visualização
walmart_pd = walmart_data.toPandas()

# Exibir as primeiras linhas do Pandas DataFrame
walmart_pd.head()


# COMMAND ----------

# MAGIC %md
# MAGIC ## Número de Linhas e Colunas
# MAGIC
# MAGIC Nesta etapa, estamos verificando o número de linhas e colunas do DataFrame. Isso é importante para entender o tamanho do conjunto de dados e garantir que ele foi carregado corretamente.
# MAGIC
# MAGIC Usamos o atributo `shape` do DataFrame Pandas para obter as dimensões do dataset. O `shape[0]` nos fornece o número de linhas e o `shape[1]` nos fornece o número de colunas.
# MAGIC

# COMMAND ----------

#Exibir o número de linhas e colunas:
print(f"Número de linhas: {walmart_pd.shape[0]}")
print(f"Número de colunas: {walmart_pd.shape[1]}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Resumo Estatístico
# MAGIC
# MAGIC Agora, vamos calcular as **estatísticas descritivas** do conjunto de dados. Esse resumo fornece informações como a média, o desvio padrão, o valor mínimo, o valor máximo e os quartis das variáveis numéricas. Ele é essencial para compreender a distribuição e a variabilidade dos dados.
# MAGIC
# MAGIC O método `describe()` do Pandas gera essas estatísticas para todas as colunas numéricas no DataFrame.
# MAGIC

# COMMAND ----------

#Exibir o resumo estatístico
walmart_pd.describe()  # Estatísticas descritivas

# COMMAND ----------

# MAGIC %md
# MAGIC ## Remover Linhas Duplicadas
# MAGIC
# MAGIC Em alguns casos, podemos ter **linhas duplicadas** nos dados, o que pode afetar a análise. Para garantir que cada observação seja única, aplicamos a função `drop_duplicates()` do Pandas. Essa função remove todas as linhas que são duplicadas, deixando apenas a primeira ocorrência de cada combinação única de valores.
# MAGIC
# MAGIC Usamos o parâmetro `inplace=True` para garantir que a remoção seja realizada diretamente no DataFrame, sem a necessidade de criar uma cópia.
# MAGIC

# COMMAND ----------

# Remover linhas duplicadas
walmart_pd.drop_duplicates(inplace=True)


# COMMAND ----------

# MAGIC %md
# MAGIC ## Verificar Valores Nulos
# MAGIC
# MAGIC Agora, vamos verificar se há **valores nulos** no conjunto de dados. A presença de valores nulos pode interferir nas análises subsequentes, por isso é importante identificá-los.
# MAGIC
# MAGIC O método `isnull()` retorna um DataFrame booleano onde `True` indica que o valor é nulo. Em seguida, usamos `sum()` para contar o número total de valores nulos em cada coluna.
# MAGIC

# COMMAND ----------

#Verificar valores nulos
walmart_pd.isnull().sum()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Converter `transaction_date` para Datetime
# MAGIC
# MAGIC Nesta etapa, estamos convertendo a coluna `transaction_date` para o tipo de dado **datetime**, que é mais adequado para análises de séries temporais, como agregações por data, visualizações e cálculos baseados em tempo.
# MAGIC
# MAGIC Usamos o método `pd.to_datetime()` do Pandas para realizar a conversão. O parâmetro `errors='coerce'` é utilizado para garantir que valores inválidos sejam transformados em `NaT` (Not a Time) em vez de gerar erros.
# MAGIC
# MAGIC Após a conversão, exibimos as primeiras linhas do DataFrame para verificar se a transformação foi bem-sucedida.
# MAGIC

# COMMAND ----------

#Converter transaction_date para datetime:
walmart_pd['transaction_date'] = pd.to_datetime(walmart_pd['transaction_date'], errors='coerce')
# Exibir as primeiras linhas do Pandas DataFrame
walmart_pd.head()

# COMMAND ----------

count_843 = walmart_pd[walmart_pd['product_id'] == 843]
count_843

# COMMAND ----------

# MAGIC %md
# MAGIC ### Converter para DataFrame do PySpark e Salvar em Parquet com Particionamento
# MAGIC
# MAGIC Após realizar as transformações e análises no DataFrame Pandas, podemos convertê-lo para um DataFrame PySpark para aproveitar o poder de processamento distribuído do Spark. Nesse processo, também podemos salvar os dados em formato **Parquet**, que é eficiente para consultas e compressão, além de particioná-los por colunas específicas para facilitar futuras consultas e análises.
# MAGIC
# MAGIC Neste caso, particionamos os dados por **ano** e **mês** (extraídos da coluna `transaction_date`) e por **localização da loja** (`store_location`). Isso cria uma estrutura de diretórios hierárquica, que melhora a organização e o desempenho em consultas distribuídas.
# MAGIC
# MAGIC #### Etapas:
# MAGIC 1. **Conversão para PySpark DataFrame**:
# MAGIC    Utilizamos a função `createDataFrame()` do PySpark para converter o DataFrame Pandas.
# MAGIC
# MAGIC 2. **Extração de Ano e Mês**:
# MAGIC    Adicionamos duas novas colunas (`year` e `month`) derivadas da coluna `transaction_date`.
# MAGIC
# MAGIC 3. **Particionamento e Salvamento**:
# MAGIC    Usamos o método `.write.partitionBy()` para salvar os dados em formato **Parquet** particionados por **ano**, **mês**, e **localização da loja** no Databricks File System (DBFS).
# MAGIC
# MAGIC

# COMMAND ----------

# Converter o DataFrame do Pandas para Spark DataFrame
walmart_spark_df = spark.createDataFrame(walmart_pd)

# Adicionar colunas de ano e mês extraídas de transaction_date
from pyspark.sql.functions import year, month

walmart_spark_df = walmart_spark_df.withColumn("year", year(walmart_spark_df["transaction_date"]))
walmart_spark_df = walmart_spark_df.withColumn("month", month(walmart_spark_df["transaction_date"]))

# Caminho para salvar no DBFS local
path_partitioned = '/Data/dados_transformed_partitioned.parquet'

# Salvar particionado por ano, mês e localização da loja
walmart_spark_df.write.partitionBy("year", "month", "store_location").parquet(path_partitioned)

