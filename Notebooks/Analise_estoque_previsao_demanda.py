# Databricks notebook source
# MAGIC %md
# MAGIC # Análise de Estoque e Previsão de Demanda
# MAGIC
# MAGIC Este notebook tem como objetivo analisar dados de vendas, estoque e demanda de produtos para ajudar a identificar padrões, otimizar a gestão de estoque e melhorar a precisão das previsões de demanda. As variáveis analisadas incluem `product_id`, `quantity_sold`, `inventory_level`, `reorder_point`, `reorder_quantity`, `forecasted_demand`, entre outras.
# MAGIC
# MAGIC ### Carregar a Biblioteca PySpark
# MAGIC
# MAGIC Para utilizar o PySpark, devemos primeiro importar a biblioteca necessária para a manipulação dos dados em um ambiente distribuído. Abaixo, estamos carregando a biblioteca PySpark e a função necessária para trabalhar com as colunas de DataFrame.
# MAGIC
# MAGIC ## Carregar Dados e Preparar o Ambiente
# MAGIC
# MAGIC Primeiro, vamos carregar os dados e preparar o ambiente de análise. Assumimos que os dados já foram armazenados no DBFS em formato Parquet e que estão prontos para análise. 
# MAGIC

# COMMAND ----------

# DBTITLE 1,1
# Bibliotecas PySpark
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum, abs, month, avg, when

# Carregar os dados como PySpark DataFrame
df = spark.read.parquet("/data/dados_transformed_partitioned.parquet", header=True, inferSchema=True)

# Exibir o esquema do DataFrame
df.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Análise de Vendas Sazonais
# MAGIC
# MAGIC Nesta etapa, vamos analisar as vendas sazonais ao longo dos meses. Para isso, criaremos uma nova coluna `month`, que extrai o mês da data da transação, e depois agruparemos os dados por mês para somar as quantidades vendidas (`quantity_sold`).
# MAGIC

# COMMAND ----------

# Criar a coluna 'month' extraindo o mês da coluna 'transaction_date'
df = df.withColumn("month", month(col("transaction_date")))

# Agregar as vendas por mês
seasonal_sales = df.groupBy("month").agg(sum("quantity_sold").alias("total_sales"))

# Exibir o resultado
seasonal_sales.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Análise de Desempenho de Promoções
# MAGIC
# MAGIC Agora, vamos analisar o impacto das promoções nas vendas. Para isso, vamos agrupar os dados pela coluna `promotion_applied` e somar as quantidades vendidas (`quantity_sold`) para cada grupo. Esse processo nos ajudará a entender se as promoções influenciam significativamente as vendas.
# MAGIC

# COMMAND ----------

# Agrupar por 'promotion_applied' e somar as quantidades vendidas
promotion_performance = df.groupBy("promotion_applied").agg(sum("quantity_sold").alias("total_sales"))

# Exibir o resultado
promotion_performance.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Análise de Erro de Previsão de Demanda
# MAGIC
# MAGIC Nesta etapa, vamos calcular o erro absoluto de previsão de demanda. Para isso, criamos uma nova coluna chamada `forecast_error`, que é a diferença absoluta entre a demanda prevista (`forecasted_demand`) e a demanda real observada (`actual_demand`). Em seguida, vamos agrupar os dados por mês e calcular a média do erro de previsão para cada mês.
# MAGIC

# COMMAND ----------

# Calcular o erro absoluto da previsão de demanda
df = df.withColumn("forecast_error", abs(col("forecasted_demand") - col("actual_demand")))

# Calcular a média do erro de previsão por mês
df.groupBy("month").agg(avg("forecast_error").alias("average_error")).orderBy("average_error", ascending=False).show()


# COMMAND ----------

# MAGIC %md
# MAGIC ### Análise de Estoque Abaixo do Ponto de Reposição
# MAGIC
# MAGIC Nesta etapa, vamos verificar quantos produtos têm o nível de estoque abaixo do ponto de reposição. Para isso, criamos uma nova coluna chamada `inventory_below_reorder_point`, que indica se o nível de estoque (`inventory_level`) é inferior ao ponto de reposição (`reorder_point`). Em seguida, agrupamos os dados por essa nova coluna para contar quantos produtos estão abaixo e acima do ponto de reposição.
# MAGIC

# COMMAND ----------

# Criar a coluna 'inventory_below_reorder_point' que verifica se o estoque está abaixo do ponto de reposição
df = df.withColumn("inventory_below_reorder_point", col("inventory_level") < col("reorder_point"))

# Contar quantos produtos estão abaixo e acima do ponto de reposição
df.groupBy("inventory_below_reorder_point").count().show()


# COMMAND ----------

# MAGIC %md
# MAGIC ### Análise de Desempenho por Localização da Loja
# MAGIC
# MAGIC Nesta etapa, vamos analisar o desempenho de vendas por localização da loja. Para isso, agrupamos os dados pela coluna `store_location` e somamos a quantidade de unidades vendidas (`quantity_sold`) para calcular o total de vendas por localização de loja.
# MAGIC

# COMMAND ----------

# Agrupar por localização da loja e somar a quantidade de unidades vendidas
store_performance = df.groupBy("store_location").agg(sum("quantity_sold").alias("total_sales")).orderBy("total_sales", ascending=False)

# Exibir os resultados
store_performance.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Análise de Vendas por Dia da Semana
# MAGIC
# MAGIC Nesta etapa, vamos analisar as vendas totais por dia da semana. Para isso, agrupamos os dados pela coluna `weekday` e somamos a quantidade de unidades vendidas (`quantity_sold`) para calcular o total de vendas por dia da semana.
# MAGIC

# COMMAND ----------

# Agrupar por dia da semana e somar a quantidade de unidades vendidas
weekday_sales = df.groupBy("weekday").agg(sum("quantity_sold").alias("total_sales")).orderBy("total_sales", ascending=False)

# Exibir os resultados
weekday_sales.show()


# COMMAND ----------

# MAGIC %md
# MAGIC ### Análise de Vendas por Condições Climáticas
# MAGIC
# MAGIC Nesta etapa, vamos analisar as vendas totais de produtos com base nas condições climáticas. Para isso, agrupamos os dados pela coluna `weather_conditions` e somamos a quantidade de unidades vendidas (`quantity_sold`) para calcular o total de vendas por condição climática.
# MAGIC

# COMMAND ----------

# Agrupar por condições climáticas e somar a quantidade de unidades vendidas
weather_sales = df.groupBy("weather_conditions").agg(sum("quantity_sold").alias("total_sales")).orderBy("total_sales", ascending=False)

# Exibir os resultados
weather_sales.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Análise de Vendas por Produto e Mês
# MAGIC
# MAGIC Nesta etapa, vamos analisar as vendas totais de produtos, agrupadas por `product_id` e `month`. O objetivo é calcular o total de unidades vendidas de cada produto em cada mês.
# MAGIC

# COMMAND ----------

# Agrupar por ID do produto e mês, somando a quantidade de unidades vendidas
product_sales = df.groupBy("product_id", "month").agg(sum("quantity_sold").alias("total_sales")).orderBy("total_sales", ascending=False)

# Exibir os resultados
product_sales.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Análise do Impacto dos "Stockouts" por Produto e Mês
# MAGIC
# MAGIC Nesta etapa, vamos calcular o impacto dos "stockouts", ou seja, a quantidade de vezes que ocorreu falta de estoque, agrupados por `product_id` e `month`. Primeiramente, convertimos a coluna `stockout_indicator`, que contém valores booleanos ('True' ou 'False'), em 1 e 0. Em seguida, calculamos a soma de "stockouts" por produto e mês.
# MAGIC

# COMMAND ----------

# Converter 'True' para 1 e 'False' para 0
df = df.withColumn("stockout_occurred", when(col("stockout_indicator") == "True", 1).otherwise(0))

# Agora podemos calcular a soma dos 'stockouts' por produto e mês
stockout_impact = df.groupBy("product_id", "month").agg(sum("stockout_occurred").alias("stockouts_count")).orderBy("stockouts_count", ascending=False).show()
