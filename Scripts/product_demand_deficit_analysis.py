from pyspark.sql import SparkSession
from pyspark.sql.functions import col, month, when, sum, avg
from pyspark.sql import functions as f

# Inicializar a sessão Spark
spark = SparkSession.builder.appName("Estoque e Previsao de Demanda").getOrCreate()

# Carregar os dados (modifique o caminho de acordo com a localização dos dados no DBFS)
df = spark.read.parquet("/data/dados_transformed_partitioned.parquet")

# Adicionar coluna de mês
df = df.withColumn("month", month(col("transaction_date")))

# Análise de Vendas Sazonais: Total de vendas por mês
seasonal_sales = df.groupBy("month").agg(sum("quantity_sold").alias("total_sales"))
seasonal_sales.show()

# Análise de Performance de Promoções: Vendas totais com e sem promoção
promotion_performance = df.groupBy("promotion_applied").agg(sum("quantity_sold").alias("total_sales"))
promotion_performance.show()

# Erro de Previsão: Diferença entre a demanda prevista e a demanda real
df = df.withColumn("forecast_error", f.abs(col("forecasted_demand") - col("actual_demand")))
forecast_error = df.groupBy("month").agg(avg("forecast_error").alias("average_error"))
forecast_error.show()

# Impacto dos 'Stockouts': Contagem de 'stockouts' por produto e mês
df = df.withColumn("stockout_occurred", when(col("stockout_indicator") == "True", 1).otherwise(0))
stockout_impact = df.groupBy("product_id", "month").agg(sum("stockout_occurred").alias("stockouts_count"))
stockout_impact.show()

# Análise de Performance de Lojas: Vendas totais por local de loja
store_performance = df.groupBy("store_location").agg(sum("quantity_sold").alias("total_sales"))
store_performance.show()

# Análise de Vendas por Dia da Semana: Total de vendas por dia da semana
weekday_sales = df.groupBy("weekday").agg(sum("quantity_sold").alias("total_sales"))
weekday_sales.show()

# Análise de Vendas por Condições Climáticas: Total de vendas por condições climáticas
weather_sales = df.groupBy("weather_conditions").agg(sum("quantity_sold").alias("total_sales"))
weather_sales.show()

# Análise de Vendas por Produto: Total de vendas por produto e mês
product_sales = df.groupBy("product_id", "month").agg(sum("quantity_sold").alias("total_sales"))
product_sales.show()

# Verificar produtos com estoque abaixo do ponto de reposição
df = df.withColumn("inventory_below_reorder_point", col("inventory_level") < col("reorder_point"))
inventory_issues = df.groupBy("inventory_below_reorder_point").count()
inventory_issues.show()

# Salvar os resultados processados em Parquet
seasonal_sales.write.parquet("/data/seasonal_sales.parquet")
promotion_performance.write.parquet("/data/promotion_performance.parquet")
forecast_error.write.parquet("/data/forecast_error.parquet")
stockout_impact.write.parquet("/data/stockout_impact.parquet")
store_performance.write.parquet("/data/store_performance.parquet")
weekday_sales.write.parquet("/data/weekday_sales.parquet")
weather_sales.write.parquet("/data/weather_sales.parquet")
product_sales.write.parquet("/data/product_sales.parquet")
inventory_issues.write.parquet("/data/inventory_issues.parquet")

# Fechar a sessão Spark
spark.stop()