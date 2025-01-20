from pyspark.sql import SparkSession
from pyspark.sql.functions import col, month, when, sum, avg
from pyspark.sql import functions as f

# Inicializar a sessão Spark
spark = SparkSession.builder.appName("Estoque e Previsao de Demanda").getOrCreate()

# Carregar os dados (modifique o caminho de acordo com a localização dos dados no DBFS)
df = spark.read.parquet("dbfs:/Data/dados_transformed_partitioned.parquet")

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

# Análise de Performance de Lojas: Vendas totais por local de loja
store_performance = df.groupBy("store_location").agg(sum("quantity_sold").alias("total_sales"))
store_performance.show()

# Análise de Vendas por Produto: Total de vendas por produto e mês
product_sales = df.groupBy("product_id", "month").agg(sum("quantity_sold").alias("total_sales"))
product_sales.show()

# Verificar produtos com estoque abaixo do ponto de reposição
df = df.withColumn("inventory_below_reorder_point", col("inventory_level") < col("reorder_point"))
inventory_issues = df.groupBy("inventory_below_reorder_point").count()
inventory_issues.show()

# Salvar os resultados processados em Parquet
seasonal_sales.write.parquet("/Data/seasonal_sales.parquet")
promotion_performance.write.parquet("/Data/promotion_performance.parquet")
forecast_error.write.parquet("/Data/forecast_error.parquet")
store_performance.write.parquet("/Data/store_performance.parquet")
product_sales.write.parquet("/Data/product_sales.parquet")
inventory_issues.write.parquet("/Data/inventory_issues.parquet")

# Fechar a sessão Spark
spark.stop()