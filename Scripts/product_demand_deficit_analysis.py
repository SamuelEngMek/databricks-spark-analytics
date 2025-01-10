# Importação das bibliotecas necessárias
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, desc

# Inicializar o SparkSession
spark = SparkSession.builder.appName("Análise de Defasagem e Demanda").getOrCreate()

# Carregar os dados (ajuste o caminho para seu dataset)
dados_estoque = spark.read.format("parquet").option("header", "true").load("dbfs:/tmp/dados_transformed.parquet")

# Conversão de colunas para tipo correto (se necessário)
# A coluna 'forecasted_demand' é a demanda prevista, 'actual_demand' é a demanda real e 'inventory_level' é o nível de estoque
dados_estoque = dados_estoque.withColumn("defasagem", col("actual_demand") - col("inventory_level"))

# Filtragem dos produtos com maior defasagem e previsão de demanda
produtos_analise = dados_estoque.filter((col("defasagem") > 100) & (col("actual_demand") > 100)) \
                                .select("store_location", "product_id", "inventory_level", "actual_demand", "forecasted_demand", "defasagem") \
                                .orderBy(desc("defasagem"), desc("forecasted_demand"))

# Dividir os resultados por loja (store_location)
lojas_unicas = produtos_analise.select("store_location").distinct().collect()

# Para cada loja, filtrar os dados, salvar e exibir os resultados
for loja in lojas_unicas:
    loja_nome = loja["store_location"]
    tabela_loja = produtos_analise.filter(col("store_location") == loja_nome)
    
    # Caminho para salvar os dados no Databricks (DBFS)
    path_to_save = f"dbfs:/tables/{loja_nome}/Resultados.parquet"
    
    # Salvar a tabela de cada loja como arquivo Parquet ou Delta
    tabela_loja.write.format("parquet").mode("overwrite").save(path_to_save)
    
    # Opcional: Você pode também salvar como Delta (mais recomendado para big data)
    # tabela_loja.write.format("delta").mode("overwrite").save(path_to_save)
    
    # Exibir uma amostra dos resultados (opcional)
    print(f"Resultados salvos para a loja: {loja_nome}")
    tabela_loja.show(10)
    
# Exemplo de leitura dos dados salvos para validação (opcional)
# exemplo_leitura = spark.read.parquet(path_to_save)
# exemplo_leitura.show()
