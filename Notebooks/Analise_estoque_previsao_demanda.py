# Databricks notebook source
# MAGIC %md
# MAGIC # Análise de Estoque e Previsão de Demanda
# MAGIC
# MAGIC Este notebook apresenta uma análise dos dados relacionados à gestão de estoque e previsão de demanda de produtos em diferentes locais de loja. Utilizando PySpark, o objetivo principal é avaliar a quantidade de produtos vendidos, a quantidade em estoque, o impacto de promoções e as condições meteorológicas sobre a defasagem entre a demanda real e a previsão.
# MAGIC
# MAGIC ## Etapas abordadas:
# MAGIC - **Carregamento e pré-processamento dos dados**.
# MAGIC - **Identificação de produtos com estoque insuficiente**.
# MAGIC - **Cálculo da defasagem entre a demanda e o estoque disponível**.
# MAGIC - **Análise do impacto de promoções e variáveis externas** (como condições meteorológicas e feriados) na defasagem de estoque.
# MAGIC - **Cálculo da precisão das previsões de demanda** e a comparação entre acertos e erros na previsão.
# MAGIC
# MAGIC O notebook oferece uma visão geral das métricas de desempenho de estoque, fornecendo insights valiosos para a gestão eficiente de inventário e estratégias de reposição.
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ## Importação de Bibliotecas
# MAGIC
# MAGIC Neste primeiro passo, importamos as bibliotecas necessárias para manipulação de dados:
# MAGIC
# MAGIC - **Pandas**: para operações de manipulação de dados em formato de DataFrame.
# MAGIC - **NumPy**: para operações matemáticas e manipulação de arrays.
# MAGIC - **PySpark**: para processamento de dados distribuídos com Apache Spark, utilizando funções da biblioteca PySpark SQL.

# COMMAND ----------

import pandas as pd
import numpy as np
from pyspark.sql import functions as F
from pyspark.sql.functions import when, col

# COMMAND ----------

# MAGIC %md
# MAGIC ## Carregamento dos Dados como PySpark DataFrame
# MAGIC
# MAGIC Neste passo, carregamos os dados de um arquivo **Parquet** utilizando o PySpark. O formato Parquet é eficiente para armazenar dados estruturados e pode ser lido diretamente com o PySpark.
# MAGIC
# MAGIC Usamos o método `spark.read.parquet` para carregar os dados no formato Parquet e armazená-los em um DataFrame do PySpark. Também configuramos as opções `header=True` e `inferSchema=True` para garantir que o cabeçalho seja interpretado e que o tipo de dados seja inferido automaticamente.
# MAGIC

# COMMAND ----------

# Carregar os dados como PySpark DataFrame 
df = spark.read.parquet("/tmp/dados_transformed.parquet", header=True, inferSchema=True)

# Exibir o esquema do DataFrame
df.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Verificação de Valores Nulos nas Colunas
# MAGIC
# MAGIC Após carregar os dados, é essencial verificar se há valores nulos em qualquer uma das colunas do DataFrame. A presença de valores nulos pode afetar análises subsequentes, por isso é importante tratá-los adequadamente.
# MAGIC
# MAGIC Neste passo, utilizamos a função `select` em conjunto com `F.count` e `F.when` do PySpark para contar o número de valores nulos em cada coluna. A função `F.col(c).isNull()` verifica se o valor da coluna `c` é nulo. O resultado é exibido utilizando o método `show()`.

# COMMAND ----------

# Verificar valores nulos nas colunas
df.select([F.count(F.when(F.col(c).isNull(), c)).alias(c) for c in df.columns]).show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Definição das Colunas a Serem Mantidas para Gestão de Estoque e Previsão de Demanda
# MAGIC
# MAGIC Neste passo, selecionamos as colunas relevantes para o processo de gestão de estoque e previsão de demanda. Essas colunas contêm informações cruciais para entender o comportamento do estoque, as vendas, as promoções, e as condições que impactam a demanda dos produtos.
# MAGIC
# MAGIC A lista `columns_to_keep` inclui as colunas a serem mantidas no DataFrame para análises futuras:
# MAGIC
# MAGIC - **product_id**: Identificador único do produto.
# MAGIC - **quantity_sold**: Quantidade de unidades vendidas.
# MAGIC - **inventory_level**: Nível atual de estoque.
# MAGIC - **reorder_point**: Ponto de reposição do estoque.
# MAGIC - **reorder_quantity**: Quantidade a ser reposta quando o estoque atingir o ponto de reposição.
# MAGIC - **store_id**: Identificador único da loja.
# MAGIC - **store_location**: Localização da loja.
# MAGIC - **forecasted_demand**: Demanda prevista para o produto.
# MAGIC - **promotion_applied**: Indica se a promoção foi aplicada ao produto.
# MAGIC - **promotion_type**: Tipo da promoção aplicada (ex: desconto, promoção combinada, etc.).
# MAGIC - **weather_conditions**: Condições climáticas que podem influenciar a demanda.
# MAGIC - **holiday_indicator**: Indicador de feriado, que pode afetar as vendas.
# MAGIC - **weekday**: Dia da semana, relevante para padrões de vendas.
# MAGIC - **actual_demand**: Demanda real observada.

# COMMAND ----------

# Colunas a serem mantidas para Gestão de Estoque e Previsão de Demanda
columns_to_keep = [
    'product_id', 'quantity_sold', 'inventory_level', 'reorder_point', 'reorder_quantity', 
    'store_id', 'store_location', 'forecasted_demand', 'promotion_applied', 'promotion_type', 
    'weather_conditions', 'holiday_indicator', 'weekday', 'actual_demand'
]

# COMMAND ----------

# MAGIC %md
# MAGIC ## Filtragem das Colunas Relevantes
# MAGIC
# MAGIC Após definir as colunas importantes para a gestão de estoque e previsão de demanda, realizamos a filtragem do DataFrame para manter apenas essas colunas. Esse passo é crucial para simplificar o DataFrame e focar nas variáveis essenciais para a análise subsequente.
# MAGIC
# MAGIC Utilizamos o método `select` para selecionar as colunas definidas na lista `columns_to_keep` e criar um novo DataFrame `df_filtered`.
# MAGIC

# COMMAND ----------

# Filtrar as colunas relevantes
df_filtered = df.select(*columns_to_keep)

# Exibir as primeiras linhas do DataFrame filtrado para conferir
df_filtered.show(5)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Exibição do Esquema do DataFrame Filtrado
# MAGIC
# MAGIC Após realizar a filtragem das colunas relevantes, é importante verificar novamente a estrutura do DataFrame filtrado para garantir que as colunas selecionadas estão presentes e que os tipos de dados estão corretos.
# MAGIC
# MAGIC Utilizamos o método `printSchema()` para exibir o esquema do DataFrame filtrado, que nos mostra as colunas e seus respectivos tipos de dados.
# MAGIC

# COMMAND ----------

# Exibir o esquema do DataFrame filtrado
df_filtered.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Exibição das Estatísticas Descritivas para as Colunas Numéricas
# MAGIC
# MAGIC Neste passo, exibimos as estatísticas descritivas das colunas numéricas do DataFrame filtrado. As estatísticas descritivas fornecem informações importantes sobre a distribuição e as características dos dados, como:
# MAGIC
# MAGIC - **count**: Número de valores não nulos.
# MAGIC - **mean**: Média dos valores.
# MAGIC - **stddev**: Desvio padrão, que indica a dispersão dos dados.
# MAGIC - **min**: Valor mínimo.
# MAGIC - **max**: Valor máximo.
# MAGIC
# MAGIC A função `describe()` é usada para calcular essas estatísticas, e o método `show()` é utilizado para exibir o resultado.
# MAGIC
# MAGIC

# COMMAND ----------

# Exibir estatísticas descritivas para as colunas numéricas
df_filtered.describe().show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Contagem de Valores Únicos por Coluna
# MAGIC
# MAGIC Neste passo, realizamos a contagem de valores únicos em uma coluna específica, neste caso, o número de registros por loja. A contagem de valores únicos é útil para entender a distribuição dos dados em uma determinada categoria, como a quantidade de registros por localização de loja.
# MAGIC
# MAGIC Usamos o método `groupBy()` para agrupar os dados pela coluna `store_location` e, em seguida, aplicamos a função `count()` para contar o número de registros em cada grupo. O método `show()` é utilizado para exibir os resultados.
# MAGIC

# COMMAND ----------

# Contar valores únicos em uma coluna, por exemplo, numero de registors por loja
df_filtered.groupBy('store_location').count().show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Contagem de Valores Únicos na Coluna de Promoções Aplicadas
# MAGIC
# MAGIC Neste passo, contamos os valores únicos na coluna `promotion_applied`, que indica se uma promoção foi aplicada ou não. Essa análise é útil para entender quantas vezes as promoções foram aplicadas em relação ao total de registros.
# MAGIC
# MAGIC Utilizamos o método `groupBy()` para agrupar os dados pela coluna `promotion_applied` e, em seguida, aplicamos a função `count()` para contar o número de registros em cada grupo. O método `show()` é utilizado para exibir os resultados.
# MAGIC

# COMMAND ----------

# Contar valores únicos na coluna de promoções aplicadas
df_filtered.groupBy('promotion_applied').count().show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Contagem de Valores Únicos na Coluna de Condição Climática
# MAGIC
# MAGIC Neste passo, contamos os valores únicos na coluna `weather_conditions`, que representa as diferentes condições climáticas presentes no conjunto de dados. Essa análise pode ser útil para verificar como diferentes condições climáticas afetam as vendas ou a demanda de produtos.
# MAGIC
# MAGIC Usamos o método `groupBy()` para agrupar os dados pela coluna `weather_conditions` e, em seguida, aplicamos a função `count()` para contar o número de registros em cada condição climática. O método `show()` é utilizado para exibir os resultados.
# MAGIC

# COMMAND ----------

# Contar valores únicos na coluna de condição do clima
df_filtered.groupBy('weather_conditions').count().show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Contagem de Valores Únicos na Coluna de Dias da Semana
# MAGIC
# MAGIC Neste passo, contamos os valores únicos na coluna `weekday`, que representa os dias da semana em que as transações ocorreram. Essa análise pode ser útil para identificar padrões de comportamento, como quais dias da semana têm mais vendas ou demanda.
# MAGIC
# MAGIC Usamos o método `groupBy()` para agrupar os dados pela coluna `weekday` e, em seguida, aplicamos a função `count()` para contar o número de registros em cada dia da semana. O método `show()` é utilizado para exibir os resultados.
# MAGIC

# COMMAND ----------

# Contar valores únicos na coluna de dias da semana
df_filtered.groupBy('weekday').count().show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Contagem de Valores Distintos na Coluna de Quantidade Vendida
# MAGIC
# MAGIC Neste passo, exibimos os valores distintos da coluna `quantity_sold`, que representa a quantidade de unidades vendidas. Verificar os valores distintos ajuda a entender a variação nas quantidades de produtos vendidos, identificando se há valores duplicados ou se existem quantidades únicas de vendas.
# MAGIC
# MAGIC Usamos o método `select()` para selecionar a coluna `quantity_sold`, seguido de `distinct()` para obter os valores únicos dessa coluna. O método `show()` é utilizado para exibir os resultados.

# COMMAND ----------

# Conta valores distintos na coluna de quantidade vendida
df_filtered.select("quantity_sold").distinct().show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Identificação dos Produtos Mais Vendidos por Localização de Loja
# MAGIC
# MAGIC Neste passo, estamos agrupando os dados por `store_location` (localização da loja) e `product_id` (identificador do produto) para calcular o total de unidades vendidas de cada produto em cada loja. Esse processo permite identificar quais produtos são mais vendidos em cada localização, uma informação importante para a gestão de estoque e planejamento de demanda.
# MAGIC
# MAGIC Utilizamos a função `groupBy()` para agrupar os dados pelas colunas `store_location` e `product_id`. Em seguida, usamos `agg()` com a função `F.sum()` para somar a quantidade vendida de cada produto. O resultado é ordenado pela localização da loja e pela quantidade total vendida, de forma decrescente, usando o método `orderBy()`.
# MAGIC
# MAGIC O método `show(10)` exibe os 10 primeiros resultados, apresentando as lojas e os produtos mais vendidos.
# MAGIC

# COMMAND ----------

# Identifica os produtos mais vendidos por localizaçao
produtos_mais_vendidos = df_filtered.groupBy("store_location", "product_id") \
    .agg(F.sum("quantity_sold").alias("total_quantity_sold")) \
    .orderBy("store_location", "total_quantity_sold", ascending=False)

produtos_mais_vendidos.show(10)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Criação do DataFrame de Estoque e Demanda
# MAGIC
# MAGIC Neste passo, criamos uma nova coluna no DataFrame `df_filtered` para verificar se o estoque é suficiente para atender à demanda real de cada produto. A coluna `estoque_suficiente` será preenchida com `True` se o nível de estoque (`inventory_level`) for maior ou igual à demanda real (`actual_demand`), e `False` caso contrário. Essa verificação é fundamental para a gestão de estoque, permitindo identificar produtos com risco de falta de estoque.
# MAGIC
# MAGIC Usamos a função `withColumn()` para adicionar a nova coluna, aplicando a lógica condicional com a função `when()` para comparar o nível de estoque com a demanda real. A expressão `when()` cria uma coluna booleana, retornando `True` ou `False` com base na condição.
# MAGIC
# MAGIC Em seguida, usamos `select()` para exibir as colunas relevantes e confirmar o resultado da verificação.
# MAGIC

# COMMAND ----------

# Criar o DataFrame estoque_Demanda com a lógica de verificação de estoque suficiente
estoque_Demanda = df_filtered.withColumn("estoque_suficiente", when(col("inventory_level") >= col("actual_demand"), True).otherwise(False))

# Exibir as colunas desejadas para verificação
estoque_Demanda.select("store_location", "product_id", "inventory_level", "actual_demand", "estoque_suficiente").show(10)


# COMMAND ----------

# MAGIC %md
# MAGIC ## Identificação de Estoques Insuficientes
# MAGIC
# MAGIC Nesta seção, analisamos os itens com estoque insuficiente em relação à demanda real, destacando a magnitude dessa insuficiência por meio de uma nova métrica chamada **defasagem**.
# MAGIC
# MAGIC ### Passos:
# MAGIC 1. **Filtragem de Estoques Insuficientes**:
# MAGIC    - Utilizamos o DataFrame `estoque_Demanda` para filtrar os itens em que o estoque disponível não é suficiente para atender à demanda real.
# MAGIC    - A condição de insuficiência é definida pela coluna `estoque_suficiente` sendo igual a `False`.
# MAGIC
# MAGIC 2. **Cálculo da Defasagem**:
# MAGIC    - Adicionamos uma nova coluna chamada `defasagem`, que calcula a diferença entre a demanda real (`actual_demand`) e o nível de estoque disponível (`inventory_level`).
# MAGIC    - A fórmula utilizada foi:  
# MAGIC      **defasagem = actual_demand - inventory_level**
# MAGIC
# MAGIC 3. **Exibição dos Dados**:
# MAGIC    - Visualizamos as primeiras 10 linhas do DataFrame resultante, destacando as colunas mais relevantes:  
# MAGIC      - `store_location`: Localização da loja.
# MAGIC      - `product_id`: Identificador do produto.
# MAGIC      - `inventory_level`: Estoque disponível.
# MAGIC      - `actual_demand`: Demanda real.
# MAGIC      - `defasagem`: Quantidade insuficiente de estoque.
# MAGIC

# COMMAND ----------

# Filtra o DataFrame para encontrar os itens com estoque insuficiente
estoque_Insuficiente_df = estoque_Demanda.filter(estoque_Demanda.estoque_suficiente == False)

# Adiciona uma nova coluna "defasagem" calculando a diferença entre a demanda e o estoque
estoque_Insuficiente_df = estoque_Insuficiente_df.withColumn("defasagem", F.col("actual_demand") - F.col("inventory_level"))

# Mostra as primeiras 10 linhas do DataFrame com a coluna "defasagem"
estoque_Insuficiente_df.select("store_location", "product_id", "inventory_level", "actual_demand", "defasagem").show(10)


# COMMAND ----------

# MAGIC %md
# MAGIC ## Ordenação dos Produtos com Maior Defasagem de Estoque
# MAGIC
# MAGIC Neste passo, estamos ordenando os produtos com maior defasagem de estoque. A defasagem é calculada com base na diferença entre o nível de estoque e a demanda real de cada produto. Produtos com maior defasagem indicam que o estoque não é suficiente para atender à demanda e podem precisar ser priorizados para reposição.
# MAGIC
# MAGIC Usamos o método `orderBy()` para ordenar os produtos pela coluna `defasagem` em ordem decrescente, ou seja, do produto com maior defasagem para o menor. A função `select()` é utilizada para exibir as colunas relevantes, como a localização da loja, o identificador do produto, o nível de estoque, a demanda real e a defasagem.
# MAGIC
# MAGIC O comando `show(10)` exibe os 10 produtos com maior defasagem, permitindo uma visualização rápida dos itens que mais necessitam de reposição.
# MAGIC

# COMMAND ----------

# Ordenar os produtos com maior defasagem
defasagem_ordenada_df = estoque_Insuficiente_df.orderBy("defasagem", ascending=False)

# Mostrar os 10 produtos com maior defasagem
defasagem_ordenada_df.select("store_location", "product_id", "inventory_level", "actual_demand", "defasagem").show(10)


# COMMAND ----------

# MAGIC %md
# MAGIC # Filtragem de Dados de Promoções Aplicadas e Análise de Defasagem
# MAGIC
# MAGIC Neste passo, adicionamos uma nova coluna chamada `promotion_status` para indicar se uma promoção foi aplicada ou não aos produtos. O objetivo é analisar o impacto das promoções em produtos que estão com defasagem de estoque. Produtos com promoção aplicada e com estoque insuficiente podem exigir um planejamento especial para garantir que o estoque esteja adequado para atender à demanda gerada pela promoção.
# MAGIC
# MAGIC Usamos o método `withColumn()` para criar a coluna `promotion_status`, que recebe o valor "Promoção Aplicada" quando a coluna `promotion_applied` for `True` e "Sem Promoção" caso contrário. 
# MAGIC
# MAGIC Em seguida, utilizamos o método `select()` para exibir as colunas relevantes: a localização da loja, o identificador do produto, o status da promoção, a demanda real e a defasagem de estoque. Os resultados são ordenados pela coluna `defasagem` de forma **decrescente** utilizando o método `orderBy()`, permitindo que os produtos com maior defasagem de estoque apareçam primeiro.
# MAGIC
# MAGIC O comando `show(10)` exibe os 10 primeiros resultados, facilitando a visualização dos produtos com promoções e suas respectivas defasagens de estoque.
# MAGIC
# MAGIC

# COMMAND ----------

# Adicionar uma nova coluna 'promotion_status' para indicar se a promoção foi aplicada ou não
impacto_promocao_df = estoque_Insuficiente_df.withColumn("promotion_status",when(estoque_Insuficiente_df.promotion_applied == True, "Promoção Aplicada").otherwise("Sem Promoção")
)

# Exibir os dados, incluindo a nova coluna 'promotion_status'
impacto_promocao_df.select("store_location", "product_id", "promotion_status", "actual_demand", "defasagem").orderBy("defasagem", ascending=False).show(10)


# COMMAND ----------

# MAGIC %md
# MAGIC ## Análise da Média da Defasagem por Tipo de Promoção
# MAGIC
# MAGIC Neste passo, estamos analisando a média da defasagem de estoque para os diferentes tipos de promoção. A ideia é entender se há uma diferença significativa na defasagem entre os produtos que receberam diferentes tipos de promoção.
# MAGIC
# MAGIC Usamos o método `groupBy()` para agrupar os dados pela coluna `promotion_type`, que representa o tipo de promoção aplicada. Em seguida, utilizamos `agg()` com a função `F.avg()` para calcular a média da defasagem para cada tipo de promoção. O comando `show()` exibe os resultados.
# MAGIC

# COMMAND ----------

# Analisando a média da defasagem por tipo de promoção
impacto_promocao_df.groupBy("promotion_type").agg(F.avg("defasagem").alias("media_defasagem")).show()

# Comparando produtos com e sem promoção
estoque_sem_promocao_df = estoque_Insuficiente_df.filter(estoque_Insuficiente_df.promotion_applied == False)
estoque_com_promocao_df = estoque_Insuficiente_df.filter(estoque_Insuficiente_df.promotion_applied == True)

# Média da defasagem para produtos sem promoção
estoque_sem_promocao_df.agg(F.avg("defasagem").alias("media_defasagem_sem_promocao")).show()

# Média da defasagem para produtos com promoção
estoque_com_promocao_df.agg(F.avg("defasagem").alias("media_defasagem_com_promocao")).show()


# COMMAND ----------

# MAGIC %md
# MAGIC ## Produtos com Maior Defasagem e Previsão de Demanda Mais Alta
# MAGIC
# MAGIC Neste passo, estamos analisando os produtos com maior defasagem de estoque, especificamente aqueles com uma defasagem superior a 100 unidades. A ideia é identificar produtos que não só estão com estoque insuficiente, mas também têm uma previsão de demanda (`forecasted_demand`) alta, o que pode indicar um risco ainda maior de falta de estoque no futuro.
# MAGIC
# MAGIC Utilizamos o método `filter()` para selecionar os produtos com uma defasagem superior a 100. Em seguida, aplicamos o `select()` para exibir as colunas relevantes, incluindo a localização da loja, o identificador do produto, o nível de estoque, a demanda real, a previsão de demanda e a defasagem. Para facilitar a análise, os resultados são ordenados pela previsão de demanda e pela defasagem, de forma **decrescente**.
# MAGIC
# MAGIC O comando `show(10)` exibe os 10 primeiros resultados, permitindo uma visão clara dos produtos com maior risco de falta de estoque devido à alta demanda prevista e à defasagem de estoque.
# MAGIC
# MAGIC

# COMMAND ----------

# Produtos com maior defasagem e previsão de demanda mais alta
estoque_Insuficiente_df.filter(estoque_Insuficiente_df.defasagem > 100).select("store_location", "product_id", "inventory_level", "actual_demand", "forecasted_demand", "defasagem").orderBy(["forecasted_demand", "defasagem"], ascending=[False, False]).show(10)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Análise de Erro na Previsão de Demanda
# MAGIC
# MAGIC Neste passo, estamos analisando a acurácia da previsão de demanda comparando a diferença entre a demanda real e a previsão de demanda. O objetivo é identificar os produtos cujas previsões estão dentro de uma margem de erro aceitável e os produtos com previsões significativamente imprecisas.
# MAGIC
# MAGIC ### Definindo a Margem de Erro Aceitável
# MAGIC Primeiro, definimos uma margem de erro de 20 unidades. Isso significa que qualquer diferença entre a demanda real e a previsão de demanda abaixo ou igual a 20 é considerada um "acerto", e qualquer diferença superior a 20 é considerada um "erro".
# MAGIC

# COMMAND ----------

# Definindo a margem de erro aceitável
margem_erro = 20

# Calcular a diferença entre demanda real e previsão de demanda
estoque_Demanda = estoque_Demanda.withColumn("erro_previsao", F.abs(col("actual_demand") - col("forecasted_demand")))

# Separar os produtos em acertos e erros
acertos_df = estoque_Demanda.filter(estoque_Demanda.erro_previsao <= margem_erro)
erros_df = estoque_Demanda.filter(estoque_Demanda.erro_previsao > margem_erro)

# Contar os acertos e erros
acertos = acertos_df.count()
erros = erros_df.count()

# Exibir o resultado
print(f"Produtos com previsão correta (dentro da margem de erro de {margem_erro}): {acertos}")
print(f"Produtos com previsão errada (fora da margem de erro de {margem_erro}): {erros}")


# COMMAND ----------

# MAGIC %md
# MAGIC ## Análise da Defasagem por Dia da Semana
# MAGIC
# MAGIC Neste passo, estamos analisando a média da defasagem de estoque por dia da semana. A ideia é verificar se há variações na defasagem ao longo da semana, o que pode ajudar a entender melhor os padrões de demanda e estoque com base no dia.
# MAGIC
# MAGIC ### Agrupamento por Dia da Semana
# MAGIC Usamos o método `groupBy()` para agrupar os dados pela coluna `weekday`, que representa o dia da semana. Em seguida, aplicamos a função `agg()` com a função `F.avg()` para calcular a média da defasagem para cada dia da semana.
# MAGIC

# COMMAND ----------

# Agrupar por dia da semana e verificar a média da defasagem
estoque_Insuficiente_df.groupBy("weekday").agg(F.avg("defasagem").alias("media_defasagem")).show()


# COMMAND ----------

# MAGIC %md
# MAGIC ## Análise do Impacto das Condições Meteorológicas e Feriados na Defasagem
# MAGIC
# MAGIC Neste passo, estamos analisando o impacto das condições meteorológicas e dos feriados na defasagem de estoque. A ideia é verificar se as condições externas, como o clima e a ocorrência de feriados, têm influência sobre a diferença entre a demanda real e o estoque disponível.
# MAGIC
# MAGIC ### Agrupamento por Condições Meteorológicas e Feriados
# MAGIC Usamos o método `groupBy()` para agrupar os dados pelas colunas `weather_conditions` (condições meteorológicas) e `holiday_indicator` (indicador de feriado). Em seguida, aplicamos a função `agg()` com a função `F.avg()` para calcular a média da defasagem para cada combinação de condições meteorológicas e feriados.
# MAGIC

# COMMAND ----------

# Verificar o impacto das condições meteorológicas e feriados na defasagem
estoque_Insuficiente_df.groupBy("weather_conditions", "holiday_indicator").agg(F.avg("defasagem").alias("media_defasagem")).show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Conclusão
# MAGIC
# MAGIC Este projeto apresentou uma análise detalhada sobre a gestão de estoque e o impacto das promoções nas vendas de produtos, com foco em identificar as oportunidades de melhoria na previsão de demanda e na adequação do estoque. A seguir, estão os principais insights obtidos a partir dos dados analisados:
# MAGIC
# MAGIC 1. **Produtos Mais Vendidos**: Identificamos os produtos que tiveram as maiores quantidades vendidas nas lojas, o que ajuda a priorizar o abastecimento e garantir que itens populares estejam sempre disponíveis para os clientes.
# MAGIC
# MAGIC 2. **Produtos com Maior Defasagem de Estoque**: A análise de defasagem revelou os produtos com a maior diferença entre a quantidade de estoque disponível e a demanda real. Estes itens exigem atenção imediata para evitar rupturas de estoque que possam impactar negativamente as vendas e a satisfação do cliente.
# MAGIC
# MAGIC 3. **Impacto das Promoções nas Defasagens de Estoque**: A filtragem de dados revelou a relação entre as promoções e a defasagem de estoque. Produtos com promoções aplicadas tendem a apresentar uma maior defasagem, o que sugere que as promoções podem estar gerando uma demanda não totalmente atendida. Isso destaca a importância de ajustar o estoque de forma mais eficaz antes de aplicar promoções, para que as lojas possam atender à demanda extra sem problemas.
# MAGIC
# MAGIC 4. **Análise da Média de Defasagem por Tipo de Promoção**: A comparação entre os diferentes tipos de promoção mostrou que o "Percentage Discount" e "BOGO" (Buy One Get One) estão associados a médias de defasagem mais altas. Isso indica que as promoções desse tipo podem estar exacerbando a escassez de produtos e exigem maior controle no planejamento de estoques.
# MAGIC
# MAGIC 5. **Produtos com Maior Defasagem e Previsão de Demanda Alta**: Identificamos produtos com previsão de alta demanda futura e alta defasagem de estoque. Esses produtos representam um risco significativo de rupturas no futuro, o que reforça a necessidade de estratégias de reposição de estoque mais precisas para atender à demanda futura e evitar vendas perdidas.
# MAGIC
# MAGIC 6. **Análise da Previsão de Demanda**: A análise das previsões de demanda revelou que, embora muitas previsões estejam dentro da margem de erro aceitável (20 unidades), um grande número de produtos apresenta previsões incorretas, o que pode resultar em falhas no planejamento de estoque. Isso destaca a importância de melhorar a precisão da previsão para garantir um alinhamento mais eficiente entre a demanda e o estoque.
# MAGIC
# MAGIC ### Recomendações:
# MAGIC
# MAGIC - **Ajuste de Estoque para Promoções**: É fundamental garantir que o estoque seja ajustado adequadamente antes da aplicação de promoções, considerando o aumento esperado na demanda.
# MAGIC   
# MAGIC - **Melhoria nas Previsões de Demanda**: O modelo de previsão de demanda precisa ser ajustado para melhorar sua precisão e reduzir a margem de erro, o que ajudará na melhor adequação dos estoques.
# MAGIC   
# MAGIC - **Foco em Produtos Críticos**: A priorização dos produtos com maior defasagem e previsão de alta demanda pode ajudar a evitar rupturas e maximizar as vendas.
# MAGIC   
# MAGIC - **Análise Contínua**: Uma análise contínua dos dados de vendas e estoque, juntamente com a revisão das previsões de demanda e impacto das promoções, ajudará a manter a eficiência operacional e a satisfação do cliente.
# MAGIC
# MAGIC Essa análise oferece uma visão estratégica valiosa para a otimização do gerenciamento de estoque e a melhoria contínua das operações de vendas nas lojas.
# MAGIC
