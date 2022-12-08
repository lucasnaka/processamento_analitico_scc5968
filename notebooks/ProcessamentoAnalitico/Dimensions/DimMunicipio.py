# Databricks notebook source
# MAGIC %run ../utils_functions

# COMMAND ----------

import pandas as pd
from pyspark.sql.types import IntegerType

# COMMAND ----------

layer = 'silver'
path = 'AcidentesDatatran'
table_name = "acidente_table"
query = """
    SELECT DISTINCT
        municipio AS nom_municipio,
        uf AS estado_sigla
    FROM acidente_table
"""
df_municipios = extract_data(path, layer, file_type='parquet', query=query, table_name=table_name)

layer = 'bronze'
path = 'Ibge'
df_municipio_ibge = extract_data(path, layer, file_type='csv', delimiter=',', encoding='utf-8')

# COMMAND ----------

df_municipio_ibge = df_municipio_ibge.withColumn("nom_municipio", F.upper(F.col("nom_municipio")))

@F.pandas_udf('string')
def strip_accents(s: pd.Series) -> pd.Series:
    return s.str.normalize('NFKD').str.encode('ascii', errors='ignore').str.decode('utf-8')

df_municipio_ibge = df_municipio_ibge.withColumn('nom_municipio', strip_accents('nom_municipio'))
df_municipio_ibge = df_municipio_ibge.withColumnRenamed("sigla_estado", "estado_sigla")

# COMMAND ----------

df_municipios = df_municipios.join(df_municipio_ibge, how="left", on=["nom_municipio", "estado_sigla"])

df_municipios = df_municipios.withColumn("estado", F.when(F.col("estado_sigla")=="AC", "Acre")
                                                    .when(F.col("estado_sigla")=="AL", "Alagoas")
                                                    .when(F.col("estado_sigla")=="AM", "Amazonas")
                                                    .when(F.col("estado_sigla")=="AP", "Amapá")
                                                    .when(F.col("estado_sigla")=="BA", "Bahia")
                                                    .when(F.col("estado_sigla")=="CE", "Ceará")
                                                    .when(F.col("estado_sigla")=="DF", "Distrito Federal")
                                                    .when(F.col("estado_sigla")=="ES", "Espírito Santo")
                                                    .when(F.col("estado_sigla")=="GO", "Goiás")
                                                    .when(F.col("estado_sigla")=="MA", "Maranhão")
                                                    .when(F.col("estado_sigla")=="MG", "Minas Gerais")
                                                    .when(F.col("estado_sigla")=="MS", "Mato Grosso do Sul")
                                                    .when(F.col("estado_sigla")=="MT", "Mato Grosso")
                                                    .when(F.col("estado_sigla")=="PA", "Pará")
                                                    .when(F.col("estado_sigla")=="PB", "Paraíba")
                                                    .when(F.col("estado_sigla")=="PE", "Pernambuco")
                                                    .when(F.col("estado_sigla")=="PI", "Piauí")
                                                    .when(F.col("estado_sigla")=="PR", "Paraná")
                                                    .when(F.col("estado_sigla")=="RJ", "Rio de Janeiro")
                                                    .when(F.col("estado_sigla")=="RN", "Rio Grande do Norte")
                                                    .when(F.col("estado_sigla")=="RO", "Rondônia")
                                                    .when(F.col("estado_sigla")=="RR", "Roraima")
                                                    .when(F.col("estado_sigla")=="RS", "Rio Grande do Sul")
                                                    .when(F.col("estado_sigla")=="SC", "Santa Catarina")
                                                    .when(F.col("estado_sigla")=="SE", "Sergipe")
                                                    .when(F.col("estado_sigla")=="SP", "São Paulo")
                                                    .when(F.col("estado_sigla")=="TO", "Tocantins")
                                                    .otherwise("Não Informado")
                                        )

df_municipios = df_municipios.withColumn("regiao", F.when(F.col("estado_sigla")=="AC", "Norte")
                                                    .when(F.col("estado_sigla")=="AL", "Nordeste")
                                                    .when(F.col("estado_sigla")=="AM", "Norte")
                                                    .when(F.col("estado_sigla")=="AP", "Norte")
                                                    .when(F.col("estado_sigla")=="BA", "Nordeste")
                                                    .when(F.col("estado_sigla")=="CE", "Nordeste")
                                                    .when(F.col("estado_sigla")=="DF", "Centro-Oeste")
                                                    .when(F.col("estado_sigla")=="ES", "Sudeste")
                                                    .when(F.col("estado_sigla")=="GO", "Centro-Oeste")
                                                    .when(F.col("estado_sigla")=="MA", "Nordeste")
                                                    .when(F.col("estado_sigla")=="MG", "Sudeste")
                                                    .when(F.col("estado_sigla")=="MS", "Centro-Oeste")
                                                    .when(F.col("estado_sigla")=="MT", "Centro-Oeste")
                                                    .when(F.col("estado_sigla")=="PA", "Norte")
                                                    .when(F.col("estado_sigla")=="PB", "Nordeste")
                                                    .when(F.col("estado_sigla")=="PE", "Nordeste")
                                                    .when(F.col("estado_sigla")=="PI", "Nordeste")
                                                    .when(F.col("estado_sigla")=="PR", "Sul")
                                                    .when(F.col("estado_sigla")=="RJ", "Sudeste")
                                                    .when(F.col("estado_sigla")=="RN", "Nordeste")
                                                    .when(F.col("estado_sigla")=="RO", "Norte")
                                                    .when(F.col("estado_sigla")=="RR", "Norte")
                                                    .when(F.col("estado_sigla")=="RS", "Sul")
                                                    .when(F.col("estado_sigla")=="SC", "Sul")
                                                    .when(F.col("estado_sigla")=="SE", "Nordeste")
                                                    .when(F.col("estado_sigla")=="SP", "Sudeste")
                                                    .when(F.col("estado_sigla")=="TO", "Norte")
                                                    .otherwise("Não Informado")
                                        )

df_municipios = df_municipios.withColumn("regiao_sigla", F.when(F.col("regiao")=="Norte", "N")
                                                          .when(F.col("regiao")=="Nordeste", "NE")
                                                          .when(F.col("regiao")=="Centro-Oeste", "CO")
                                                          .when(F.col("regiao")=="Sudeste", "SE")
                                                          .when(F.col("regiao")=="Sul", "S")
                                                          .otherwise("Não Informado")
                                        )

df_municipios = generate_primary_key(df_municipios, "pk_municipio")

df_municipios = df_municipios.withColumn("total_populacao_2010", df_municipios["total_populacao_2010"].cast(IntegerType()))
df_municipios = df_municipios.withColumnRenamed("total_populacao_2010", "total_populacao")

df_municipios = df_municipios.withColumn("faixa_populacao", F.when(F.col("total_populacao")<5001, "Até 5 mil")
                                                          .when(F.col("total_populacao")<10001, "De 5 a 10 mil")
                                                          .when(F.col("total_populacao")<20001, "De 10 a 20 mil")
                                                          .when(F.col("total_populacao")<50001, "De 20 a 50 mil")
                                                          .when(F.col("total_populacao")<100001, "De 50 a 100 mil")
                                                          .when(F.col("total_populacao")<500001, "De 100 a 500 mil")
                                                          .when(F.col("total_populacao")>=500001, "Acima de 500 mil")                                 
                                                          .otherwise("Não Informado")
                                        )


df_municipios = df_municipios.withColumn("faixa_pib_per_capita", F.when(F.col("pib_per_capita")<=10000, "Até 10 mil")
                                                          .when(F.col("pib_per_capita")<=15000, "De 10 a 15 mil")
                                                          .when(F.col("pib_per_capita")<=20000, "De 15 a 20 mil")
                                                          .when(F.col("pib_per_capita")<=27000, "De 20 a 27 mil")
                                                          .when(F.col("pib_per_capita")<=35000, "De 27 a 35 mil")
                                                          .when(F.col("pib_per_capita")<=45000, "De 35 a 45 mil")
                                                          .when(F.col("pib_per_capita")>45000, "Acima de 45 mil")                                 
                                                          .otherwise("Não Informado")
                                        )

# COMMAND ----------

df_municipios.display()

# COMMAND ----------

path = "Dimensions/DimMunicipio"
load_data(df_municipios, path=path, layer='gold')

# COMMAND ----------

