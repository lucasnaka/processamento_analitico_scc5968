# Databricks notebook source
# MAGIC %run ../utils_functions

# COMMAND ----------

layer = 'silver'
path = 'AcidentesGranularDatatran'
table_name = "acidente_table"
query = """
    SELECT DISTINCT
        data_inversa
    FROM acidente_table
    ORDER BY data_inversa
"""
df_data = extract_data(path, layer, file_type='parquet', query=query, table_name=table_name)

# COMMAND ----------

df_data = (df_data.withColumn('dia_da_semana', F.dayofweek(F.col('data_inversa')))
                  .withColumn('dia_do_mes', F.dayofmonth(F.col('data_inversa')))
                  .withColumn('dia_do_ano', F.dayofyear(F.col('data_inversa')))
                  .withColumn('mes', F.month(F.col('data_inversa')))
                  .withColumn('ano', F.year(F.col('data_inversa')))
                  .withColumn('trimestre', F.quarter(F.col('data_inversa')))
          )

df_data = df_data.withColumn("dia_da_semana_extenso", F.when(F.col("dia_da_semana")==1, "domingo")
                                                       .when(F.col("dia_da_semana")==2, "segunda-feira")
                                                       .when(F.col("dia_da_semana")==3, "terça-feira")
                                                       .when(F.col("dia_da_semana")==4, "quarta-feira")
                                                       .when(F.col("dia_da_semana")==5, "quinta-feira")
                                                       .when(F.col("dia_da_semana")==6, "sexta-feira")
                                                       .when(F.col("dia_da_semana")==7, "sábado")
                                                       .otherwise("não informado")
                            )

df_data = df_data.withColumn("mes_extenso", F.when(F.col("mes")==1, "Janeiro")
                                             .when(F.col("mes")==2, "Fevereiro")
                                             .when(F.col("mes")==3, "Março")
                                             .when(F.col("mes")==4, "Abril")
                                             .when(F.col("mes")==5, "Maio")
                                             .when(F.col("mes")==6, "Junho")
                                             .when(F.col("mes")==7, "Julho")
                                             .when(F.col("mes")==8, "Agosto")
                                             .when(F.col("mes")==9, "Setembro")
                                             .when(F.col("mes")==10, "Outubro")
                                             .when(F.col("mes")==11, "Novembro")
                                             .otherwise("Dezembro"))

df_data = generate_primary_key(df_data, "pk_data")

# COMMAND ----------

df_data.display()

# COMMAND ----------

path = "Dimensions/DimData"
load_data(df_data, path=path, layer='gold')

# COMMAND ----------

