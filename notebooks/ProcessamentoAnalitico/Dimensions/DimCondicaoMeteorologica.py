# Databricks notebook source
# MAGIC %run ../utils_functions

# COMMAND ----------

layer = 'silver'
path = 'AcidentesGranularDatatran'
table_name = "acidente_table"
query = """
    SELECT DISTINCT
        A.condicao_metereologica AS condicao_meteorologica,
        B.fase_dia
    FROM acidente_table A
    CROSS JOIN acidente_table B
"""
df_acidentes = extract_data(path, layer, file_type='parquet', query=query, table_name=table_name)

# COMMAND ----------

df_cond_meteo = generate_primary_key(df_acidentes, "pk_cond_meteorologica")

# COMMAND ----------

df_cond_meteo.display()

# COMMAND ----------

path = "Dimensions/DimCondicaoMeteorologica"
load_data(df_cond_meteo, path=path, layer='gold')

# COMMAND ----------

