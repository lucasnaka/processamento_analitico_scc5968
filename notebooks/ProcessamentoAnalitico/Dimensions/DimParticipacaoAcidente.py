# Databricks notebook source
# MAGIC %run ../utils_functions

# COMMAND ----------

layer = 'silver'
path = 'AcidentesGranularDatatran'
table_name = "acidente_granular_table"
query = """
    SELECT DISTINCT
        estado_fisico,
        tipo_envolvido
    FROM acidente_granular_table
"""
df_participacao_acidente = extract_data(path, layer, file_type='parquet', query=query, table_name=table_name)

# COMMAND ----------

df_participacao_acidente = generate_primary_key(df_participacao_acidente, "pk_participacao_acidente")

# COMMAND ----------

df_participacao_acidente.display()

# COMMAND ----------

path = "Dimensions/DimParticipacaoAcidente"
load_data(df_participacao_acidente, path=path, layer='gold')

# COMMAND ----------

