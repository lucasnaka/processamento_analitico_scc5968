# Databricks notebook source
# MAGIC %run ../utils_functions

# COMMAND ----------

layer = 'bronze'
path = 'AcidentesGranularDatatran'
table_name = "acidente_granular_table"
query = """
    SELECT DISTINCT
        id_veiculo,
        tipo_veiculo,
        marca AS marca_veiculo,
        ano_fabricacao_veiculo
    FROM acidente_granular_table
"""
df_veiculo = extract_data(path, layer, file_type='csv', query=query, table_name=table_name, encoding="ISO-8859-1")

# COMMAND ----------

df_veiculo = generate_primary_key(df_veiculo, "pk_veiculos")

# COMMAND ----------

df_veiculo.display()

# COMMAND ----------

path = "Dimensions/DimVeiculo"
load_data(df_veiculo, path=path, layer='gold')

# COMMAND ----------

