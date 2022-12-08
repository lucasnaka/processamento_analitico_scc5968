# Databricks notebook source
# MAGIC %run ../utils_functions

# COMMAND ----------

layer = 'bronze'
path = 'AcidentesDatatran'
df_acidentes = extract_data(path, layer, file_type='csv', encoding="ISO-8859-1")

# COMMAND ----------

df_acidentes.display()

# COMMAND ----------

path = "AcidentesDatatran"
load_data(df_acidentes, path=path, layer='silver')

# COMMAND ----------

