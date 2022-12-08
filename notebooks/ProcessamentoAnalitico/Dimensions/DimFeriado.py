# Databricks notebook source
# MAGIC %run ../utils_functions

# COMMAND ----------

import itertools

# COMMAND ----------

list_binary_combinations = list(itertools.product([0, 1], repeat=3))

# COMMAND ----------

df_feriado = spark.createDataFrame(
    list_binary_combinations,
    ["feriado_municipal", "feriado_estadual", "feriado_nacional"]
)

# COMMAND ----------

df_feriado = generate_primary_key(df_feriado, "pk_feriado")

# COMMAND ----------

df_feriado.display()

# COMMAND ----------

path = "Dimensions/DimFeriado"
load_data(df_feriado, path=path, layer='gold')

# COMMAND ----------

