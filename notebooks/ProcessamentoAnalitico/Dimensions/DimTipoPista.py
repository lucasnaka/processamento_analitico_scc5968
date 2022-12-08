# Databricks notebook source
# MAGIC %run ../utils_functions

# COMMAND ----------

layer = 'bronze'
path = 'AcidentesGranularDatatran'
table_name = "acidente_table"
query = """
    SELECT DISTINCT
        a.tipo_pista,
        b.tracado_via,
        c.uso_solo,
        d.sentido_via
    FROM acidente_table a
    CROSS JOIN acidente_table b
    CROSS JOIN acidente_table c
    CROSS JOIN acidente_table d
"""
df_tipo_pista = extract_data(path, layer, file_type='csv', query=query, table_name=table_name, encoding="ISO-8859-1")

# COMMAND ----------

df_tipo_pista = df_tipo_pista.withColumn("uso_solo", F.when(F.col("uso_solo")=="Sim", "Urbano").otherwise("Rural"))

df_tipo_pista = generate_primary_key(df_tipo_pista, "pk_tipo_pista")

# COMMAND ----------

df_tipo_pista.display()

# COMMAND ----------

path = "Dimensions/DimTipoPista"
load_data(df_tipo_pista, path=path, layer='gold')

# COMMAND ----------

