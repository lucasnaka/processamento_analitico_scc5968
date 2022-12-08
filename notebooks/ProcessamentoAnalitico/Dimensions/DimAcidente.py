# Databricks notebook source
# MAGIC %run ../utils_functions

# COMMAND ----------

layer = 'silver'
path = 'AcidentesGranularDatatran'
table_name = "acidente_table"
query = """
    SELECT DISTINCT
        id AS id_acidente,
        tipo_acidente,
        classificacao_acidente,
        causa_acidente,
        CAST(REPLACE(latitude, ',', '.') AS DECIMAL(18,8)) AS latitude,
        CAST(REPLACE(longitude, ',', '.') AS DECIMAL(18,8)) AS longitude
    FROM acidente_table
"""
df_acidentes = extract_data(path, layer, file_type='parquet', query=query, table_name=table_name)

# COMMAND ----------

df_carac_acidente = generate_primary_key(df_acidentes, "pk_acidente")

# COMMAND ----------

df_carac_acidente = df_carac_acidente.withColumn("grupo_tipo_acidente", F.when((F.col("tipo_acidente").contains("Colisão")) | (F.col("tipo_acidente").contains("Engavetamento")), "Colisão")
                                                .when(F.col("tipo_acidente").contains("Atropelamento"), "Atropelamento")
                                                .when((F.col("tipo_acidente").contains("Capotamento")) | (F.col("tipo_acidente").contains("Tombamento")), "Capotamento/Tombamento")
                                                .when(F.col("tipo_acidente").contains("Saída"), "Saída de pista")
                                                .when(F.col("tipo_acidente").contains("Queda de ocupante"), "Queda de ocupante")
                                                .when(F.col("tipo_acidente").contains("Incêndio"), "Incêndio")
                                                .when(F.col("tipo_acidente").contains("Derramamento de carga"), "Derramamento de carga")
                                                                              .otherwise("Eventos atípicos"))

# COMMAND ----------

df_carac_acidente.display()

# COMMAND ----------

path = "Dimensions/DimAcidente"
load_data(df_carac_acidente, path=path, layer='gold')

# COMMAND ----------

