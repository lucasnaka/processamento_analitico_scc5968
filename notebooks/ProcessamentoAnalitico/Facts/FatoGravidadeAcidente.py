# Databricks notebook source
# MAGIC %run ../utils_functions

# COMMAND ----------

layer = 'bronze'
path = 'AcidentesDatatran'
table_name = "table_acidentes"
query = """
    SELECT
        id,
        data_inversa,
        dia_semana,
        uf,
        municipio,
        causa_acidente,
        tipo_acidente,
        classificacao_acidente,
        fase_dia,
        sentido_via,
        condicao_metereologica AS condicao_meteorologica,
        tipo_pista,
        tracado_via,
        uso_solo,
        pessoas AS quant_pessoas,
        mortos AS quant_mortos,
        feridos_leves AS quant_feridos_leves,
        feridos_graves AS quant_feridos_graves,
        ilesos AS quant_ilesos,
        ignorados AS quant_ignorados,
        feridos AS quant_feridos,
        veiculos AS quant_veiculos,
        CAST(REPLACE(latitude, ',', '.') AS DECIMAL(18,8)) AS latitude,
        CAST(REPLACE(longitude, ',', '.') AS DECIMAL(18,8)) AS longitude
    FROM table_acidentes
"""
df_acidentes = extract_data(path, layer, file_type='csv', query=query, table_name=table_name, encoding="ISO-8859-1")

path = 'Feriados'
df_feriado_raw = extract_data(path, layer, file_type='csv', delimiter=",")

layer = 'gold'
path = 'Dimensions/DimAcidente'
df_dim_acidente = extract_data(path, layer, file_type='parquet')

path = 'Dimensions/DimData'
df_dim_data = extract_data(path, layer, file_type='parquet')

path = 'Dimensions/DimMunicipio'
df_dim_municipio = extract_data(path, layer, file_type='parquet')

path = 'Dimensions/DimCondicaoMeteorologica'
df_dim_cond_meteorologica = extract_data(path, layer, file_type='parquet')

path = 'Dimensions/DimTipoPista'
df_dim_tipo_pista = extract_data(path, layer, file_type='parquet')

path = 'Dimensions/DimFeriado'
df_dim_feriado = extract_data(path, layer, file_type='parquet')

# COMMAND ----------

df_feriado_municipal = df_feriado_raw.filter(F.col("tipo_feriado")=="M").select(F.col("dia").alias("dia_do_mes"), "mes", "estado_sigla", F.col("municipio").alias("nom_municipio"), F.lit(1).alias("feriado_municipal"))
df_feriado_estadual = df_feriado_raw.filter(F.col("tipo_feriado")=="E").select(F.col("dia").alias("dia_do_mes"), "mes", "estado_sigla", F.lit(1).alias("feriado_estadual"))
df_feriado_nacional = df_feriado_raw.filter(F.col("tipo_feriado")=="N").select(F.col("dia").alias("dia_do_mes"), "mes", F.lit(1).alias("feriado_nacional"))

# COMMAND ----------

# Join DimAcidente
df_fato_gravidade_acidente = df_acidentes.join(df_dim_acidente.select("id_acidente", "pk_acidente", "latitude", "longitude"), how="left", on=(df_acidentes.id == df_dim_acidente.id_acidente))
df_fato_gravidade_acidente = df_fato_gravidade_acidente.drop("id", "id_acidente", "tipo_acidente", "causa_acidente", "classificacao_acidente", "latitude", "longitude")

# Join DimCondicaoMeteorologica
df_fato_gravidade_acidente = df_fato_gravidade_acidente.join(df_dim_cond_meteorologica, how="left", on=["condicao_meteorologica", "fase_dia"])
df_fato_gravidade_acidente = df_fato_gravidade_acidente.drop("condicao_meteorologica", "fase_dia")

# Join DimData
df_fato_gravidade_acidente = df_fato_gravidade_acidente.join(df_dim_data.select("data_inversa", "dia_da_semana_extenso", "pk_data"), 
                                                             how="left", 
                                                             on=(df_fato_gravidade_acidente.data_inversa == df_dim_data.data_inversa)
                                                                & (df_fato_gravidade_acidente.dia_semana == df_dim_data.dia_da_semana_extenso)
                                                            )
df_fato_gravidade_acidente = df_fato_gravidade_acidente.drop("data_inversa", "dia_semana", "dia_da_semana_extenso")

# Join DimMunicipio
df_fato_gravidade_acidente = df_fato_gravidade_acidente.join(df_dim_municipio.select("nom_municipio", "estado_sigla", "pk_municipio"), 
                                                             how="left", 
                                                             on=(df_fato_gravidade_acidente.municipio == df_dim_municipio.nom_municipio)
                                                             & (df_fato_gravidade_acidente.uf == df_dim_municipio.estado_sigla)
                                                            )
df_fato_gravidade_acidente = df_fato_gravidade_acidente.drop("municipio", "nom_municipio", "uf", "estado_sigla")

# Join DimTipoPista
df_fato_gravidade_acidente = df_fato_gravidade_acidente.withColumn("uso_solo", F.when(F.col("uso_solo")=="Sim", "Urbano").otherwise("Rural"))
df_fato_gravidade_acidente = df_fato_gravidade_acidente.join(df_dim_tipo_pista, how="left", on=['tipo_pista', 'tracado_via', 'uso_solo', 'sentido_via'])
df_fato_gravidade_acidente = df_fato_gravidade_acidente.drop('tipo_pista', 'tracado_via', 'uso_solo', 'sentido_via')

# Join Feriado
df_fato_gravidade_acidente = df_fato_gravidade_acidente.join(df_dim_data.select("dia_do_mes", "mes", "pk_data"), how="left", on="pk_data")
df_fato_gravidade_acidente = df_fato_gravidade_acidente.join(df_dim_municipio.select("estado_sigla", "nom_municipio", "pk_municipio"), how="left", on="pk_municipio")

df_fato_gravidade_acidente = df_fato_gravidade_acidente.join(df_feriado_nacional, how="left", on=["dia_do_mes", "mes"])
df_fato_gravidade_acidente = df_fato_gravidade_acidente.join(df_feriado_estadual, how="left", on=["dia_do_mes", "mes", "estado_sigla"])
df_fato_gravidade_acidente = df_fato_gravidade_acidente.join(df_feriado_municipal, how="left", on=["dia_do_mes", "mes", "estado_sigla", "nom_municipio"])

df_fato_gravidade_acidente = df_fato_gravidade_acidente.fillna(0, subset=["feriado_municipal", "feriado_estadual", "feriado_nacional"])
df_fato_gravidade_acidente = df_fato_gravidade_acidente.drop("dia_do_mes", "mes", "estado_sigla", "nom_municipio")
df_fato_gravidade_acidente = df_fato_gravidade_acidente.join(df_dim_feriado, how="left", on=["feriado_municipal", "feriado_estadual", "feriado_nacional"])
df_fato_gravidade_acidente = df_fato_gravidade_acidente.drop("feriado_municipal", "feriado_estadual", "feriado_nacional")

# COMMAND ----------

df_fato_gravidade_acidente = df_fato_gravidade_acidente.join(df_dim_data.select("dia_do_mes", "mes", "pk_data"), how="left", on="pk_data")

# COMMAND ----------

df_fato_gravidade_acidente.filter((F.col("dia_do_mes")==12) & (F.col("mes")==10)).display()

# COMMAND ----------

df_feriado_nacional.display()

# COMMAND ----------

df_fato_gravidade_acidente.display()

# COMMAND ----------

path = "Facts/FatoGravidadeAcidente"
load_data(df_fato_gravidade_acidente, path=path, layer='gold')

# COMMAND ----------

