# Databricks notebook source
# MAGIC %run ../utils_functions

# COMMAND ----------

layer = 'bronze'
path = 'AcidentesGranularDatatran'
table_name = "acidente_granular_table"
query = """
    SELECT DISTINCT
        id, 
        pesid,
        data_inversa,
        uf,
        municipio,
        classificacao_acidente,
        estado_fisico,
        tipo_envolvido
    FROM acidente_granular_table
    WHERE
        pesid != "NA"
"""
df_fato_custo_pessoa = extract_data(path, layer, file_type='csv', query=query, table_name=table_name, encoding="ISO-8859-1")

path = 'CustosPessoa'
table_name = "custo_pessoas"
query = """
    SELECT 
        estado_fisico,
        classificacao_acidente,
        CAST(REPLACE(custo_pessoa, ',', '.') AS DECIMAL(18,2)) AS custo_pessoa
    FROM custo_pessoas
"""
df_custos_pessoas = extract_data(path, layer, file_type='csv', query=query, table_name=table_name,encoding="ISO-8859-1" )

layer = 'gold'
path = 'Dimensions/DimAcidente'
df_dim_acidente = extract_data(path, layer, file_type='parquet')

path = 'Dimensions/DimData'
df_dim_data = extract_data(path, layer, file_type='parquet')

path = 'Dimensions/DimMunicipio'
df_dim_municipio = extract_data(path, layer, file_type='parquet')

path = 'Dimensions/DimParticipacaoAcidente'
df_dim_part_acidente = extract_data(path, layer, file_type='parquet')

path = 'Dimensions/DimPessoa'
df_dim_pessoa = extract_data(path, layer, file_type='parquet')

# COMMAND ----------

# Join DimAcidente
df_fato_custo_pessoa = df_fato_custo_pessoa.join(
    df_dim_acidente.select("id_acidente", "pk_acidente"),
    how="left",
    on=(df_fato_custo_pessoa.id == df_dim_acidente.id_acidente),
)
df_fato_custo_pessoa = df_fato_custo_pessoa.drop("id_acidente", "id")

# Join DimPessoa
df_fato_custo_pessoa = df_fato_custo_pessoa.join(
    df_dim_pessoa.select("id_pessoa", "pk_pessoa"),
    how="left",
    on=(df_fato_custo_pessoa.pesid == df_dim_pessoa.id_pessoa),
)
df_fato_custo_pessoa = df_fato_custo_pessoa.drop("id_pessoa","pesid")

# Join DimData
df_fato_custo_pessoa = df_fato_custo_pessoa.join(
    df_dim_data.select("data_inversa", "pk_data"),
    how="left",
    on=(df_fato_custo_pessoa.data_inversa == df_dim_data.data_inversa)
    )
df_fato_custo_pessoa = df_fato_custo_pessoa.drop("data_inversa")


# Join DimMunicipio
df_fato_custo_pessoa = df_fato_custo_pessoa.join(
    df_dim_municipio.select("nom_municipio", "estado_sigla", "pk_municipio"),
    how="left",
    on=(df_fato_custo_pessoa.municipio == df_dim_municipio.nom_municipio)
    & (df_fato_custo_pessoa.uf == df_dim_municipio.estado_sigla),
)
df_fato_custo_pessoa = df_fato_custo_pessoa.drop("municipio", "nom_municipio", "uf", "estado_sigla")

# Join DimParticipacaoAcidente
df_fato_custo_pessoa = df_fato_custo_pessoa.join(
    df_dim_part_acidente, how="left", on=["tipo_envolvido", "estado_fisico"]
)
df_fato_custo_pessoa = df_fato_custo_pessoa.drop("tipo_envolvido")

# COMMAND ----------

# Join Custos
df_fato_custo_pessoa = df_fato_custo_pessoa.join(df_custos_pessoas, how="left", on=["classificacao_acidente", "estado_fisico"])
df_fato_custo_pessoa = df_fato_custo_pessoa.drop("classificacao_acidente", "estado_fisico")

# COMMAND ----------

df_fato_custo_pessoa.display()

# COMMAND ----------

path = "Facts/FatoCustoPessoa"
load_data(df_fato_custo_pessoa, path=path, layer='gold')

# COMMAND ----------

