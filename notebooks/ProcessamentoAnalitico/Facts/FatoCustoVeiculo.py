# Databricks notebook source
# MAGIC %run ../utils_functions

# COMMAND ----------

layer = 'bronze'
path = 'AcidentesGranularDatatran'
table_name = "acidente_granular_table"
query = """
    SELECT DISTINCT
        id,
        data_inversa,
        uf,
        municipio,
        classificacao_acidente,
        id_veiculo,
        tipo_veiculo
    FROM acidente_granular_table
"""
df_fato_custo_veiculo = extract_data(path, layer, file_type='csv', query=query, table_name=table_name, encoding="ISO-8859-1")

path = 'CustosVeiculo'
table_name = "custo_veiculos"
query = """
    SELECT 
        tipo_veiculo,
        classificacao_acidente,
        CAST(REPLACE(custo_veiculo, ',', '.') AS DECIMAL(18,2)) AS custo_veiculo
    FROM custo_veiculos
"""
df_custos_veiculos = extract_data(path, layer, file_type='csv', query=query, table_name=table_name, encoding="ISO-8859-1")

layer = 'gold'
path = 'Dimensions/DimAcidente'
df_dim_acidente = extract_data(path, layer, file_type='parquet')

path = 'Dimensions/DimData'
df_dim_data = extract_data(path, layer, file_type='parquet')

path = 'Dimensions/DimMunicipio'
df_dim_municipio = extract_data(path, layer, file_type='parquet')

path = 'Dimensions/DimVeiculo'
df_dim_veiculo = extract_data(path, layer, file_type='parquet')

# COMMAND ----------

# Join DimAcidente
df_fato_custo_veiculo = df_fato_custo_veiculo.join(
    df_dim_acidente.select("id_acidente", "pk_acidente"),
    how="left",
    on=(df_fato_custo_veiculo.id == df_dim_acidente.id_acidente),
)
df_fato_custo_veiculo = df_fato_custo_veiculo.drop("id_acidente", "id")

# Join DimVeiculo
df_fato_custo_veiculo = df_fato_custo_veiculo.join(
    df_dim_veiculo.select("id_veiculo", "pk_veiculos"),
    how="left",
    on=(df_fato_custo_veiculo.id_veiculo == df_dim_veiculo.id_veiculo),
)
df_fato_custo_veiculo = df_fato_custo_veiculo.drop("id_veiculo")

# Join DimData
df_fato_custo_veiculo = df_fato_custo_veiculo.join(
    df_dim_data.select("data_inversa", "pk_data"),
    how="left",
    on=(df_fato_custo_veiculo.data_inversa == df_dim_data.data_inversa)
    )
df_fato_custo_veiculo = df_fato_custo_veiculo.drop("data_inversa")


# Join DimMunicipio
df_fato_custo_veiculo = df_fato_custo_veiculo.join(
    df_dim_municipio.select("nom_municipio", "estado_sigla", "pk_municipio"),
    how="left",
    on=(df_fato_custo_veiculo.municipio == df_dim_municipio.nom_municipio)
    & (df_fato_custo_veiculo.uf == df_dim_municipio.estado_sigla),
)
df_fato_custo_veiculo = df_fato_custo_veiculo.drop("municipio", "nom_municipio", "uf", "estado_sigla")

# COMMAND ----------

df_fato_custo_veiculo = df_fato_custo_veiculo.withColumn("tipo_veiculo_2", F.when(df_fato_custo_veiculo.tipo_veiculo.isin(["Automóvel", "Bicicleta", "Caminhão", "Motocicleta", "Ônibus", "Utilitário"]), df_fato_custo_veiculo.tipo_veiculo).otherwise("Outros"))

# COMMAND ----------

# Join Custos
df_fato_custo_veiculo = df_fato_custo_veiculo.join(
    df_custos_veiculos, how="left", on=(df_fato_custo_veiculo.classificacao_acidente == df_custos_veiculos.classificacao_acidente)
    & (df_fato_custo_veiculo.tipo_veiculo_2 == df_custos_veiculos.tipo_veiculo)
)
df_fato_custo_veiculo = df_fato_custo_veiculo.drop("classificacao_acidente", "tipo_veiculo", "tipo_veiculo_2")

# COMMAND ----------

df_fato_custo_veiculo.display()

# COMMAND ----------

path = "Facts/FatoCustoVeiculo"
load_data(df_fato_custo_veiculo, path=path, layer='gold')

# COMMAND ----------

