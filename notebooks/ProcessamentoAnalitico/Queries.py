# Databricks notebook source
# MAGIC %run ./utils_functions

# COMMAND ----------

# MAGIC %md #Drill Down

# COMMAND ----------

layer = "gold"
path = 'Facts/FatoGravidadeAcidente'
table_name = "fato_gravidade_acidente"
df_fato_gravidade_acidente = extract_data(path, layer, file_type='parquet', table_name=table_name)

path = 'Dimensions/DimMunicipio'
table_name = "dim_municipio"
df_dim_municipio = extract_data(path, layer, file_type='parquet', table_name=table_name)


# COMMAND ----------

df_drill_down = spark.sql("""
    SELECT
        regiao_sigla AS Regiao,
        (SUM(quant_mortos) + SUM(quant_feridos_graves))/ SUM(quant_pessoas) AS `Índice Gravidade (%)`
    FROM fato_gravidade_acidente fga
    LEFT JOIN dim_municipio dm
        ON fga.pk_municipio = dm.pk_municipio  
    GROUP BY regiao_sigla
    ORDER BY  `Índice Gravidade (%)` DESC
""")
df_drill_down.display()

# COMMAND ----------

df_drill_down = spark.sql("""
    SELECT
        estado_sigla AS Estado,
        (SUM(quant_mortos) + SUM(quant_feridos_graves))/ SUM(quant_pessoas) AS `Índice Gravidade (%)`
    FROM fato_gravidade_acidente fga
    LEFT JOIN dim_municipio dm
        ON fga.pk_municipio = dm.pk_municipio  
        WHERE regiao_sigla = "NE"
    GROUP BY estado_sigla
    ORDER BY  `Índice Gravidade (%)` DESC
""")
df_drill_down.display()

# COMMAND ----------

# MAGIC %md # Roll Up

# COMMAND ----------

layer = "gold"
path = 'Facts/FatoGravidadeAcidente'
table_name = "fato_gravidade_acidente"
df_fato_gravidade_acidente = extract_data(path, layer, file_type='parquet', table_name=table_name)

path = 'Dimensions/DimMunicipio'
table_name = "dim_municipio"
df_dim_municipio = extract_data(path, layer, file_type='parquet', table_name=table_name)

# COMMAND ----------

df_roll_up = spark.sql("""
    SELECT
        nom_municipio,
        SUM(quant_ignorados) AS ignorados
    FROM fato_gravidade_acidente fga
    LEFT JOIN dim_municipio dm
        ON fga.pk_municipio = dm.pk_municipio
    GROUP BY  nom_municipio
    ORDER BY ignorados DESC
""")
df_roll_up.display()

# COMMAND ----------

df_roll_up = spark.sql("""
    SELECT
        estado_sigla,
        SUM(quant_ignorados) AS ignorados
    FROM fato_gravidade_acidente fga
    LEFT JOIN dim_municipio dm
        ON fga.pk_municipio = dm.pk_municipio
    GROUP BY estado_sigla
    ORDER BY ignorados DESC
""")
df_roll_up.display()

# COMMAND ----------

# MAGIC %md # Slide and Dice

# COMMAND ----------

layer = "gold"
path = 'Facts/FatoGravidadeAcidente'
table_name = "fato_gravidade_acidente"
df_fato_gravidade_acidente = extract_data(path, layer, file_type='parquet', table_name=table_name)


path = 'Dimensions/DimData'
table_name = "dim_data"
df_dim_data = extract_data(path, layer, file_type='parquet', table_name=table_name)

# COMMAND ----------

df_slice = spark.sql("""
    SELECT
        mes as `M`,
        mes_extenso as `Mes`,
        COUNT(DISTINCT pk_acidente) as `Quantidade de acidentes`
    FROM fato_gravidade_acidente fga
    LEFT JOIN dim_data dd
        ON fga.pk_data = dd.pk_data
    WHERE
        ano = "2020" OR ano = "2021"
    GROUP BY
        mes,mes_extenso
    ORDER BY
        `M` ASC
""")
df_slice.display()

# COMMAND ----------

df_slice = spark.sql("""
    SELECT
        mes as `M`,
        mes_extenso as `Mes`,        
        COUNT(DISTINCT pk_acidente) as `Quantidade de acidentes`
    FROM fato_gravidade_acidente fga
    LEFT JOIN dim_data dd
        ON fga.pk_data = dd.pk_data
    WHERE
        ano = "2020" and dia_da_semana IN (1, 7) 
    GROUP BY
        mes,mes_extenso
    ORDER BY
        `M` ASC
""")
df_slice.display()

# COMMAND ----------

# MAGIC %md # Pivot

# COMMAND ----------

layer = "gold"
path = 'Facts/FatoCustoVeiculo'
table_name = "fato_custo_veiculo"
df_fato_custo_veiculo = extract_data(path, layer, file_type='parquet', table_name=table_name)


path = 'Dimensions/DimVeiculo'
table_name = "dim_veiculo"
query = """
    SELECT 
        pk_veiculos,
        tipo_veiculo
    FROM dim_veiculo
"""
df_dim_veiculo = extract_data(path, layer, file_type='parquet', query=query, table_name=table_name)

path = 'Dimensions/DimMunicipio'
table_name = "dim_municipio"
query = """
    SELECT 
        pk_municipio,
        estado_sigla,
        total_populacao
    FROM dim_municipio
"""
df_dim_municipio = extract_data(path, layer, file_type='parquet', query=query, table_name=table_name)

# COMMAND ----------

df_pivot = spark.sql("""
    SELECT
        estado_sigla,
        tipo_veiculo,
        SUM(custo_veiculo)
    FROM fato_custo_veiculo fcv
    LEFT JOIN dim_municipio dm
        ON fcv.pk_municipio = dm.pk_municipio
    LEFT JOIN dim_veiculo dv
        ON fcv.pk_veiculos = dv.pk_veiculos
    GROUP BY
        estado_sigla,tipo_veiculo
""")
df_pivot.display()

# COMMAND ----------

df_fato_custo_veiculo = df_fato_custo_veiculo.join(df_dim_veiculo, how="left", on="pk_veiculos")\
                                             .join(df_dim_municipio, how="left", on="pk_municipio")

# COMMAND ----------

df_fato_custo_veiculo.groupBy("estado_sigla").pivot("tipo_veiculo").agg(F.sum("custo_veiculo")).fillna(0).display()

# COMMAND ----------

# MAGIC %md # Drill Across

# COMMAND ----------

layer = "gold"
path = 'Facts/FatoCustoVeiculo'
table_name = "fato_custo_veiculo"
df_fato_custo_veiculo = extract_data(path, layer, file_type='parquet', table_name=table_name)

path = 'Facts/FatoCustoPessoa'
table_name = "fato_custo_pessoa"
df_fato_custo_pessoa = extract_data(path, layer, file_type='parquet', table_name=table_name)

path = 'Facts/FatoGravidadeAcidente'
table_name = "fato_gravidade"
df_fato_gravidade_acidente = extract_data(path, layer, file_type='parquet', table_name=table_name)

path = 'Dimensions/DimData'
table_name = "dim_data"
df_dim_data = extract_data(path, layer, file_type='parquet', table_name=table_name)

# COMMAND ----------

query = """
    SELECT c.ano_c_p AS Ano,
           c.tri_c_p AS Trimestre,
           ROUND(c.custopessoa, 2) AS `Custos com Pessoas`, 
           ROUND(c.custoveiculo, 2) AS `Custos com Veiculos`,
           ROUND(c.custopessoa + c.custoveiculo, 2) AS `Custos Totais`,
           o.mortes AS `Quantidade de óbitos`
    FROM (
        (SELECT 
            ano AS ano_c_p, 
            trimestre AS tri_c_p,
            SUM(custo_pessoa) AS custopessoa
         FROM dim_data dd 
         JOIN fato_custo_pessoa fcp 
             ON fcp.pk_data = dd.pk_data
         GROUP BY 
             ano,trimestre
        ) p
        JOIN
        (SELECT 
            ano AS ano_c_v,
            trimestre AS tri_c_v,
            SUM(custo_veiculo) AS custoveiculo
        FROM dim_data dd 
        JOIN fato_custo_veiculo fcv 
            ON fcv.pk_data = dd.pk_data
        GROUP BY 
            ano,trimestre
        ) v
            ON (p.ano_c_p = v.ano_c_v AND p.tri_c_p = v.tri_c_v)
    ) c
    JOIN
        (SELECT 
            ano AS ano_g, 
            trimestre AS tri_g,
            SUM(quant_mortos) AS mortes
        FROM dim_data dd 
        JOIN fato_gravidade fg 
            ON fg.pk_data = dd.pk_data
        GROUP BY 
            ano, trimestre
        ) AS o
        ON (c.ano_c_p = o.ano_g AND c.tri_c_p = o.tri_g)
    ORDER BY 
        c.ano_c_p,c.tri_c_p
"""
spark.sql(query).show()

# COMMAND ----------

