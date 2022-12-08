# Databricks notebook source
# MAGIC %run ../utils_functions

# COMMAND ----------

layer = 'bronze'
path = 'AcidentesGranularDatatran'
df_acidentes_granular = extract_data(path, layer, file_type='csv', encoding="ISO-8859-1")

# COMMAND ----------

def replace_accents(df: DataFrame, columns: list) -> DataFrame:
    for col in columns:
        df = (df.withColumn(col, F.regexp_replace(col, 'Ãµ', 'õ'))
                .withColumn(col, F.regexp_replace(col, 'Ã£', 'ã'))
                .withColumn(col, F.regexp_replace(col, 'Ã', 'Ó'))
                .withColumn(col, F.regexp_replace(col, 'Ã§', 'ç'))
                .withColumn(col, F.regexp_replace(col, 'Ã', 'í'))
                .withColumn(col, F.regexp_replace(col, 'Ã¡', 'á'))
                .withColumn(col, F.regexp_replace(col, 'Ã³', 'ó'))
                .withColumn(col, F.regexp_replace(col, 'Ãª', 'ê'))
                .withColumn(col, F.regexp_replace(col, 'Ã¢', 'â'))
                .withColumn(col, F.regexp_replace(col, 'Ã©', 'é'))
                .withColumn(col, F.regexp_replace(col, 'Ã ', 'à'))
                .withColumn(col, F.regexp_replace(col, 'Ã´', 'ô'))
             )
        
    return df

# COMMAND ----------

# df_acidentes_granular = replace_accents(df_acidentes_granular, ["tipo_acidente", "causa_acidente", "classificacao_acidente", "condicao_metereologica", "tracado_via", "uso_solo", "tipo_veiculo", "estado_fisico"])

# COMMAND ----------

df_acidentes_granular.display()

# COMMAND ----------

path = "AcidentesGranularDatatran"
load_data(df_acidentes_granular, path=path, layer='silver')

# COMMAND ----------

