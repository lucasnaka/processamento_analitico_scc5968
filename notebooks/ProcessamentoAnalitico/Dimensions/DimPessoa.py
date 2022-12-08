# Databricks notebook source
# MAGIC %run ../utils_functions

# COMMAND ----------

layer = 'bronze'
path = 'AcidentesGranularDatatran'
table_name = "acidente_granular_table"
query = """
    SELECT DISTINCT
        pesid AS id_pessoa,
        sexo,
        idade
    FROM acidente_granular_table
    WHERE
        pesid != "NA"
"""
df_pessoa = extract_data(path, layer, file_type='csv', query=query, table_name=table_name, encoding="ISO-8859-1")

# COMMAND ----------

df_pessoa = generate_primary_key(df_pessoa, "pk_pessoa")

# COMMAND ----------

from pyspark.sql.types import IntegerType
df_pessoa = df_pessoa.withColumn("idade", df_pessoa["idade"].cast(IntegerType()))

# COMMAND ----------

from pyspark.sql.functions import when

df_pessoa = df_pessoa.withColumn("faixa_etaria", \
              when(df_pessoa["idade"] < 21, "De 0 a 20 anos") \
                .when(df_pessoa["idade"] < 41,"De 21 a 40 anos") \
                .when(df_pessoa["idade"] < 61,"De 41 a 60 anos") \
                .when(df_pessoa["idade"] < 81,"De 61 a 80 anos") \
                .when(df_pessoa["idade"] >= 81,"Acima de 80 anos")                           
                                 .otherwise("Não Informado")) \





# COMMAND ----------

df_pessoa.display()

# COMMAND ----------

path = "Dimensions/DimPessoa"
load_data(df_pessoa, path=path, layer='gold')

# COMMAND ----------

