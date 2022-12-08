# Databricks notebook source
from pyspark.sql import DataFrame
import pyspark.sql.functions as F
from pyspark.sql.window import Window
from pyspark.sql.functions import monotonically_increasing_id

storage_account_access_key = dbutils.secrets.get('ProcessamentoAnaliticoStorageKey', 'StorageAccountKey')

storage_account_name = "processamentoanalitico"
output_container_name = 'dados-acidentes'

spark.conf.set("fs.azure.account.key."+storage_account_name+".blob.core.windows.net", storage_account_access_key)

def list_files_in_path(path: str, layer: str) -> None:
    for file in dbutils.fs.ls(f"wasbs://{layer}@processamentoanalitico.blob.core.windows.net/{path}"):
        print(file)

def extract_data(path: str, layer: str, query: str=None, table_name: str="table", file_type: str='csv', delimiter: str=';', encoding: str='utf-8') -> DataFrame:
    storage_account_name = "processamentoanalitico"
    
    file_location = f"wasbs://{layer}@{storage_account_name}.blob.core.windows.net/{path}"
    has_partition = sum([file.size for file in dbutils.fs.ls(file_location)]) == 0
    
    if not has_partition:
        file_path = file_location + "/*.{file_type}"
    else:
        file_path = file_location + "/*/*.{file_type}"
    
    df = (spark.read.format(file_type)
          .option("inferSchema", "true")
          .option("delimiter", delimiter)
          .option("header", "true")
          .option("encoding", encoding)
          .load(file_location)
         )
    
    df.createOrReplaceTempView(table_name)
    if query:
        df = spark.sql(query)
    else:
        df = spark.sql(f"SELECT * FROM {table_name}")
    
    return df

def load_data(df: DataFrame, path: str, layer: str, partition_by: str = None, mode: str = "overwrite") -> None:
    output_container_path = f"wasbs://{layer}@processamentoanalitico.blob.core.windows.net"
    output_blob_folder = f"{output_container_path}/{path}"

    if partition_by:
        (df
            .coalesce(1)
            .write
            .mode(mode)
            .option("header", "true")
            .partitionBy(partition_by)
            .parquet(output_blob_folder)
        )
    else:
        (df
            .coalesce(1)
            .write
            .mode(mode)
            .option("header", "true")
            .parquet(output_blob_folder)
        )
        
def generate_primary_key(df: DataFrame, pk_name: str) -> DataFrame:
    df = df.withColumn("monotonically_increasing_id", F.monotonically_increasing_id())
    window = Window.orderBy(F.col('monotonically_increasing_id'))
    df = df.withColumn(pk_name, F.row_number().over(window))
    df = df.drop("monotonically_increasing_id")
    
    return df