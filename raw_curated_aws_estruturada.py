import pyspark.sql.functions as F
import pyspark.sql.types as T

from utils import get_spark_session


def leitura_csv(source_path):
    spark = get_spark_session("Raw to Curated AWS com Parâmetros no Spark-submit")

    schema = T.StructType([
            T.StructField('id', T.StringType(), False),
            T.StructField('nome', T.StringType(), True),
            T.StructField('status', T.StringType(), True),    
            T.StructField('cidade', T.StringType(), True),
            T.StructField('vendas', T.DoubleType(), True),
            T.StructField('data', T.DateType(), True)
    ])

    df = spark.read.csv(source_path, header=False, schema=schema)
    return df

def quebra_string_tipo(df):
    df_split = df.na.replace(["Noêmia   Orriça", ""], ["Noêmia Orriça", "Orriça"], "nome")

    df_split = df_split.withColumn("nome1", F.split(df_split["nome"], "\ ")[0]) \
                .withColumn("sobrenome", F.split(df_split["nome"], "\ ")[1]) \
                .drop("nome") \
                .withColumnRenamed("nome1", "primeiro_nome")
    return df_split

def calcula_media_vendas(df_split):
    df_media_vendas = df_split.filter(df_split.cidade.contains("P")).agg(F.round(F.avg(df_split.vendas), 2).alias("media_vendas"))
    return df_media_vendas

def agrupa_cidade_vendas(df):
    df_agrupado = df.groupBy("cidade").sum("vendas")
    return df_agrupado

def escreve_arquivo_parquet(df_agrupado, target_path):
    df_agrupado.write.partitionBy("cidade").mode('overwrite').parquet(target_path)

