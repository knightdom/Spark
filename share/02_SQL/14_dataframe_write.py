# coding:utf8

# SparkSession对象的导包
import time

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StringType, IntegerType
import pandas as pd
import pyspark.sql.functions as F

if __name__ == '__main__':
    # 构建SparkSession执行环境入口对象
    spark = SparkSession.builder.\
        appName("test").\
        master("local[*]").\
        config("spark.sql.shuffle.partitions",2).\
        getOrCreate()
    sc = spark.sparkContext

    # 1 读取信息
    schema = StructType(). \
        add('user_id', StringType(), nullable=True). \
        add('movie_id', IntegerType(), nullable=True). \
        add('rank', IntegerType(), nullable=True). \
        add('ts', StringType(), nullable=True)
    df = spark.read.format('csv'). \
        option('sep', '\t'). \
        option('header', False). \
        option('encoding', 'utf-8'). \
        schema(schema=schema). \
        load("/input/u.data")

    # 写出到text，只能写出一个列的数据，需要将df转换成单列df
    df.select(F.concat_ws("---", 'user_id', 'movie_id', 'rank', 'ts')).\
        write.\
        mode('overwrite').\
        format('text').\
        save('/output/movie.txt')

    # 写出到csv
    df.write.mode('overwrite').\
        format('csv').\
        option('sep', ';').\
        option('header', True).\
        save('/output/movie.csv')

    # 写出到json
    df.write.mode('overwrite').\
        format('json').\
        save('/output/movie.json')

    # 写出到parquet
    df.write.mode('overwrite').\
        format('parquet').\
        save('/output/movie.parquet')