# coding:utf8

# SparkSession对象的导包
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StringType, IntegerType
import pandas as pd

if __name__ == '__main__':
    # 构建SparkSession执行环境入口对象
    spark = SparkSession.builder.\
        appName("test").\
        master("local[*]").\
        getOrCreate()

    # 读取csv文件
    df = spark.read.format('csv').\
        option('sep', ';').\
        option('header', True).\
        option('encoding', 'utf-8').\
        schema('name STRING, age INT, job STRING').\
        load("/input/people.csv")

    df.printSchema()
    df.show()
