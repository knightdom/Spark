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

    # 构建StructType， text数据源，读取特点：将一整行只作为'一个列'读取，默认列名是value， 类型为String
    schema = StructType().add("data", StringType(), nullable=True)
    df = spark.read.format("text").schema(schema=schema).load("/input/people.txt")

    df.printSchema()
    df.show()
