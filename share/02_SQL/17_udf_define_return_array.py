# coding:utf8

# SparkSession对象的导包
import time

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StringType, IntegerType, ArrayType
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

    # 构建一个rdd
    rdd = sc.parallelize([['hadoop spark flink'], ['hadoop flink java']])
    df = rdd.toDF(['line'])

    # 注册UDF，UDF的执行函数定义
    def split_line(data):
        return data.split(" ")  # 返回一个Array对象

    # 方式1构建udf
    udf2 = spark.udf.register("udf1", split_line, ArrayType(StringType()))
    # DLS风格
    df.select(udf2(df['line'])).show()
    # SQL风格
    df.createTempView("lines")
    spark.sql("SELECT udf1(line) FROM lines").show(truncate=False)

    # 方式2构建udf
    udf3 = F.udf(split_line, ArrayType(StringType()))
    df.select(udf3(df['line'])).show(truncate=False)
