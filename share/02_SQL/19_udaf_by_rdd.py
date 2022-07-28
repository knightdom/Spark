# coding:utf8

# SparkSession对象的导包
import string
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

    rdd = sc.parallelize([1,2,3,4,5], 3)
    df = rdd.map(lambda x:[x]).toDF(['num'])

    # 折中的方式 就是使用RDD的mapPartitions算子来完成聚合操作
    # 如果用mapPartitions api完成UDAF聚合，一定要单分区
    single_partition_rdd = df.rdd.repartition(1)

    def process(iter):
        sum = 0
        for row in iter:
            sum += row['num']

        return [sum]

    print(single_partition_rdd.mapPartitions(process).collect())
