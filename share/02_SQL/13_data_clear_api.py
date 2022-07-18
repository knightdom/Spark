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

    # 读取数据
    df = spark.read.format('csv').\
        option('sep', ';').\
        option('header', True).\
        load('/input/people_new.csv')

    # 数据清洗：数据去重
    # dropDuplicates是DataFrame的API，可以完成数据去重
    # 无参数使用，对全部的列联合起来进行比较，去除重复值
    df.dropDuplicates().show()
    df.dropDuplicates(['age', 'job']).show()

    # 数据清洗：缺失值处理：删除
    # dropna API可以处理有空值的数据项
    # 无参数使用，只要列中有null，就删除这一行数据
    df.dropna().show()
    # thresh=3表示，最少满足3个有效列，不满足，就删除当前行数据
    df.dropna(thresh=3).show()
    df.dropna(thresh=2, subset=['name', 'age']).show()

    # 数据清洗：缺失值处理：填充
    df.fillna('loss').show()
    # 指定列进行填充
    df.fillna('N/A', subset=['job']).show()
    # 通过一个字典，对各列提供填充规则
    df.fillna({"name":"未知", "age":1, "job": "N/A"}).show()