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

    # 读取文件
    df = spark.read.format("csv").\
        schema('id INT, subject STRING, score INT').\
        load("/input/stu_score.txt")

    # Column对象的获取
    id_column = df['id']
    subjcet_column = df['subject']

    # DSL风格演示
    df.select(['id', 'subject']).show()
    df.select('id', 'subject').show()
    df.select(id_column, subjcet_column).show()

    # filter api
    df.filter('score = 99').show()
    df.filter(df['score'] == 99).show()

    # where api
    df.where('score < 99').show()
    df.where(df['score'] < 99).show()

    # group by api
    # df.groupBy()返回值类型GroupedData
    # GroupedData对象不是DataFrame
    # 是一个有分组关系的数据结构，用于聚合操作，例如sum, avg, count, min, max
    # 调用聚合操作后，返回的才是DataFrame
    df.groupBy('subject').count().show()
    df.groupBy(df['subject']).count().show()
