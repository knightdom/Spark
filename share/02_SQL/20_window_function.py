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

    rdd = sc.parallelize([
        ('张三', 'class_1', 99),
        ('张二', 'class_2', 35),
        ('张一', 'class_3', 57),
        ('张四', 'class_4', 12),
        ('张五', 'class_5', 99),
        ('王一', 'class_1', 90),
        ('王二', 'class_2', 91),
        ('王三', 'class_3', 33),
        ('王四', 'class_4', 55),
        ('王五', 'class_5', 66),
        ('李一', 'class_1', 11),
        ('李二', 'class_2', 33),
        ('李三', 'class_3', 36),
        ('李四', 'class_1', 79),
        ('李五', 'class_2', 90),
    ])
    schema = StructType().add('name', StringType()).add('class', StringType()).add('score', IntegerType())
    df = rdd.toDF(schema)

    df.createTempView('stu')

    # 1 聚合窗口函数的演示
    spark.sql('''
        SELECT *, AVG(score) OVER() AS avg_score FROM stu
    ''').show()

    # 2 排序相关的窗口函数计算
    # RANK over, DENSE_RANK over, ROW_NUMBER over
    spark.sql('''
        SELECT *, ROW_NUMBER() OVER(ORDER BY score DESC) AS row_number_rank,
        DENSE_RANK() OVER(PARTITION BY class ORDER BY score DESC) AS dense_rank,
        RANK() OVER(ORDER BY score) AS rank
        FROM stu
    ''').show()

    # 3 NTILE
    spark.sql('''
        SELECT *, NTILE(6) OVER(ORDER BY score DESC) FROM stu
    ''').show()