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

    # 注册成临时表
    df.createTempView("score")
    df.createOrReplaceTempView("score_2")
    df.createGlobalTempView("score_3")  # 注册全局视图，在使用的时候，需要在前面带上global_temp.前缀

    # 通过SparkSession对象的sql API完成sql语句的执行
    spark.sql("SELECT subject, COUNT(*) AS cnt FROM score GROUP BY subject").show()
    spark.sql("SELECT subject, COUNT(*) AS cnt FROM score_2 GROUP BY subject").show()
    spark.sql("SELECT subject, COUNT(*) AS cnt FROM global_temp.score_3 GROUP BY subject").show()
