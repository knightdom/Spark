# coding:utf8

# SparkSession对象的导包
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StringType, IntegerType
import pandas as pd
import pyspark.sql.functions as F

if __name__ == '__main__':
    # 构建SparkSession执行环境入口对象
    spark = SparkSession.builder.\
        appName("test").\
        master("local[*]").\
        getOrCreate()
    sc = spark.sparkContext

    # 1 SQL风格进行处理
    rdd = sc.textFile('/input/words.txt').\
        flatMap(lambda x:x.split(' ')).\
        map(lambda x: [x])

    df = rdd.toDF(['word'])
    # 注册df为表格
    df.createTempView('words')
    spark.sql('SELECT word, COUNT(*) AS cnt FROM words GROUP BY word ORDER BY cnt DESC').show()

    # 2 DSL风格进行处理
    df = spark.read.format('text').load('/input/words.txt')
    # withColumn方法，对已存在的列进行操作，返回一个新的列，如果重名，就替换
    df2 = df.withColumn('value', F.explode(F.split(df['value'], " ")))
    df2.groupBy("value").\
        count().\
        withColumnRenamed('value', 'word').\
        withColumnRenamed('count', 'cnt').\
        orderBy('cnt', ascending=False).\
        show()
