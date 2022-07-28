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

    # 构建一个rdd
    rdd = sc.parallelize(range(1,10)).map(lambda x:[x])
    df = rdd.toDF(["num"])

    # 方式1 sparksession.udf.register(), DSL和SQL风格都可以使用
    # UDF的处理函数
    def num_x_10(num):
        return num*10
    # 参数1：注册的UDF的名称，这个UDF名称，仅用于SQL风格
    # 参数2：UDF的处理逻辑，是个单独的方法
    # 参数3：声明UDF的返回值类型，注意：UDF注册时，必须声明返回值类型，并且UDF的真实返回值必须和声明的返回值一致
    # 返回值对象：这是一个UDF对象，仅可以用于DSL语法
    # 这种方式定义的UDF，可以通过参数1的名称用于SQL风格，通过返回值对象用于DSL风格
    udf2 = spark.udf.register('udf1', num_x_10, IntegerType())

    # SQL风格中使用
    # selectExpr 以select的表达式执行，表达式SQL风格的表达式（字符串）
    # select方法，接受普通的字符串字段名，或者返回值是Column对象的计算
    df.selectExpr("udf1(num)").show()

    # DSL风格使用
    # 返回值UDF对象，如果作为方法使用，传入的参数一定是column对象
    df.select(udf2(df['num'])).show()


    # 方式2 仅用于DSL风格
    udf3 = F.udf(num_x_10, IntegerType())
    df.select(udf3(df['num'])).show()
