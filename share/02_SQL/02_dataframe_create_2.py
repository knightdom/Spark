# coding:utf8

# SparkSession对象的导包
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StringType, IntegerType

if __name__ == '__main__':
    # 构建SparkSession执行环境入口对象
    spark = SparkSession.builder.\
        appName("test").\
        master("local[*]").\
        getOrCreate()

    # 基于RDD转换成DataFrame
    # 通过SparkSession对象获取SparkContext对象
    sc = spark.sparkContext
    rdd = sc.textFile("/input/people.txt").\
        map(lambda x: x.split(",")).\
        map(lambda x: (x[0], int(x[1])))

    # 构建表结构的描述对象：StructType对象
    schema = StructType().add("name", StringType(), nullable=True).add("age", IntegerType(), nullable=False)

    # 基于StructType对象去构建RDD到DF的转换
    df = spark.createDataFrame(rdd, schema=schema)

    df.printSchema()
    df.show()
