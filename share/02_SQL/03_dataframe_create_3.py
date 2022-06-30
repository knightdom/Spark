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

    # toDF的方式1 构建DataFrame
    df1 = rdd.toDF(["name", "age"])
    df1.printSchema()
    df1.show()

    # toDF的方式2 通过StructType来构建
    schema = StructType().add("name", StringType(), nullable=True).add("age", IntegerType(), nullable=False)

    df2 = rdd.toDF(schema=schema)
    df2.printSchema()
    df2.show()
