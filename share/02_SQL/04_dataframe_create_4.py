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

    # 基于Pandas的DataFrame构建SparkSQL的DataFrame对象
    pdf = pd.DataFrame(
        {
            "id": [1,2,3],
            "name": ['张三', '李四', '王五'],
            "age": [11, 21, 15]
        }
    )

    df = spark.createDataFrame(pdf)

    df.printSchema()
    df.show()