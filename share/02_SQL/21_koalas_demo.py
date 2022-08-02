# coding:utf8

# SparkSession对象的导包
import pandas as pd
import numpy as np
import databricks.koalas as ks
from pyspark.sql import SparkSession

if __name__ == '__main__':
    # 构建SparkSession执行环境入口对象
    spark = SparkSession.builder. \
        appName("test"). \
        master("local[*]"). \
        config("spark.sql.shuffle.partitions", 2). \
        getOrCreate()

    dates = pd.date_range('20130101', periods=6)
    pdf = pd.DataFrame(np.random.randn(6,4), index=dates, columns=list('ABCD'))
    # 从pandas转换成koalas 方式1
    # sdf = spark.createDataFrame(pdf)
    # kdf = sdf.to_koalas()
    # 从pandas转换成koalas 方式2
    kdf = ks.from_pandas(pdf)

    # 通过koalas直接创建Dataframe
    ks.DataFrame({'A': ['foo', 'bar'], 'B': [1,2]})

    # 使用koalas的API
    kh = kdf.head()
    ki = kdf.index
    kc = kdf.columns

    # koalas的Dataframe转换成numpy
    ndf = kdf.to_numpy()
