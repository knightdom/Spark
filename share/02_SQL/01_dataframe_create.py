# coding:utf8

# SparkSession对象的导包
from pyspark.sql import SparkSession

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

    # 构建DataFrame对象
    # 参数1 被转换的rdd
    # 参数2 指定列名，通过list的形式指定，按照顺序一次提供字符串名称即可
    df = spark.createDataFrame(rdd, schema=['name', 'age'])

    # 打印DataFrame的表结构
    df.printSchema()

    # 打印df中的数据
    # 参数1 展示出多少条数据，默认20
    # 参数2 表示是否截断，如果列的数据长度超过20个字符串长度，后续的内容以...显示
    # 如果是False，表示全部显示，默认是False
    df.show(20, False)

    # 将DF对象转换成临时视图表，可供sql语句查询
    df.createOrReplaceTempView("people")
    spark.sql("SELECT * FROM people WHERE age < 30").show()

