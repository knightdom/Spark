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

    '''
    spark.sql.shuffle.partitions 参数指的是，在sql计算中，shuffle算子阶段默认的分区数是200个，
    对于集群模式来说，200个也比较合适
    在local下，2/4/10都可
    这个参数和spark RDD中设置并行度的参数 是相互独立的
    '''

    # 1 读取信息
    schema = StructType().\
        add('user_id', StringType(), nullable=True).\
        add('movie_id', IntegerType(), nullable=True).\
        add('rank', IntegerType(), nullable=True).\
        add('ts', StringType(), nullable=True)
    df = spark.read.format('csv').\
        option('sep', '\t').\
        option('header', False).\
        option('encoding', 'utf-8').\
        schema(schema=schema).\
        load("/input/u.data")

    # 2-1 用户平均分
    df.groupBy("user_id").\
        avg('rank').\
        withColumnRenamed('avg(rank)', 'avg_rank').\
        withColumn('avg_rank', F.round('avg_rank', 2)).\
        orderBy('avg_rank', ascending=False).\
        show()

    # 2-2 电影平均分
    df.createTempView('movie')
    spark.sql('''
        SELECT movie_id, ROUND(avg(rank), 2) AS avg_rank FROM movie GROUP BY movie_id ORDER BY avg_rank DESC;
    ''').show()

    # 2-3 查询大于平均分的电影的数量
    print('大于平均分的电影的数量：', df.where(df['rank'] > df.select(F.avg(df['rank'])).first()['avg(rank)']).count())

    # 2-4 查询高分电影(>3)打分次数最多的用户，此人打分的平均分
    # 先找这个人
    user_id = df.where("rank > 3").\
        groupBy('user_id').\
        count().\
        withColumnRenamed('count', 'cnt').\
        orderBy('cnt', ascending=False).\
        limit(1).\
        first()['user_id']
    # 计算这个人的打分平均分
    df.where(df['user_id'] == user_id).\
        select(F.round(F.avg('rank'), 2)).show()

    # 2-5 查询每个用户的平均打分，最低打分，最高打分
    df.groupBy('user_id').\
        agg(    # 可以做多个聚合
            F.round(F.avg('rank'), 2).alias('avg_rank'),
            F.min('rank').alias('min_rank'),
            F.max('rank').alias('max_rank')
    ).show()

    # 2-6 查询评分超过100次的电影的平均分，排名top10
    df.groupBy('movie_id').\
        agg(
        F.count('movie_id').alias('cnt_movie'),
        F.round(F.avg('rank'), 2).alias('avg_rank'),
    ).where('cnt_movie > 100').\
        orderBy('avg_rank', ascending=False).\
        limit(10).show()

'''
1 agg 是GroupedData对象的API，可以写多个聚合
2 alias 是Column对象的API，针对一个列进行改名
3 withColumnRenamed 是DataFrame的API，可以对DF中的列进行改名，一次该一个列，可链式调用
4 orderBy DataFrame的API，进行排序，参数1是被排序的列名，参数2是升序（True）/降序（False）
5 first DataFrame的API，取出DF的第一行数据，返回值结果是Row对象
    Row对象，就是一个数组，可以通过row['列名']来取出当前行中，某一列的具体数值，返回值不再是DF、Column、Row对象，而是一个数值
'''