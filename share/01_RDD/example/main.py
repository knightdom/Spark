# coding:utf8
import jieba
from pyspark import SparkConf, SparkContext
from pyspark.storagelevel import StorageLevel
from defs import context_jieba, filter_words, extract_user_word
from operator import add
import os
import sys

os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

if __name__ == '__main__':
    # conf = SparkConf().setAppName('test').setMaster('local[*]')
    # conf.set("spark.submit.pyFiles", "./defs.py")
    # 集群运行
    conf = SparkConf().setAppName('test')
    sc = SparkContext(conf=conf)

    # 1. 读取数据文件
    file_rdd = sc.textFile("/input/SogouQ.reduced")

    # 2. 对数据进行切分
    split_rdd = file_rdd.map(lambda line: line.split("\t"))

    # 3.split_rdd作为基础的rdd会被多次使用
    split_rdd.persist(StorageLevel.DISK_ONLY)

    # TODO: 需求1： 用户搜索的关键词分析
    # 将所有的搜索内容取出
    # print(split_rdd.takeSample(True, 5))
    context_rdd = split_rdd.map(lambda x: x[2].strip('[]'))
    # print(context_rdd.collect())

    # 对搜索内容进行分词
    words_rdd = context_rdd.flatMap(context_jieba)
    # print(words_rdd.collect())
    # 过滤不要的内容
    filter_rdd = words_rdd.filter(filter_words)
    # 转换成元组
    final_words_rdd = filter_rdd.map(lambda x: (x, 1))
    # 对单词进行分组聚合，并求出前5名
    result1 = final_words_rdd.reduceByKey(lambda a,b: a+b).sortBy(lambda x: x[1], ascending=False, numPartitions=1).take(5)

    print('需求1的结果：', result1)

    # TODO: 需求2： 用户和关键词组合分析
    user_content_rdd = split_rdd.map(lambda x: (x[1], x[2].strip('[]')))
    # 对用户的搜索信息进行分词，然后与用户ID再次组合
    user_word_with_one_rdd = user_content_rdd.flatMap(extract_user_word)
    # 对单词进行分组聚合，并求出前5名
    result2 = user_word_with_one_rdd.reduceByKey(lambda a,b: a+b).sortBy(lambda x: x[1], ascending=False, numPartitions=1).take(5)
    print('需求2的结果：', result2)

    # TODO: 需求3： 热门搜索时间段分析
    # 取出所有的时间
    time_rdd = split_rdd.map(lambda x: x[0])
    # 时间仅保存小时精度
    hour_with_one_rdd = time_rdd.map(lambda x: (x.split(':')[0], 1))
    # 分组 聚合 排序
    result3 = hour_with_one_rdd.reduceByKey(add).sortBy(lambda x:x[1], ascending=False, numPartitions=1).collect()
    print('需求3的结果：', result3)
