# coding:utf8

from pyspark import SparkConf, SparkContext

if __name__ == '__main__':
    conf = SparkConf().setAppName('test').setMaster('local[*]')
    sc = SparkContext(conf=conf)

    rdd = sc.parallelize([('a', 1), ('b', 1), ('c', 1), ('b', 1), ('c', 1), ('b', 1), ('a', 1)])

    # 使用partitionBy自定义分区
    def process(k):
        if 'a' == k or 'b' == k: return 0
        if 'c' == k: return 1
        return 2

    print(rdd.partitionBy(3, process).glom().collect())
