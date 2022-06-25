# coding:utf8

from pyspark import SparkConf, SparkContext

if __name__ == '__main__':
    conf = SparkConf().setAppName('test').setMaster('local[*]')
    sc = SparkContext(conf=conf)

    rdd = sc.parallelize([1,3,4,6,8,7],3)

    print(rdd.repartition(1).getNumPartitions())
    print(rdd.repartition(5).getNumPartitions())

    # coalesce 修改分区
    print(rdd.coalesce(1).getNumPartitions())
    print(rdd.coalesce(5).getNumPartitions())
    print(rdd.coalesce(5, True).getNumPartitions())

