# coding:utf8

from pyspark import SparkConf, SparkContext

if __name__ == '__main__':
    conf = SparkConf().setAppName('test').setMaster('local[*]')
    sc = SparkContext(conf=conf)

    rdd1 = sc.parallelize(range(1,10), 3)

    print(rdd1.collect())
    print(rdd1.glom().collect())
