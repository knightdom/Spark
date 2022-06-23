# coding:utf8

from pyspark import SparkConf, SparkContext

if __name__ == '__main__':
    conf = SparkConf().setAppName('test').setMaster('local[*]')
    sc = SparkContext(conf=conf)

    rdd = sc.parallelize(range(1, 10))

    # 通过filter算子，过滤奇数
    result = rdd.filter(lambda x: x % 2 == 1)

    print(result.collect())