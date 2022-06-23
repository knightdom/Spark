# coding:utf8

from pyspark import SparkConf, SparkContext

if __name__ == '__main__':
    conf = SparkConf().setAppName('test').setMaster('local[*]')
    sc = SparkContext(conf=conf)

    rdd1 = sc.parallelize([1, 1, 1, 2, 2, 2, 2, 3, 3, 3])
    rdd2 = sc.parallelize(['a', 'b', 'c'])

    rdd3 = rdd1.union(rdd2)
    print(rdd3.collect())

    '''
    1. union算子不会去重
    2. RDD的类型不同也可以合并
    '''
