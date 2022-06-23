# coding:utf8

from pyspark import SparkConf, SparkContext

if __name__ == '__main__':
    conf = SparkConf().setAppName('test').setMaster('local[*]')
    sc = SparkContext(conf=conf)

    rdd = sc.parallelize([('a', 1), ('b', 1), ('a', 1), ('b', 1), ('a', 1)])

    rdd2 = rdd.groupByKey()

    print(rdd2.collect())
    print(rdd2.map(lambda x: (x[0], list(x[1]))).collect())

    '''
    groupByKey仅仅只返回value
    groupBy返回的是(key, value)的元组
    '''
