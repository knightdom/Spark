# coding:utf8

from pyspark import SparkConf, SparkContext

if __name__ == '__main__':
    conf = SparkConf().setAppName('test').setMaster('local[*]')
    sc = SparkContext(conf=conf)

    rdd1 = sc.parallelize([(1001, "zhangsan"), (1002, "lisi"), (1003, "wangwu")])
    rdd2 = sc.parallelize([(1003, "wangwu"), (1004, "zhaoliu")])

    # 通过intersection求rdd之间的交集
    rdd3 = rdd1.intersection(rdd2)
    print(rdd3.collect())
