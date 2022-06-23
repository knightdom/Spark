# coding:utf8

from pyspark import SparkConf, SparkContext

if __name__ == '__main__':
    conf = SparkConf().setAppName('test').setMaster('local[*]')
    sc = SparkContext(conf=conf)

    rdd = sc.parallelize([('a', 11), ('F', 1), ('w', 7), ('b', 1), ('c', 9), ('O', 3), ('l', 4), ('f', 8), ('d', 1), ('A', 6), ('h', 4), ('e', 8), ('d', 1), ('s', 6)], 3)

    # 使用sortByKey对rdd进行排序
    print(rdd.sortByKey(ascending=True, numPartitions=1, keyfunc=lambda key: str(key).lower()).collect())
