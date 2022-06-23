# coding:utf8

from pyspark import SparkConf, SparkContext

if __name__ == '__main__':
    conf = SparkConf().setAppName('test').setMaster('local[*]')
    sc = SparkContext(conf=conf)

    rdd = sc.parallelize([('a', 11), ('f', 1), ('a', 7), ('b', 1), ('c', 9), ('a', 3), ('e', 4), ('f', 8), ('d', 1), ('a', 6)], 3)

    # 使用sortBy对rdd进行排序

    # 按照value数字排序
    # 参数1函数，表示告知spark按照数据中的哪列进行排序
    # 参数2： True表示升序，False表示降序
    # 参数3： 排序的分区数
    print(rdd.sortBy(lambda x: x[1], ascending=True, numPartitions=3).collect())
    '''
    注意，如果在集群中要全局有序，排序分区数请设置为1，如果多个，每个executor中有序，多分区之间可能无序
    '''

    # 按照key进行降序排序
    print(rdd.sortBy(lambda x: x[0], ascending=False, numPartitions=1).collect())
