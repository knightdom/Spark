# coding:utf8

# 导入spark相关包
from pyspark import SparkConf, SparkContext

if __name__ == '__main__':
    # 1. 初始化执行环境，构建SparkContext对象
    conf = SparkConf().setAppName('test').setMaster("local[*]")
    sc = SparkContext(conf=conf)

    # 2. 通过并行化集合的方式创建RDD
    rdd = sc.parallelize([1, 2, 3, 4, 5, 6, 7, 8, 9])
    # parallelize方法，默认分区是根据CPU核心来定的
    print("默认分区数：", rdd.getNumPartitions())

    rdd = sc.parallelize([1, 2, 3, 4, 5, 6, 7, 8, 9], 3)
    print("分区数：", rdd.getNumPartitions())

    # 3. collect方法，是将RDD（分布式对象）中每个分区的数据，都发送到Driver中，形成一个python list对象
    print("rdd的内容是：", rdd.collect())
