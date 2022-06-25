# coding:utf8

from pyspark import SparkConf, SparkContext
from pyspark.storagelevel import StorageLevel

if __name__ == '__main__':
    conf = SparkConf().setAppName('test').setMaster('local[*]')
    sc = SparkContext(conf=conf)

    # 1. 告知spark，开启checkPoint功能
    sc.setCheckpointDir("/output/ckp")
    rdd1 = sc.textFile("/input/words.txt")
    rdd2 = rdd1.flatMap(lambda x:x.split(' '))
    rdd3 = rdd2.map(lambda  x: (x,1))

    # 2. 调用checkpoint API保留数据
    rdd3.checkpoint()

    rdd4 = rdd3.reduceByKey(lambda a, b: a+b)
    print(rdd4.collect())

    rdd5 = rdd3.groupByKey()
    rdd6 = rdd5.mapValues(lambda  x: sum(x))
    print(rdd6.collect())
    rdd3.unpersist()
