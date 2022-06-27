# coding:utf8

from pyspark import SparkConf, SparkContext
from pyspark.storagelevel import StorageLevel

if __name__ == '__main__':
    conf = SparkConf().setAppName('test').setMaster('local[*]')
    sc = SparkContext(conf=conf)

    rdd = sc.parallelize(range(1, 11), 2)

    # count = 0
    # Spark提供的累加器变量，参数是初始值
    count = sc.accumulator(0)

    def map_func(data):
        global count
        count += 1
        # print(count)

    rdd2 = rdd.map(map_func)
    rdd2.cache()
    rdd2.collect()

    # 使用累加器时，需要注意因为rdd不会保存过程rdd，因此rdd3会导致累加器再累加一次，需要用cache缓存一下
    rdd3 = rdd2.map(lambda x:x)
    rdd3.collect()
    print(count)
