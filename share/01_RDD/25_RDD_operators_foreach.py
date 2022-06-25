# coding:utf8

from pyspark import SparkConf, SparkContext

if __name__ == '__main__':
    conf = SparkConf().setAppName('test').setMaster('local[*]')
    sc = SparkContext(conf=conf)

    rdd = sc.parallelize([1,3,4,6,8,7],1)

    result = rdd.foreach(lambda x: x*10)
    print(result)

    # 如果一定要返回的话
    rdd.foreach(lambda x: print(x*10))

    '''
    1. 如果需要返回的话，用map()，如果不需要返回值的话，用foreach()
    2. foreach()算子和其他算子不同，它是直接在executor上操作且不会返回给driver，
    因此在部分情况下，它的效率更高，例如将计算所得的结果插入到mysql中，每个executor直接插入到表中，
    如果collect()，需要发送给driver，然后统一插入到表中
    '''
