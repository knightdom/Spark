# coding:utf8

from pyspark import SparkConf, SparkContext

if __name__ == '__main__':
    conf = SparkConf().setAppName('test').setMaster('local[*]')
    sc = SparkContext(conf=conf)

    rdd = sc.parallelize([1,3,4,6,8,7],1)
    rdd.saveAsTextFile("/output/out1")

    rdd = sc.parallelize([1, 3, 4, 6, 8, 7], 3)
    rdd.saveAsTextFile("/output/out2")

    '''
    注意：和foreach算子类似，也是executor直接执行并输出
    '''