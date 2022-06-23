# coding:utf8

from pyspark import SparkConf, SparkContext

if __name__ == '__main__':
    conf = SparkConf().setAppName('test').setMaster('local[*]')
    sc = SparkContext(conf=conf)

    rdd = sc.parallelize([1,3,4,6,8,3,6,7,4,5,2,7,8,3,5,0,6],1)
    print(rdd.takeSample(True, 30))
    print(rdd.takeSample(False, 30))
    print(rdd.takeSample(True, 5))