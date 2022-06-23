# coding:utf8

from pyspark import SparkConf, SparkContext

if __name__ == '__main__':
    conf = SparkConf().setAppName('test').setMaster('local[*]')
    sc = SparkContext(conf=conf)

    rdd = sc.parallelize([1, 2, 3, 4, 5, 6, 7, 8, 9])


    # 定义方法，作为算子的传入函数体
    def add(data):
        return data * 10


    print(rdd.map(add).collect())

    # 定义lambda表达式写匿名函数
    print(rdd.map(lambda x: x * 10).collect())
