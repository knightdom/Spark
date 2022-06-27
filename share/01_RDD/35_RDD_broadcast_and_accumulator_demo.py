# coding:utf8

from pyspark import SparkConf, SparkContext
import re

if __name__ == '__main__':
    conf = SparkConf().setAppName('test').setMaster('local[*]')
    sc = SparkContext(conf=conf)

    # 1 读取数据文件
    file_rdd = sc.textFile('/input/accumulator_broadcast_data.txt')
    # 2 将特殊字符变成广播变量
    abnormal_char = [',', '.', '!', '#', '$', '%']
    broadcast = sc.broadcast(abnormal_char)
    # 3 对特殊字符出现的次数做累加，使用spark的累加器
    acmlt = sc.accumulator(0)
    # 4 数据处理，先处理空行，在python中有内容为True，没内容None就是False
    lines_rdd = file_rdd.filter(lambda line: line.strip())
    # 5 去除每行前后的空格
    data_rdd = lines_rdd.map(lambda line: line.strip())
    # 6 对数据进行切分，按照正则表达式切分，因为空格分隔符无法正确切分有多个空格的情况
    word_rdd = data_rdd.flatMap(lambda line: re.split("\s+", line))


    # 7 需要过滤数据，仅保留正常字符，删除特殊字符并累加次数
    def filter_func(data):
        # 仅保留正常字符，删除特殊字符并累加次数
        global acmlt
        abnormal_chars = broadcast.value
        if data in abnormal_chars:
            acmlt += 1
            return False
        else:
            return True

    filter_rdd = word_rdd.filter(filter_func)
    # 8 计算正常字符的个数
    result_rdd = filter_rdd.map(lambda x: (x, 1)).reduceByKey(lambda a, b: a+b)

    print('正常单词计数结果：',result_rdd.collect())
    print('特殊字符数量：', acmlt)
