# coding:utf8

from pyspark import SparkConf, SparkContext

if __name__ == '__main__':
    conf = SparkConf().setAppName('test').setMaster('local[*]')
    sc = SparkContext(conf=conf)

    # 1. 读取文件获取数据，构建rdd
    file_rdd = sc.textFile("/input/words.txt")

    # 2. 通过flatMap api取出所有的单词
    word_rdd = file_rdd.flatMap(lambda line:line.split(' '))

    # 3. 将单词转换成元组，key是单词，value是1
    word_with_one_rdd = word_rdd.map(lambda x: (x, 1))

    # 4. 将相同key进行分组并集合
    result_rdd = word_with_one_rdd.reduceByKey(lambda a, b: a+b)

    # 5. 通过collect算子，将rdd的数据收集到Driver中，并打印输出
    print(result_rdd.collect())
