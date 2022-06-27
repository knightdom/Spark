# coding:utf8

from pyspark import SparkConf, SparkContext
from pyspark.storagelevel import StorageLevel

if __name__ == '__main__':
    conf = SparkConf().setAppName('test').setMaster('local[*]')
    sc = SparkContext(conf=conf)

    stu_info_list = [
        (1, '张三', 11),
        (2, '李四', 13),
        (3, '王五', 11),
        (4, '赵六', 11),
    ]
    # 1. 将本地python list对象标记为广播变量
    broadcast = sc.broadcast(stu_info_list)

    score_info_rdd = sc.parallelize([
        (1, '语文', 99),
        (1, '数学', 99),
        (1, '英语', 99),
        (1, '理综', 99),
        (2, '语文', 99),
        (2, '数学', 99),
        (2, '英语', 99),
        (2, '理综', 99),
        (3, '语文', 99),
        (3, '数学', 99),
        (3, '英语', 99),
        (3, '理综', 99),
        (4, '语文', 99),
        (4, '数学', 99),
        (4, '英语', 99),
        (4, '理综', 99),
    ])

    def map_fun(data):
        id = data[0]
        name = ""

        # 2. 使用到本地集合对象的地方，从广播变量中取出用即可
        # for stu_info in stu_info_list:
        for stu_info in broadcast.value:
            stu_id = stu_info[0]
            if id == stu_id:
                name = stu_info[1]
        return (name, data[1], data[2])

    print(score_info_rdd.map(map_fun).collect())

'''
场景：本地集合对象(小数据集 几k几兆) 和 分布式集合对象(RDD)进行关联的时候
类似ADB中的维度表和数据表
需要将本地集合对象 封装成广播变量
可以节省：
1 网络io的次数
2 Executor的内存占用
'''
