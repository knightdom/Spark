# coding:utf8

from pyspark import SparkConf, SparkContext
import json

if __name__ == '__main__':
    conf = SparkConf().setAppName('test').setMaster('local[*]')
    sc = SparkContext(conf=conf)

    rdd = sc.textFile("/input/order.txt")

    # 进行rdd数据的split 按照｜进行分组，得到一个个的json
    jsons_rdd = rdd.flatMap(lambda line: line.split('|'))
    # 通过python自带的json库，将json字符串转化成字典对象
    dicts_rdd = jsons_rdd.map(lambda json_str: json.loads(json_str))
    # 过滤json对象的数据，仅保留北京的数据
    beijing_rdd = dicts_rdd.filter(lambda d: d['areaName'] == '北京')
    # 组合北京和商品类型形成新的字符串
    category_rdd = beijing_rdd.map(lambda x: x['areaName'] + '-' + x['category'])
    # 对结果进行去重
    result_rdd = category_rdd.distinct()
    # output
    print(result_rdd.collect())
