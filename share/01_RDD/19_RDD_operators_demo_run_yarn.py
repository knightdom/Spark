# coding:utf8

from pyspark import SparkConf, SparkContext
from defs_19 import city_with_category
import json

if __name__ == '__main__':
    # 提交到yarn集群，master设置为yarn
    conf = SparkConf().setAppName('test').setMaster('yarn')
    # 如果提交到集群运行，除了主代码以外，还依赖了其他的代码文件
    # 需要设置一个参数，告知spark，还有依赖文件要同步上传到集群中
    # 参数叫做：spark.submit.pyFiles
    # 参数可以是单个.py文件，也可以是.zip压缩包（有多个依赖文件的时候可以用zip压缩包上传）
    conf.set("spark.submit.pyFiles", "./defs_19.py")
    sc = SparkContext(conf=conf)

    # 在集群中运行，需要用hdfs路径，本地路径不能用
    rdd = sc.textFile("/input/order.txt")

    # 进行rdd数据的split 按照｜进行分组，得到一个个的json
    jsons_rdd = rdd.flatMap(lambda line: line.split('|'))
    # 通过python自带的json库，将json字符串转化成字典对象
    dicts_rdd = jsons_rdd.map(lambda json_str: json.loads(json_str))
    # 过滤json对象的数据，仅保留北京的数据
    beijing_rdd = dicts_rdd.filter(lambda d: d['areaName'] == '北京')
    # 组合北京和商品类型形成新的字符串
    category_rdd = beijing_rdd.map(city_with_category)
    # 对结果进行去重
    result_rdd = category_rdd.distinct()
    # output
    print(result_rdd.collect())
