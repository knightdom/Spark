from pyspark import SparkConf, SparkContext

if __name__ == '__main__':
    # 构建SparkContext对象
    conf = SparkConf().setAppName("test").setMaster("local[*]")
    sc = SparkContext(conf=conf)

    # 通过textFile API读取数据

    # 读取本地文件
    file_rdd1 = sc.textFile("/input/words.txt")
    print("默认读取分区数：", file_rdd1.getNumPartitions())
    print("file_rdd1内容：", file_rdd1.collect())
