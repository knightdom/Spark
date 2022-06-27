# coding:utf8

import jieba

if __name__ == '__main__':
    content = "小明硕士毕业于宁波大学计算机学院，后在浙江大学深造"

    result = jieba.cut(content, True)
    print(list(result))

    result2 = jieba.cut(content, False)
    print(list(result2))

    # 搜索引擎模式
    result3 = jieba.cut_for_search(content)
    print(",".join(result3))
