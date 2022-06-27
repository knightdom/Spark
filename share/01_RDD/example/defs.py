# coding:utf8

import jieba


def context_jieba(data):
    # 通过jieba库 进行分词操作
    seg = jieba.cut_for_search(data)
    res = list()
    for datum in seg:
        res.append(datum)

    return res


def filter_words(data):
    # 过滤不要的字
    return data not in ['+', ':', '/', '.', '的', '[', ']']


def extract_user_word(data):
    # 传入内容是 元组 （1，地震）
    user_id = data[0]
    content = data[1]
    words = context_jieba(content)

    return_list = list()
    for word in words:
        # 过滤不要的词
        if filter_words(word):
            return_list.append((user_id + '_' + word, 1))

    return return_list
