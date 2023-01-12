import os
import re
import ress
from typing import DefaultDict, Dict, List, Tuple
from collections import Counter, defaultdict
from procSQL import *


# 列出所有文件
def findAllFile(base):
    for root, ds, fs in os.walk(base):
        for f in fs:
            if f.endswith('.sql'):
                fullname = os.path.join(root, f)
                yield fullname


# 将解析完的SQL语句写入list
def get_trans(baseDir, schemaDict):
    # base = './join-order-benchmark/'  # 文件夹路径
    base = baseDir  # 文件夹路径
    # itemAll : defaultdict[str, set] = {}
    itemAll = defaultdict(list)
    condAll = defaultdict(set)
    index = 0
    for f in findAllFile(base):
        # if f.find("/query53.sql") == -1:
        #     continue
        # if f.find("/query63.sql") != -1:
        #     continue
        # if f.find("/query75.sql") == -1:
        #     continue
        print(f)
        with open(f) as file:
            data = file.read()
            sql = normalizeSql(data)
            item_table, cond_table = ress.get_itemstable(sql, schemaDict)
            # # 处理为list
            # for item, itemsets in item_table.items():
            #     item_table[item] = [str(itemset).strip() for itemset in itemsets]
            # print(item_table)
            for item_name, item in item_table.items():
                itemAll[item_name].append([str(itemset).strip() for itemset in item])
            for item_name, item in cond_table.items():
                condAll[item_name] = condAll[item_name].union(item)

    # 如果条件字段在select后面已经有了，在cond里面就不再重复出现
    for table in condAll.keys():
        # condAll[table] = [str(itemset) for itemset in cols]
        if table not in itemAll:
            continue
        cols = condAll[table]
        for col in cols.copy():
            if str(itemAll[table]).find(col) == -1:
                continue
            condAll[table].remove(col)

    ansItem = defaultdict(dict)
    for item_name, item in itemAll.items():
        if item_name == '' or item_name is None:
            continue
        map = defaultdict(list)
        index = 0
        for data in item:
            map['T' + str(10000 + index)] = data
            index += 1
        ansItem[item_name] = map

    return ansItem, condAll
    # return itemAll, condAll


if __name__ == '__main__':
    itemAll = get_trans()
    for item_name, name in itemAll.items():
        print(item_name)
        print(name)
