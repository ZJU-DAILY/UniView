from get_physical_plan import *
from procSQL import *
import sys
import csv
import numpy as np
import math
import pickle


# 从候选视图中选择topk
def getInitMVs_SP(q_mv_maps, clusters, cntDict, method="topk"):
    cidList = [] # list: 所有的视图id，包含重复的视图
    tmpDict = {} # dict: 视图id(str) : (视图id出现的次数, 视图出现的表的个数)
    mv_q_map = defaultdict(list) # dict: 视图id(int) : query集合(list)
    refThreshold = getRefThreshold() # 最小引用计数
    cntLimit = int(getMvCntLimit()) # topn视图数量

    # 过滤大表和小表join的情况
    # 如果大表和中表的数量超过2，保留；否则删除
    remove_mv_list = []
    for id, node in clusters.items():
        big_and_mid_table_cnt = 0
        big_table_cnt = 0
        small_table_cnt = 0
        total_table_cnt = len(node.filterNode.tableList)
        for table in node.filterNode.tableList:
            table = table.upper()
            if tables_bytes[table]["type"] != "small":
                big_and_mid_table_cnt += 1
            if tables_bytes[table]["type"] == "big":
                big_table_cnt += 1
            elif tables_bytes[table]["type"] == "small":
                small_table_cnt += 1
        # 规则1:如果没有大表，不考虑
        if big_table_cnt == 0:
            if id not in remove_mv_list:
                remove_mv_list.append(id)
        # 规则2:如果都是大表且大表数据大于等于4，数据量太大，不考虑
        if big_table_cnt == total_table_cnt and big_table_cnt >= 4:
            if id not in remove_mv_list:
                remove_mv_list.append(id)
        # 规则3:存在小表，不考虑
        if small_table_cnt > 0:
            if id not in remove_mv_list:
                remove_mv_list.append(id)

    # 获取id和原始查询的对应关系
    for q, mv in q_mv_maps.items():
        for mv_id in mv:
            mv_q_map[int(mv_id)].append(q)
        cidList += list(set(mv))
    # 获取每个cid被使用的次数及每个cid涉及的表的个数
    for id in set(cidList):
        if cntDict[int(id)] < refThreshold:
            continue
        if len(clusters[int(id)].relations) > 1 or len(clusters[int(id)].filterNode.tableList) > 1:
            tmpDict.update({id: (cidList.count(id), len(clusters[int(id)].relations), len(clusters[int(id)].filterNode.commonFilterList))})
    # 同tmpDict
    candDict = {k: v for k, v in tmpDict.items()}
    if len(tmpDict) < cntLimit:
        cntLimit = len(tmpDict)

    # 计算权值并写入csv文件
    cal_weight_select(clusters, q_mv_maps, cntDict)

    if method == "random":
        output = random_view_selection_method(tmpDict, cntLimit)
        print(output)
        return tmpDict, output
    elif method == "topk":
        # 根据配置文件，获取引用次数最多的N个
        output = topk_weighting_view_selection_method(candDict, clusters, mv_q_map, cntLimit, q_mv_maps, remove_mv_list)
        # output = sorted(output, key=lambda x: int(x))
        print(output)
        return candDict, output[:cntLimit]
    elif method == "weighting":
        output = weighting_view_selection_method(q_mv_maps, clusters, cntDict, cntLimit)
        print(output)
        return candDict, output[:cntLimit]
    else:
        # 匹配尽可能多的query
        output = topquery_view_selection_method(mv_q_map, cntDict, refThreshold)
        print(output)
        return candDict, output[:cntLimit]


# 计算权值并写入csv文件
def cal_weight_select(clusters, q_mv_maps, cntDict):
    cidList = []
    refThreshold = getRefThreshold()

    # 先获取每个mv的涉及的query数
    mv_q_len = {}  # mvid : 使用到的query数
    mv_q_map = defaultdict(list)  # mvid : query的list
    maxMVLen = -sys.maxsize
    minMVLen = sys.maxsize
    for q, mv in q_mv_maps.items():
        for mv_id in mv:
            mv_id = int(mv_id)
            mv_q_map[mv_id].append(q)
            if mv_id not in mv_q_len:
                mv_q_len[mv_id] = 1
            else:
                mv_q_len[mv_id] += 1
            maxMVLen = max(maxMVLen, mv_q_len[mv_id])
            minMVLen = min(minMVLen, mv_q_len[mv_id])
        cidList += list(set(mv))
    # 计算每个mv的空间成本
    mv_byte = {}
    maxTableByte = -sys.maxsize
    minTableByte = sys.maxsize
    for mv_id, node in clusters.items():
        mv_id = int(mv_id)
        tableByteSum = 0.0
        for table in node.filterNode.tableList:
            if table.find(" AS ") != -1:
                table = table.split(" AS ")[0]
            table = table.upper()
            if table not in tables_bytes:
                print("error:[weighting_view_selection__method] get table byte empty! {}".format(table))
                continue
            tableByteSum += float(tables_bytes[table.upper()]["size"])
        maxTableByte = max(maxTableByte, tableByteSum)
        minTableByte = min(minTableByte, tableByteSum)

        mv_byte[mv_id] = tableByteSum
    # 计算每个mv的过滤条件个数
    mv_filter = {}
    maxFilter = -sys.maxsize
    minFilter = sys.maxsize
    for mv_id, node in clusters.items():
        mv_id = int(mv_id)
        filterNum = len(node.filterNode.commonFilterList)
        maxFilter = max(maxFilter, filterNum)
        minFilter = min(minFilter, filterNum)
        mv_filter[mv_id] = filterNum

    # 归一化处理
    weighting_normalized(mv_q_len, maxMVLen, minMVLen)
    weighting_normalized(mv_byte, maxTableByte, minTableByte)
    weighting_normal_distribution(mv_filter, maxFilter, minFilter)

    # 集中处理下
    mv_dict = {}
    query_ratio = get_selection_query_ratio()
    space_ratio = get_selection_space_ratio()
    filter_ratio = get_selection_filter_ratio()
    for mv_id, node in clusters.items():
        mv_id = int(mv_id)
        # if mv_id in cntDict and cntDict[mv_id] < refThreshold:
        #     continue
        if mv_id not in mv_q_len or mv_id not in mv_byte or mv_id not in mv_filter:
            print("[weighting_view_selection__method] mv_q_len/mv_byte/mv_filter may not exists!")
            continue

        ratio = mv_q_len[mv_id] * float(query_ratio) + (1 - mv_byte[mv_id]) * float(space_ratio) + mv_filter[
            mv_id] * float(filter_ratio)
        mv_dict[mv_id] = ratio

    # 写入csv文件
    with open("./resources/data/clusters_data.csv", "w", newline="") as f:
        csv_writer = csv.writer(f, dialect="excel")
        for k, v in mv_dict.items():
            csv_writer.writerow([k, v])


# 计算权值并写入csv文件
def cal_weight_select_PG_CH(clusters, q_mv_maps, cntDict):
    cidList = []
    refThreshold = getRefThreshold()

    # 先获取每个mv的涉及的query数
    mv_q_len = {}  # mvid : 使用到的query数
    mv_q_map = defaultdict(list)  # mvid : query的list
    maxMVLen = -sys.maxsize
    minMVLen = sys.maxsize
    for q, mv in q_mv_maps.items():
        for mv_id in mv:
            mv_id = int(mv_id)
            mv_q_map[mv_id].append(q)
            if mv_id not in mv_q_len:
                mv_q_len[mv_id] = 1
            else:
                mv_q_len[mv_id] += 1
            maxMVLen = max(maxMVLen, mv_q_len[mv_id])
            minMVLen = min(minMVLen, mv_q_len[mv_id])
        cidList += list(set(mv))
    # 计算每个mv的空间成本
    mv_byte = {}
    maxTableByte = -sys.maxsize
    minTableByte = sys.maxsize
    for mv_id, node in clusters.items():
        mv_id = int(mv_id)
        tableByteSum = 0.0
        for table in node["Tree"].relations.keys():

            table = table.lower()
            if table not in tables_bytes:
                print("error:[weighting_view_selection__method] get table byte empty! {}".format(table))
                continue
            tableByteSum += float(tables_bytes[table.lower()])
        maxTableByte = max(maxTableByte, tableByteSum)
        minTableByte = min(minTableByte, tableByteSum)
        mv_byte[mv_id] = tableByteSum
    # # 计算每个mv的过滤条件个数
    # mv_filter = {}
    # maxFilter = -sys.maxsize
    # minFilter = sys.maxsize
    # for mv_id, node in clusters.items():
    #     mv_id = int(mv_id)
    #     filterNum = len(node.filterNode.commonFilterList)
    #     maxFilter = max(maxFilter, filterNum)
    #     minFilter = min(minFilter, filterNum)
    #     mv_filter[mv_id] = filterNum

    # 归一化处理
    weighting_normalized(mv_q_len, maxMVLen, minMVLen)
    weighting_normalized(mv_byte, maxTableByte, minTableByte)
    # weighting_normal_distribution(mv_filter, maxFilter, minFilter)

    # 集中处理下
    mv_dict = {}
    query_ratio = get_selection_query_ratio()
    space_ratio = get_selection_space_ratio()
    filter_ratio = get_selection_filter_ratio()
    for mv_id, node in clusters.items():
        mv_id = int(mv_id)
        # if mv_id in cntDict and cntDict[mv_id] < refThreshold:
        #     continue
        # if mv_id not in mv_q_len or mv_id not in mv_byte or mv_id not in mv_filter:
        if mv_id not in mv_q_len or mv_id not in mv_byte:
            print("[weighting_view_selection__method] mv_q_len/mv_byte/mv_filter may not exists!")
            continue

        # ratio = mv_q_len[mv_id] * float(query_ratio) + (1 - mv_byte[mv_id]) * float(space_ratio) + mv_filter[
        #     mv_id] * float(filter_ratio)
        ratio = mv_q_len[mv_id] * float(query_ratio) + mv_byte[mv_id] * float(space_ratio)
        mv_dict[mv_id] = ratio

    # 写入csv文件
    with open("./resources/data/clusters_data.csv", "w", newline="") as f:
        csv_writer = csv.writer(f, dialect="excel")
        for k, v in mv_dict.items():
            csv_writer.writerow([k, v])


# random算法
def random_view_selection_method(tmpDict, cntLimit):
    tmpList = random.sample([int(k) for k, v in tmpDict.items()], k=cntLimit)
    return tmpList


# 获取每个mv设计的表和数量
def get_mv_table_set(clusters, mv_id):
    mv_table_set = set()
    # 如果用relations可能遗漏
    for table in clusters[mv_id].filterNode.tableList:
        mv_table_set.add(table)
    join_type = ""
    if len(clusters[mv_id].filterNode.outerJoinFilterList) > 0:
        join_type = clusters[mv_id].filterNode.outerJoinFilterList[0][3]
    mv_table_num = len(mv_table_set)
    mv_table_set.add(join_type)

    return join_type, mv_table_num, mv_table_set


# 将列表内的表不处理
def remove_table_in_list_old(table_list, remove_list=["DATE_DIM"]):
    # 检查remove_list中的表
    for table in remove_list:
        if table.lower() in table_list:
            return False
    return True


# 根据表的大小处理表的情况
def remove_table_size_old(planTree, remove_list=["DATE_DIM"]):
    table_list = planTree.relations.keys()
    if not remove_table_in_list_old(table_list):
        return False
    # 检查大表和小表join的情况
    small_table = 0
    big_table = 0
    for table in table_list:
        table = table.upper()
        if table == "OUTER_JOIN":
            continue
        if tables_bytes[table]["type"] == "small":
            small_table += 1
        elif tables_bytes[table]["type"] == "big":
            big_table += 1
    if len(table_list) == 2:
        if small_table > 0 and big_table > 0:
            return False
    return True



# topk带权重
def topk_weighting_view_selection_method(candDict, clusters, mv_q_map, cntLimit, q_mv_maps, remove_mv_list):
    # 根据配置文件，获取引用次数最多的N个
    # 排序，先按照引用次数降序，再按照表的个数升序
    tmpList = sorted(candDict.items(), key=lambda x: (x[1][0], x[1][2]), reverse=True)
    # tmpList = sorted(candDict.items(), key=lambda x: x[1], reverse=True)
    cntDict = defaultdict(list)
    # 按引用次数排序之后，从中选择覆盖更多表的候选视图
    for x in tmpList:
        cntDict[x[1]].append(int(x[0]))
    print(cntDict)

    coveredRel = set()
    coveredQuery = set()
    output = []
    table_set_dict = []
    # （1）在尽可能多的覆盖原始查询的同时，主要筛选大表和大表join的情况
    for ids in cntDict.values():
        for mv_id in ids:
            # 去掉一些不需要考虑的视图
            if mv_id in remove_mv_list:
                continue
            if mv_id in output:
                continue
            if clusters[mv_id].isGroupBy:
                continue
            # 获取join类型、表的数量、表的set集合
            join_type, mv_table_num, mv_table_set = get_mv_table_set(clusters, mv_id)
            if join_type == "SEMI JOIN" or join_type == "ANTI JOIN":
                continue
            if mv_table_set in table_set_dict:
                continue
            # 大小表
            if not remove_table_size(clusters[mv_id].filterNode.tableList):
                continue

            # 表的个数大于4舍弃
            if mv_table_num > 3:
                continue
            # 处理表的情况
            if mv_table_num == 2 and len(clusters[mv_id].filterNode.joinFilterList) == 3 and len(clusters[mv_id].filterNode.commonFilterList) == 0:
                remove_mv_list.append(mv_id)
                continue

            # query集合重合的情况
            newQuerySet = set(mv_q_map[mv_id])
            if newQuerySet.issubset(coveredQuery):
                continue

            table_set_dict.append(mv_table_set)
            coveredQuery = coveredQuery.union(set(mv_q_map[mv_id]))
            output.append(mv_id)

        if len(output) >= cntLimit or len(coveredQuery) == len(q_mv_maps):
            break

    # （2）在筛选大表和大表的同时，不考虑覆盖原始query的情况
    for ids in cntDict.values():
        for mv_id in ids:
            if mv_id in remove_mv_list:
                continue
            if mv_id in output:
                continue
            if clusters[mv_id].isGroupBy:
                continue
            # 获取join类型、表的数量、表的set集合
            join_type, mv_table_num, mv_table_set = get_mv_table_set(clusters, mv_id)
            if join_type == "SEMI JOIN" or join_type == "ANTI JOIN":
                continue
            if mv_table_set in table_set_dict:
                continue

            # 大小表
            if not remove_table_size(clusters[mv_id].filterNode.tableList):
                continue
            # 表的个数大于4也舍弃
            if mv_table_num > 3:
                continue

            output.append(mv_id)
            table_set_dict.append(mv_table_set)
            coveredQuery = coveredQuery.union(set(mv_q_map[mv_id]))
        if len(output) >= cntLimit or len(coveredQuery) == len(q_mv_maps):
            break

    # （3）考虑表的数量为3，且过滤条件大于等于3的情况
    for ids in cntDict.values():
        for mv_id in ids:
            if mv_id in remove_mv_list:
                continue
            if mv_id in output:
                continue
            if clusters[mv_id].isGroupBy:
                continue
            # mv_id的表
            mv_table_set = set()
            for table in clusters[mv_id].filterNode.tableList:
                mv_table_set.add(table)
            join_type = ""
            if len(clusters[mv_id].filterNode.outerJoinFilterList) > 0:
                join_type = clusters[mv_id].filterNode.outerJoinFilterList[0][3]
            mv_table_num = len(mv_table_set)
            mv_table_set.add(join_type)
            if join_type == "SEMI JOIN" or join_type == "ANTI JOIN":
                continue
            if mv_table_set in table_set_dict:
                continue

            # 表的个数大于4也舍弃
            if mv_table_num > 3:
                continue

            com_fil_num = len(clusters[mv_id].filterNode.commonFilterList)
            if mv_table_num == 2:
                continue
            if mv_table_num == 3 and com_fil_num < 3:
                continue

            output.append(mv_id)
            table_set_dict.append(mv_table_set)
            coveredQuery = coveredQuery.union(set(mv_q_map[mv_id]))
        if len(output) >= cntLimit or len(coveredQuery) == len(q_mv_maps):
            break

    # （4）考虑group by的视图
    for ids in cntDict.values():
        for mv_id in ids:
            if mv_id in output:
                continue
            if not clusters[mv_id].isGroupBy:
                continue

            # print(cntDict[mv_id])
            output.append(mv_id)
        if len(output) >= cntLimit or len(coveredQuery) == len(q_mv_maps):
            break


    # （4）考虑覆盖率因素加入的——只是用于测试出覆盖率上限
    # 再尽可能多的覆盖原始查询
    # for ids in cntDict.values():
    #     for mv_id in ids:
    #         if mv_id in output:
    #             continue
    #         newQuerySet = set(mv_q_map[mv_id])
    #         if newQuerySet.issubset(coveredQuery):
    #             continue
    #         output.append(mv_id)
    #         coveredQuery = coveredQuery.union(set(mv_q_map[mv_id]))
    #     if len(output) >= cntLimit or len(coveredQuery) == len(q_mv_maps):
    #         break



    # （4）考虑表的数量为2，且过滤条件大于等于1的情况
    # for ids in cntDict.values():
    #     for mv_id in ids:
    #         if mv_id in remove_mv_list:
    #             continue
    #         if mv_id in output:
    #             continue
    #         # mv_id的表
    #         mv_table_set = set()
    #         for table in clusters[mv_id].filterNode.tableList:
    #             mv_table_set.add(table)
    #         join_type = ""
    #         if len(clusters[mv_id].filterNode.outerJoinFilterList) > 0:
    #             join_type = clusters[mv_id].filterNode.outerJoinFilterList[0][3]
    #         mv_table_num = len(mv_table_set)
    #         mv_table_set.add(join_type)
    #         if join_type == "SEMI JOIN" or join_type == "ANTI JOIN":
    #             continue
    #         if mv_table_set in table_set_dict:
    #             continue
    #
    #         # 表的个数大于4也舍弃
    #         if mv_table_num > 3:
    #             continue
    #
    #         com_fil_num = len(clusters[mv_id].filterNode.commonFilterList)
    #         if mv_table_num == 2 and com_fil_num < 1:
    #             continue
    #         if mv_table_num == 3 and com_fil_num < 3:
    #             continue
    #
    #         output.append(mv_id)
    #         table_set_dict.append(mv_table_set)
    #         coveredQuery = coveredQuery.union(set(mv_q_map[mv_id]))
    #     if len(output) >= cntLimit or len(coveredQuery) == len(q_mv_maps):
    #         break

    # for ids in cntDict.values():
    #     for mv_id in ids:
    #         if mv_id in remove_mv_list:
    #             continue
    #         # mv_id的表
    #         mv_table_set = set()
    #         for table in clusters[mv_id].filterNode.tableList:
    #             mv_table_set.add(table)
    #         join_type = ""
    #         if len(clusters[mv_id].filterNode.outerJoinFilterList) > 0:
    #             join_type = clusters[mv_id].filterNode.outerJoinFilterList[0][3]
    #         if join_type == "LEFT OUTER JOIN":
    #             print(12)
    #             print(34)
    #         mv_table_num = len(mv_table_set)
    #         mv_table_set.add(join_type)
    #
    #         # 大小表
    #         # 表的个数大于5也舍弃
    #         if mv_table_num > 5 and len(clusters[mv_id].filterNode.commonFilterList) < 2:
    #             continue
    #         # 处理表的情况
    #         if mv_table_num == 2 and len(clusters[mv_id].filterNode.joinFilterList) == 3 and len(clusters[mv_id].filterNode.commonFilterList) == 0:
    #             continue
    #         if mv_id in output:
    #             continue
    #         if mv_table_set in table_set_dict:
    #             continue
    #
    #         output.append(mv_id)
    #         table_set_dict.append(mv_table_set)
    #         coveredQuery = coveredQuery.union(set(mv_q_map[mv_id]))
    #     if len(output) >= cntLimit or len(coveredQuery) == len(q_mv_maps):
    #         break


    sort_output_list = [v for v in output]
    for tmp in tmpList:
        if int(tmp[0]) not in sort_output_list:
            sort_output_list.append(int(tmp[0]))
    with open(get_sort_mv_test_csv(), "w", newline="") as f:
        csv_writer = csv.writer(f, dialect="excel")
        for tmp in sort_output_list:
            csv_writer.writerow([tmp])

    '''
    计算clusters_data.csv
    '''
    # 将其他元素也一并加入数组
    all_table_list = []
    for x in output:
        all_table_list.append(x)
    for x in tmpList:
        x = int(x[0])
        if x not in output:
            all_table_list.append(x)
    for id, node in clusters.items():
        id = int(id)
        if id not in all_table_list:
            all_table_list.append(id)
    # 给所有的id赋值
    all_table_dict = {}
    for i in range(len(all_table_list)):
        all_table_dict[all_table_list[i]] = len(all_table_list) - i
    with open("./resources/data/clusters_data.csv", "w", newline="") as f:
        csv_writer = csv.writer(f, dialect="excel")
        for id, node in clusters.items():
            id = int(id)
            csv_writer.writerow([id, all_table_dict[id]])

    return output[:cntLimit]


# topk带权重
def topk_weighting_view_selection_method_new(candDict, clusters, mv_q_map, cntLimit, q_mv_maps, remove_mv_list):
    # 根据配置文件，获取引用次数最多的N个
    tmpList = sorted(candDict.items(), key=lambda x: x[1])
    cntDict = defaultdict(list)
    # 按引用次数排序之后，从中选择覆盖更多表的候选视图
    for x in reversed(tmpList):
        cntDict[x[1]].append(int(x[0]))
    print(cntDict)

    coveredRel = set()
    coveredQuery = set()
    output = []

    # 1.先按照规则过滤表
    # 先尽可能多的覆盖表
    for ids in cntDict.values():
        for mv_id in ids:
            # 处理表的情况
            if mv_id in remove_mv_list:
                continue
            if mv_id in output:
                continue
            newRelSet = set(clusters[mv_id].relations.keys())
            if newRelSet.issubset(coveredRel):
                continue
            output.append(mv_id)
            coveredRel = coveredRel.union(newRelSet)
            coveredQuery = coveredQuery.union(set(mv_q_map[mv_id]))
            # 如果提前满额或者cover完所有的表之后，暂时退出，去覆盖查询
        if len(output) >= cntLimit or len(coveredRel) == len(g_table.data):
            break
    print(f"len {len(output)}")
    # 再尽可能多的覆盖原始查询
    for ids in cntDict.values():
        for mv_id in ids:
            # 处理表的情况
            if mv_id in remove_mv_list:
                continue
            if mv_id in output:
                continue
            newQuerySet = set(mv_q_map[mv_id])
            if newQuerySet.issubset(coveredQuery):
                continue
            output.append(mv_id)
            coveredQuery = coveredQuery.union(set(mv_q_map[mv_id]))
        if len(output) >= cntLimit or len(coveredQuery) == len(q_mv_maps):
            break
    print(f"len {len(output)}")
    # 2.如果这时候还没有达到cntLimit，那么关闭限制
    # 先尽可能多的覆盖表
    for ids in cntDict.values():
        for mv_id in ids:
            if mv_id in output:
                continue
            newRelSet = set(clusters[mv_id].relations.keys())
            if newRelSet.issubset(coveredRel):
                continue
            output.append(mv_id)
            coveredRel = coveredRel.union(newRelSet)
            coveredQuery = coveredQuery.union(set(mv_q_map[mv_id]))
            # 如果提前满额或者cover完所有的表之后，暂时退出，去覆盖查询
        if len(output) >= cntLimit or len(coveredRel) == len(g_table.data):
            break
    print(f"len {len(output)}")
    # 再尽可能多的覆盖原始查询
    for ids in cntDict.values():
        for mv_id in ids:
            if mv_id in output:
                continue
            newQuerySet = set(mv_q_map[mv_id])
            if newQuerySet.issubset(coveredQuery):
                continue
            output.append(mv_id)
            coveredQuery = coveredQuery.union(set(mv_q_map[mv_id]))
        if len(output) >= cntLimit or len(coveredQuery) == len(q_mv_maps):
            break
    print(f"len {len(output)}")
    return output[:cntLimit]


# 按引用计数、每个视图使用到的query数、涉及到表的大小、过滤条件个数加权处理
def weighting_view_selection_method(q_mv_maps, clusters, cntDict, cntLimit, recommend_list=[]):
    cidList = []
    refThreshold = getRefThreshold()

    # 先获取每个mv的涉及的query数
    mv_q_len = {} # mvid : 使用到的query数
    mv_q_map = defaultdict(list) # mvid : query的list
    maxMVLen = -sys.maxsize
    minMVLen = sys.maxsize
    for q, mv in q_mv_maps.items():
        for mv_id in mv:
            mv_id = int(mv_id)
            mv_q_map[mv_id].append(q)
            if mv_id not in mv_q_len:
                mv_q_len[mv_id] = 1
            else:
                mv_q_len[mv_id] += 1
            maxMVLen = max(maxMVLen, mv_q_len[mv_id])
            minMVLen = min(minMVLen, mv_q_len[mv_id])
        cidList += list(set(mv))
    # 计算每个mv的空间成本
    mv_byte = {}
    maxTableByte = -sys.maxsize
    minTableByte = sys.maxsize
    for mv_id, node in clusters.items():
        mv_id = int(mv_id)
        tableByteSum = 0.0
        for table in node.filterNode.tableList:
            if table.find(" AS ") != -1:
                table = table.split(" AS ")[0]
            table = table.upper()
            if table not in tables_bytes:
                print("error:[weighting_view_selection__method] get table byte empty! {}".format(table))
                continue
            tableByteSum += float(tables_bytes[table.upper()]["size"])
            maxTableByte = max(maxTableByte, float(tables_bytes[table.upper()]["size"]))
            minTableByte = min(minTableByte, float(tables_bytes[table.upper()]["size"]))

        mv_byte[mv_id] = tableByteSum
    # 计算每个mv的过滤条件个数
    mv_filter = {}
    maxFilter = -sys.maxsize
    minFilter = sys.maxsize
    for mv_id, node in clusters.items():
        mv_id = int(mv_id)
        filterNum = len(node.filterNode.commonFilterList)
        maxFilter = max(maxFilter, filterNum)
        minFilter = min(minFilter, filterNum)
        mv_filter[mv_id] = filterNum

    # 归一化处理
    weighting_normalized(mv_q_len, maxMVLen, minMVLen)
    weighting_normalized(mv_byte, maxTableByte, minTableByte)
    weighting_normal_distribution(mv_filter, maxFilter, minFilter)

    # 集中处理下
    mv_dict = {}
    query_ratio = get_selection_query_ratio()
    space_ratio = get_selection_space_ratio()
    filter_ratio = get_selection_filter_ratio()
    for mv_id, node in clusters.items():
        mv_id = int(mv_id)
        if mv_id in cntDict and cntDict[mv_id] < refThreshold:
            continue
        if mv_id not in mv_q_len or mv_id not in mv_byte or mv_id not in mv_filter:
            print("[weighting_view_selection__method] mv_q_len/mv_byte/mv_filter may not exists!")
            continue

        ratio = mv_q_len[mv_id] * float(query_ratio) + (1 - mv_byte[mv_id]) * float(space_ratio) + mv_filter[mv_id] * float(filter_ratio);
        mv_dict[mv_id] = ratio

    mv_list = [(k, v) for k, v in mv_dict.items()]
    tmpList = sorted(mv_list, key=lambda x: x[1])
    tmpList = reversed(tmpList)
    querySet = set()
    output = []
    for x in tmpList:
        mvId, mvList = x[0], mv_q_map[x[0]]
        mvSet = set(mvList)
        if len(recommend_list) != 0 and int(mvId) not in recommend_list:
            continue
        if cntDict[int(mvId)] < refThreshold:
            continue
        if mvSet.issubset(querySet):
            continue
        output.append(mvId)
        querySet = querySet.union(mvSet)
        if len(output) >= cntLimit:
            break

    return output[:cntLimit]


# 尽可能匹配多的query
def topquery_view_selection_method(mv_q_map, cntDict, refThreshold):
    mv_list = [(k, v) for k, v in mv_q_map.items()]
    tmpList = sorted(mv_list, key=lambda x: len(x[1]))
    tmpList = reversed(tmpList)
    querySet = set()
    output = []
    for x in tmpList:
        mvId, mvList = x[0], x[1]
        mvSet = set(mvList)
        if cntDict[int(mvId)] < refThreshold:
            continue
        if mvSet.issubset(querySet):
            continue
        output.append(mvId)
        querySet = querySet.union(mvSet)
        if len(output) >= cntLimit:
            break

    return output

# 用于筛选推荐的视图
# 从推荐的视图中选择topk
def get_recommend_init_mv_SP(q_mv_maps, clusters, cntDict, method="topk"):
    cidList = []
    tmpDict = {}
    refThreshold = getRefThreshold()

    # 打开csv文件
    recommend_list = []
    recommend_path = getRecommendMvCSV()
    with open(recommend_path, "r") as f:
        csv_reader = csv.reader(f)
        for line in csv_reader:
            mv_id = line[0].split(".")[1].split("mv")[1]
            recommend_list.append(int(mv_id))

    # 获取所有的cid（query之间不去重）
    mv_q_map = defaultdict(list)
    # 获取id和原始查询的对应关系
    for q, mv in q_mv_maps.items():
        mv_set = set()
        for mv_id in mv:
            mv_id = int(mv_id)
            if mv_id not in recommend_list:
                continue
            mv_q_map[mv_id].append(q)
            mv_set.add(mv_id)
        cidList += list(mv_set)
    # 获取每个cid被使用的次数及每个cid涉及的表的个数
    for id in set(cidList):
        if cntDict[int(id)] < refThreshold:
            continue
        if len(clusters[int(id)].relations) > 1:
            tmpDict.update({id: (cidList.count(id), len(clusters[int(id)].relations))})

    cntLimit = int(getMvCntLimit())
    candDict = {k: v for k, v in tmpDict.items()}
    if len(tmpDict) < cntLimit:
        cntLimit = len(tmpDict)
    if method == "random":
        tmpList = random.sample([int(k) for k, v in tmpDict.items()], k=cntLimit)
        output = tmpList
        print(output)
        return tmpDict, output
    elif method == "topk":
        # 根据配置文件，获取引用次数最多的N个
        tmpList = sorted(tmpDict.items(), key=lambda x: x[1])[-cntLimit:]
        output = [int(x[0]) for x in tmpList]
        print(output)
        return tmpDict, output
    elif method == "topkk":
        # 根据配置文件，获取引用次数最多的N个
        tmpList = sorted(candDict.items(), key=lambda x: x[1])
        cntDict = defaultdict(list)
        # 按引用次数排序之后，从中选择覆盖更多表的候选视图
        for x in reversed(tmpList):
            cntDict[x[1]].append(int(x[0]))
        print(cntDict)

        coveredRel = set()
        coveredQuery = set()
        output = []
        # 先尽可能多的覆盖表
        for ids in cntDict.values():
            for mv_id in ids:
                newRelSet = set(clusters[mv_id].relations.keys())
                if newRelSet.issubset(coveredRel):
                    continue
                output.append(mv_id)
                coveredRel = coveredRel.union(newRelSet)
                coveredQuery = coveredQuery.union(set(mv_q_map[mv_id]))
                # 如果提前满额或者cover完所有的表之后，暂时退出，去覆盖查询
            if len(output) >= cntLimit or len(coveredRel) == len(g_table.data):
                break

        # 再尽可能多的覆盖原始查询
        for ids in cntDict.values():
            for mv_id in ids:
                if mv_id in output:
                    continue
                newQuerySet = set(mv_q_map[mv_id])
                if newQuerySet.issubset(coveredQuery):
                    continue
                output.append(mv_id)
                coveredQuery = coveredQuery.union(set(mv_q_map[mv_id]))
            if len(output) >= cntLimit or len(coveredQuery) == len(q_mv_maps):
                break

        print(output)
        return candDict, output[:cntLimit]
    elif method == "weighting":
        output = weighting_select_method(q_mv_maps, clusters, cntDict, cntLimit, recommend_list)
        print(output)
        return candDict, output[:cntLimit]
    else:
        # 匹配尽可能多的query
        mv_list = [(k, v) for k, v in mv_q_map.items()]
        tmpList = sorted(mv_list, key=lambda x: len(x[1]))
        tmpList = reversed(tmpList)
        querySet = set()
        output = []
        for x in tmpList:
            mvId, mvList = x[0], x[1]
            mvSet = set(mvList)
            if cntDict[int(mvId)] < refThreshold:
                continue
            if mvSet.issubset(querySet):
                continue
            output.append(mvId)
            querySet = querySet.union(mvSet)
            if len(output) >= cntLimit:
                break

        print(output)
        return candDict, output[:cntLimit]


# 归一化公式
def normalized_float(x, max_x, min_x):
    return ((float(x) - float(min_x)) + 1e-7) / (float(max_x) - float(min_x))


# 归一化处理
def weighting_normalized(mv_data, max_data, min_data):
    for mv_id in mv_data.keys():
        mv_data[mv_id] = normalized_float(mv_data[mv_id], max_data, min_data)


# 正态分布
def weighting_normal_distribution(mv_data, max_data, min_data):
    cur_sum = 0.0
    for mv_id, value in mv_data.items():
        cur_sum += value
    # 计算平均值
    n = len(mv_data)
    u = cur_sum / n
    sig = 0.0
    for value in mv_data.values():
        sig += (value - u) * (value - u)
    sig = math.sqrt(sig / n)
    # 计算正态分布
    maxSig = -sys.maxsize
    minSig = sys.maxsize
    for mv_id, value in mv_data.items():
        y_sig = np.exp(-(value - u) ** 2 / (2 * sig ** 2)) / (math.sqrt(2 * math.pi) * sig)
        mv_data[mv_id] = y_sig
        maxSig = max(maxSig, y_sig)
        minSig = min(minSig, y_sig)
    # 赋值
    for mv_id in mv_data.keys():
        mv_data[mv_id] = normalized_float(mv_data[mv_id], maxSig, minSig)


# 获取每个表对应的存储空间大小——根据cnt，即最大最小
def get_tables_bytes_cnt():
    big_table = int(get_table_size_big_bound())
    small_table = int(get_table_size_small_bound())
    database = getDatabase()
    spark_session = init_pyspark(database)
    tables_bytes = {}
    for table in g_table.data:
        mv_table_bytes = get_mv_bytes(spark_session, table)
        tables_bytes[table] = {"size": float(mv_table_bytes), "type": "mid"}
    spark_session.stop()

    # 按大小排序，取前big_table作为大表
    tmpList = [(k, v) for k, v in tables_bytes.items()]
    tmpList = sorted(tmpList, key=lambda x:x[1]["size"])

    i = 0
    while i < len(tables_bytes):
        if i < small_table:
            tables_bytes[tmpList[i][0]]["type"] = "small"
        if len(tables_bytes) - i <= big_table:
            tables_bytes[tmpList[i][0]]["type"] = "big"
        i += 1

    return tables_bytes


# 获取每个表对应的存储空间大小——根据上下界
def get_tables_bytes_bound():
    database = getDatabase()
    spark_session = init_pyspark(database)
    tables_bytes = {}
    table_size_lower_bound = int(get_table_size_lower_bound())
    table_size_upper_bound = int(get_table_size_upper_bound())
    for table in g_table.data:
        mv_table_bytes = int(get_mv_bytes(spark_session, table))
        if mv_table_bytes < table_size_lower_bound:
            tables_bytes[table] = {"size": float(mv_table_bytes), "type": "small"}
        elif mv_table_bytes > table_size_upper_bound:
            tables_bytes[table] = {"size": float(mv_table_bytes), "type": "big"}
        else:
            tables_bytes[table] = {"size": float(mv_table_bytes), "type": "mid"}

    return tables_bytes


def get_tables_bytes_ds():
    tables_bytes = {}
    with open("./resources/data/tables_bytes.ds", "rb") as f:
        tables_bytes = pickle.load(f)
    return tables_bytes


def save_tables_bytes_ds(tables_bytes):
    with open("./resources/data/tables_bytes.ds", "wb") as f:
        pickle.dump(tables_bytes, f)


def get_tables_bytes_bound_ds_pg():
    try:
        with open("./resources/data/table_bytes_pg.ds", "rb") as f:
          table_bytes = pickle.load(f)
    except:
        table_bytes = {}
    return table_bytes


def get_tables_bytes_bound_ds_ch():
    try:
        with open("./resources/data/table_bytes_pg.ds", "rb") as f:
            table_bytes = pickle.load(f)
    except:
        table_bytes = {}
    return table_bytes




tables_bytes = get_tables_bytes_cnt() if get_engine() == "spark" \
    else (get_tables_bytes_bound_ds_pg() if get_engine() == "PG" else get_tables_bytes_bound_ds_ch())
# save_tables_bytes_ds(tables_bytes)
# tables_bytes = get_tables_bytes_ds()
# save_tables_bytes_ds(tables_bytes)
# print(tables_bytes)
# tables_bytes = {
# 	'CALL_CENTER': {'size': 7616.0, 'type': 'small'},
# 	'CATALOG_PAGE': {'size': 404960.0, 'type': 'small'},
# 	'CATALOG_RETURNS': {'size': 315874039.0, 'type': 'big'},
# 	'CATALOG_SALES': {'size': 2839463765.0, 'type': 'big'},
# 	'CUSTOMER': {'size': 22810574.0, 'type': 'mid'},
# 	'CUSTOMER_ADDRESS': {'size': 3732442.0, 'type': 'small'},
# 	'CUSTOMER_DEMOGRAPHICS': {'size': 78507.0, 'type': 'small'},
# 	'DATE_DIM': {'size': 191206.0, 'type': 'small'},
# 	'HOUSEHOLD_DEMOGRAPHICS': {'size': 1434.0, 'type': 'small'},
# 	'INCOME_BAND': {'size': 665.0, 'type': 'small'},
# 	'INVENTORY': {'size': 62781279.0, 'type': 'big'},
# 	'ITEM': {'size': 3280431.0, 'type': 'small'},
# 	'PROMOTION': {'size': 18205.0, 'type': 'small'},
# 	'REASON': {'size': 1256.0, 'type': 'small'},
# 	'SHIP_MODE': {'size': 2023.0, 'type': 'small'},
# 	'STORE': {'size': 11283.0, 'type': 'small'},
# 	'STORE_RETURNS': {'size': 470240344.0, 'type': 'big'},
# 	'STORE_SALES': {'size': 4108987368.0, 'type': 'big'},
# 	'TIME_DIM': {'size': 133501.0, 'type': 'small'},
# 	'WAREHOUSE': {'size': 3281.0, 'type': 'small'},
# 	'WEB_PAGE': {'size': 10252.0, 'type': 'small'},
# 	'WEB_RETURNS': {'size': 155372063.0, 'type': 'big'},
# 	'WEB_SALES': {'size': 1556159890.0, 'type': 'big'},
# 	'WEB_SITE': {'size': 8163.0, 'type': 'small'}
# }


# 将列表内的表不处理
def remove_table_in_list(table_list, remove_list=["date_dim", "item", "inventory", "store", "customer", "household_demographics", "warehouse"]):
    # 检查remove_list中的表
    for table in remove_list:
        if table.lower() in table_list:
            return False
    return True


# 根据表的大小处理表的情况
def remove_table_size(table_list, remove_list=["date_dim", "item", "inventory", "store", "customer", "household_demographics", "warehouse"]):
    if not remove_table_in_list(table_list, remove_list):
        return False
    # 检查大表和小表join的情况
    small_table = 0
    big_table = 0
    for table in table_list:
        table = table.upper()
        if tables_bytes[table]["type"] == "small":
            small_table += 1
        elif tables_bytes[table]["type"] == "big":
            big_table += 1
    if len(table_list) == 2:
        if big_table != 2:
            return False
    if big_table < 2:
        return False

    return True


# 根据表的大小处理表的情况，和上边差不多
def remove_table_size_less(table_list, remove_list=["date_dim", "item", "inventory", "store", "customer", "household_demographics", "warehouse"]):
    if not remove_table_in_list(table_list, remove_list):
        return False
    # 检查大表和小表join的情况
    small_table = 0
    big_table = 0
    for table in table_list:
        table = table.upper()
        if tables_bytes[table]["type"] == "small":
            small_table += 1
        elif tables_bytes[table]["type"] == "big":
            big_table += 1
    if len(table_list) == 2:
        if big_table != 2:
            return False
    if big_table < 2:
        return False

    return True


if __name__ == "__main__":
    pass
