import random

from parsePlanJson import *
from parsePipelineTime_CH import *
from procCSVFile import *
import os.path
import gc
import pickle
from z3 import *
from FPMAX import *
from procConf import *
import shutil
from graphviz import Digraph
# from clickhouse_driver import Client, connect
import subprocess


def getInitMVs(q_mv_maps, clusters, method="topk"):
    cidList = []
    tmpDict = {}
    refThreshold = getRefThreshold()
    # tmpList = []
    # 获取所有的cid（query之间不去重）
    mv_q_map = defaultdict(list)
    # 获取id和原始查询的对应关系
    for q, mv in q_mv_maps.items():
        for mv_id in mv:
            mv_q_map[int(mv_id)].append(q)
        cidList += list(set(mv))
    # 获取每个cid被使用的次数及每个cid涉及的表的个数
    for id in set(cidList):
        if int(clusters[int(id)]["Cnt"]) < refThreshold:
            continue
        if len(clusters[int(id)]["Tree"].relations) > 1:
            tmpDict.update({id: (cidList.count(id), len(clusters[int(id)]["Tree"].relations), int(id))})
    cntLimit = int(getMvCntLimit())
    candDict = {k: v for k, v in tmpDict.items() if v[0] >= refThreshold}
    # print(candDict)
    count = len(candDict)
    if count < cntLimit:
        cntLimit = count
    #     随机选择N个
    if method == "random":
        tmpList = random.sample([int(k) for k, v in candDict.items()], k=cntLimit)
        output = tmpList
        print(output)
        return candDict, output
    else:
        # 根据配置文件，获取引用次数最多的N个
        # tmpList = sorted(candDict.items(), key=lambda x: x[1])[-cntLimit:]
        tmpList = sorted(candDict.items(), key=lambda x: (x[1][0],-x[1][1], x[1][2]))
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
                newRelSet = set(clusters[mv_id]["Tree"].relations.keys())
                if newRelSet.issubset(coveredRel):
                    continue
                output.append(mv_id)
                coveredRel = coveredRel.union(newRelSet)
                coveredQuery = coveredQuery.union(set(mv_q_map[mv_id]))
                # 如果提前满额或者cover完所有的表之后，暂时退出，去覆盖查询
            if len(output) >= cntLimit or len(coveredRel) == len(g_table.data):
                break

        # print(output)
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

        # output = [int(x[0]) for x in tmpList]
        print(output)
        return candDict, output[:cntLimit]


def getInitMVs_CH(q_mv_maps, clusters, method="topk"):
    cidList = []
    tmpDict = {}
    refThreshold = getRefThreshold()
    # tmpList = []
    # 获取所有的cid（query之间不去重）
    mv_q_map = defaultdict(list)
    # 获取id和原始查询的对应关系
    for q, mv in q_mv_maps.items():
        for mv_id in mv:
            mv_q_map[int(mv_id)].append(q)
        cidList += list(set(mv))
    # 获取每个cid被使用的次数及每个cid涉及的表的个数
    for id in set(cidList):
        if int(clusters[int(id)]["Cnt"]) < refThreshold:
            continue
        if len(clusters[int(id)]["Tree"].relations) > 1:
            tmpDict.update({id: (cidList.count(id), len(clusters[int(id)]["Tree"].relations), int(id))})
    cntLimit = int(getMvCntLimit())
    candDict = {k: v for k, v in tmpDict.items() if v[0] >= refThreshold and v[1] < 4}
    # print(candDict)
    count = len(candDict)
    if count < cntLimit:
        cntLimit = count
    # 随机选择N个
    if method == "random":
        tmpList = random.sample([int(k) for k, v in candDict.items()], k=cntLimit)
        output = tmpList
        print(output)
        return candDict, output
    else:
        # 根据配置文件，获取引用次数最多的N个
        # tmpList = sorted(candDict.items(), key=lambda x: x[1])[-cntLimit:]
        tmpList = sorted(candDict.items(), key=lambda x: (x[1][0],-x[1][1], -x[1][2]))
        cntDict = defaultdict(list)
        # 按引用次数排序之后，从中选择覆盖更多表的候选视图
        for x in reversed(tmpList):
            cntDict[x[1]].append(int(x[0]))
        print(cntDict)

        coveredRel = set()
        coveredQuery = set()
        output = []
        # 先尽可能多的覆盖表

        # 一些无法处理的sql
        can_not_run_file = ["5a", "5b", "5c", "3c"]
        # 再尽可能多的覆盖原始查询
        for ids in cntDict.values():
            for mv_id in ids:
                if mv_id in output:
                    continue
                newQuerySet = set(mv_q_map[mv_id])
                # 处理一些特殊的sql
                flag = False
                for run_file in can_not_run_file:
                    if run_file in newQuerySet:
                        flag = True
                        break
                if flag:
                    continue


                if newQuerySet.issubset(coveredQuery):
                    continue
                output.append(mv_id)
                coveredQuery = coveredQuery.union(set(mv_q_map[mv_id]))
            if len(output) >= cntLimit or len(coveredQuery) == len(q_mv_maps):
                break

        for ids in cntDict.values():
            for mv_id in ids:
                if mv_id in output:
                    continue
                newRelSet = set(clusters[mv_id]["Tree"].relations.keys())
                if newRelSet.issubset(coveredRel):
                    continue
                output.append(mv_id)
                coveredRel = coveredRel.union(newRelSet)
                coveredQuery = coveredQuery.union(set(mv_q_map[mv_id]))
                # 如果提前满额或者cover完所有的表之后，暂时退出，去覆盖查询
            if len(output) >= cntLimit or len(coveredRel) == len(g_table.data):
                break

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

        print(output)
        return candDict, output[:cntLimit]


def getInitMVs_projection(clusters, method="topk"):
    # 将其他元素也一并加入数组
    all_table_list = []
    for id, node in enumerate(clusters):
        id = int(id)
        if id not in all_table_list:
            all_table_list.append(id)
    # 给所有的id赋值
    all_table_dict = {}
    for i in range(len(all_table_list)):
        all_table_dict[all_table_list[i]] = len(all_table_list) - i
    with open("./resources/data/clusters_data.csv", "w", newline="") as f:
        csv_writer = csv.writer(f, dialect="excel")
        for id, node in enumerate(clusters):
            id = int(id)
            csv_writer.writerow([id, all_table_dict[id]])



def getReferredAttrs(planTree):
    attrs = ""
    for rel, attr in planTree.referKeys.items():
        attrs += rel + "." + (", " + rel + ".").join(attr) + ", "
    return attrs


# 去除相等的属性
def removeEqualAttr(attrList, whereSql, planTree):
    tmpAttrs = attrList
    tmpSet = set(tmpAttrs)
    newAttrs = list(tmpSet)
    procAttrs = []
    for attr in tmpSet:
        # attr = attr.strip()
        if whereSql.find("(" + attr + " = ") != -1 or whereSql.find("(" + attr.split(".")[1] + " = ") != -1:
            startPos = whereSql.find(attr + " = ")
            if startPos != -1:
                startPos = whereSql.find(attr + " = ") + len(attr + " = ")
            else:
                startPos = whereSql.find("(" + attr.split(".")[1] + " = ") + len("(" + attr.split(".")[1] + " = ")
            endPos = whereSql.find(")", startPos)
            equalAttr = whereSql[startPos:endPos]
            # 是属性，不是常量，但是还没有处理过
            if equalAttr in tmpAttrs:
                if equalAttr not in procAttrs:
                    # 还没有处理过就被删掉了，说明有相等属性在最终列表中。本属性也可以删除
                    if equalAttr not in newAttrs:
                        if attr in newAttrs:
                            newAttrs.remove(attr)
                    else:
                        # 还没有处理过也还没有被删，说明当前属性就是他的第一个相等属性。可以将equal删除，保留当前属性
                        newAttrs.remove(equalAttr)
            else:
                curTable = getTableFromItem(equalAttr, planTree.relations.keys(), g_table.data)
                if curTable is not None:
                    equalAttr = curTable + "." + equalAttr
                    if equalAttr not in procAttrs:
                        if equalAttr not in newAttrs:
                            if attr in newAttrs:
                                newAttrs.remove(attr)
                        else:
                            newAttrs.remove(equalAttr)
        if whereSql.find(" = " + attr + ")") != -1 or whereSql.find(" = " + attr.split(".")[1] + ")") != -1:
            endPos = whereSql.find(" = " + attr)
            if endPos == -1:
                endPos = whereSql.find(" = " + attr.split(".")[1] + ")")
            startPos = whereSql[:endPos].rfind("(") + 1
            equalAttr = whereSql[startPos:endPos]
            # 是属性，不是常量
            # 是属性，不是常量，但是还没有处理过
            if equalAttr in tmpAttrs:
                if equalAttr not in procAttrs:
                    # 还没有处理过就被删掉了，说明有相等属性在最终列表中。本属性也可以删除
                    if equalAttr not in newAttrs:
                        if attr in newAttrs:
                            newAttrs.remove(attr)
                    else:
                        # 还没有处理过也还没有被删，说明当前属性就是他的第一个相等属性。可以将equal删除，保留当前属性
                        newAttrs.remove(equalAttr)
            else:
                curTable = getTableFromItem(equalAttr, planTree.relations.keys(), g_table.data)
                if curTable is not None:
                    equalAttr = curTable + "." + equalAttr
                    if equalAttr not in procAttrs:
                        if equalAttr not in newAttrs:
                            if attr in newAttrs:
                                newAttrs.remove(attr)
                        else:
                            newAttrs.remove(equalAttr)
        procAttrs.append(attr)
    # print("======================================")
    # print("=======where:{}".format(whereSql[:-5]))
    # print("=======before:{}, \n=======after :{}".format(tmpSet, newAttrs))
    # print("======================================")
    return newAttrs


def remove_equal_attr_PG(attrList, whereSql, planTree):
    # pattern = r"([\w]+\.[\w]+)[ ]*\=[ ]*([\w]+\.[\w]+)"
    # for condition in conditionList:
    #     key = re.findall(pattern, condition)
    #     if len(key) == 0:
    #         continue
    #     l_k, r_k = key[0][0], key[0][1]
    tmpAttrs = attrList
    tmpSet = set(tmpAttrs)
    newAttrs = list(tmpSet)
    newAttrs = sorted(newAttrs, key=lambda x: x)
    procAttrs = []
    for attr in newAttrs:
        # attr = attr.strip()
        if whereSql.find("(" + attr + " = ") != -1 or whereSql.find("(" + attr.split(".")[1] + " = ") != -1:
            startPos = whereSql.find(attr + " = ")
            if startPos != -1:
                startPos = whereSql.find(attr + " = ") + len(attr + " = ")
            else:
                startPos = whereSql.find("(" + attr.split(".")[1] + " = ") + len("(" + attr.split(".")[1] + " = ")
            endPos = whereSql.find(")", startPos)
            equalAttr = whereSql[startPos:endPos]
            # 是属性，不是常量，但是还没有处理过
            if equalAttr in tmpAttrs:
                if equalAttr not in procAttrs:
                    # 还没有处理过就被删掉了，说明有相等属性在最终列表中。本属性也可以删除
                    if equalAttr not in newAttrs:
                        if attr in newAttrs:
                            newAttrs.remove(attr)
                    else:
                        # 还没有处理过也还没有被删，说明当前属性就是他的第一个相等属性。可以将equal删除，保留当前属性
                        newAttrs.remove(equalAttr)
            else:
                curTable = getTableFromItem(equalAttr, planTree.relations.keys(), g_table.data)
                if curTable is not None:
                    equalAttr = curTable + "." + equalAttr
                    if equalAttr not in procAttrs:
                        if equalAttr not in newAttrs:
                            if attr in newAttrs:
                                newAttrs.remove(attr)
                        else:
                            newAttrs.remove(equalAttr)
        if whereSql.find(" = " + attr + ")") != -1 or whereSql.find(" = " + attr.split(".")[1] + ")") != -1:
            endPos = whereSql.find(" = " + attr)
            if endPos == -1:
                endPos = whereSql.find(" = " + attr.split(".")[1] + ")")
            startPos = whereSql[:endPos].rfind("(") + 1
            equalAttr = whereSql[startPos:endPos]
            # 是属性，不是常量
            # 是属性，不是常量，但是还没有处理过
            if equalAttr in tmpAttrs:
                if equalAttr not in procAttrs:
                    # 还没有处理过就被删掉了，说明有相等属性在最终列表中。本属性也可以删除
                    if equalAttr not in newAttrs:
                        if attr in newAttrs:
                            newAttrs.remove(attr)
                    else:
                        # 还没有处理过也还没有被删，说明当前属性就是他的第一个相等属性。可以将equal删除，保留当前属性
                        newAttrs.remove(equalAttr)
            else:
                curTable = getTableFromItem(equalAttr, planTree.relations.keys(), g_table.data)
                if curTable is not None:
                    equalAttr = curTable + "." + equalAttr
                    if equalAttr not in procAttrs:
                        if equalAttr not in newAttrs:
                            if attr in newAttrs:
                                newAttrs.remove(attr)
                        else:
                            newAttrs.remove(equalAttr)
        procAttrs.append(attr)
    # print("======================================")
    # print("=======where:{}".format(whereSql[:-5]))
    # print("=======before:{}, \n=======after :{}".format(tmpSet, newAttrs))
    # print("======================================")
    return newAttrs


def isAttrCntOver(table, attrs, tableSchema):
    # print("table:{}, attrs:{}".format(table, attrs))
    totalCnt = len(tableSchema.get(table.upper(), ""))
    if totalCnt == 0:
        return True
    cntLimit = int(totalCnt * (float)(getColThreshold()))
    attrCnt = 0
    if len(attrs) == 1 and attrs[0] == "root":
        return True
    for attr in attrs:
        if attr.upper() in tableSchema[table.upper()]:
            attrCnt = attrCnt + 1
            if attrCnt > cntLimit:
                return True
    return False


# 如果出现相同的列，那么通过取别名的方式来避免冲突，如果不是相同的列，取本身的别名
def check_Referred(newAttrs):
    attr_dict = {}
    for attr in newAttrs:
        if attr.find(".") != -1:
            attr = attr.split(".")[1]
            if attr not in attr_dict:
                attr_dict[attr] = 1
            else:
                attr_dict[attr] += 1
    for index, attr in enumerate(newAttrs):
        if attr.find(".") != -1:
            l, r = attr.split(".")[0:2]
            if r in attr_dict and attr_dict[r] > 1:
                newAttrs[index] = attr + " AS " + l + "_" + r + f"_{index}"
            else:
                newAttrs[index] = attr + " AS " + r

    return newAttrs



# 从join子树中获取候选视图
def getJoinCandidate(candidate_clusters, mv_pre_path, initIDS, cur_engine):
    cid = None
    createPre = "create materialized view if not exists mv"
    if cur_engine == 'PG':
        createENGINE = " as "
    elif cur_engine == 'CH':
        # createENGINE = " engine = ReplicatedMergeTree() order by tuple() as "
        createENGINE = " engine = MergeTree() order by tuple() POPULATE as \n"
    index = 0
    for cid, cluster in candidate_clusters.items():
        filename = "mv" + str(cid) + ".sql"
        jsonName = "mv" + str(cid) + ".json"
        dsName = "mv" + str(cid) + ".ds"

        if len(cluster["Tree"].relations) == 1:
            continue
        if cluster["Tree"].relations == {}:
            continue
        with open(mv_pre_path + "/json/" + jsonName, "w", encoding="utf-8") as f:
            if cur_engine == 'CH':
                jsonPlan = cluster["Tree"].plan
            elif cur_engine == 'PG':
                jsonPlan = cluster["Tree"].data
            jsonPlan = {"Plan": jsonPlan}
            json.dump(jsonPlan, f, indent=2)
        with open(mv_pre_path + "/ds/" + dsName, "wb") as f:
            pickle.dump(cluster, f)
        # # 如果引用计数小于配置的阈值，不处理这个子树
        # if str(cid) not in idWithCnt:
        #     continue
        attrs = ""
        fromSql = " from "
        whereSql = " where "
        conditionList = []
        for rel, colCondition in cluster["Tree"].relations.items():
            fromSql += rel + ","
            for col, conditions in colCondition.items():
                if col == "variableNames":
                    continue
                # print("col:{}, condition:{}".format(col, conditions))
                conditions = sorted(conditions, key=lambda x: x)
                for condition in conditions:
                    attrs += rel + "." + col + ", "
                    patten = re.compile('\w+Of\w+')
                    colOfTables = patten.findall(condition)
                    for colOfTable in list(set(colOfTables)):
                        column, table = colOfTable.split("Of")
                        if table != rel and table not in cluster["Tree"].relations.keys():
                            break
                        # condition = condition.replace(colOfTable, table+"."+column)
                        condition = re.sub(r"\b" + colOfTable + r"\b", table + "." + column, condition)
                    # print("condition:{}, resetCondition:{}".format(condition,resetCondition2Sql(condition)))
                    # print("condition:{}".format(condition))
                    # if len(colOfTables) == 0:
                    # condition = re.sub(r"\b"+col+r"\b", rel + "." + col, condition)
                    condition = resetCondition2Sql(condition)
                    cons = []
                    for c in condition.split("And"):
                        if whereSql.find(c.strip().strip("(").strip(")")) != -1:
                            continue
                        cons.append(c)
                    condition = " And ".join(cons)

                    if condition != '':
                        # if cid == 28:
                        #     print(condition)
                        whereSql += condition + " And "
                        conditionList.append(condition.replace("==", "=").replace("\"", "'"))
        whereSql = whereSql.replace("==", "=").replace("\"", "'")

        fromSql = fromSql[:-1]
        # fromSql = sortRelations(fromSql[6:-1])
        # if cluster["Tree"].data.type == NodeType.EXPRESSION:
        #     attrs = cluster["Tree"].data.attribute
        attrs = getReferredAttrs(cluster["Tree"])

        attrType = type(attrs)
        if attrType == str:
            attrs = attrs[:-2]
            tmpAttrs = attrs.split(",")
        elif attrType == list:
            attrs = str(attrs)[1:-2].replace("'", "")
            tmpAttrs = attrs
        tmpAttrs = [a.strip() for a in tmpAttrs]
        newAttrs = remove_equal_attr_PG(tmpAttrs, whereSql, cluster["Tree"])
        newAttrs = check_Referred(newAttrs)
        attrs = str(newAttrs)[1:-2].replace("'", "")
        if " where " == whereSql:
            sql = "select " + attrs + "\n" + fromSql
        else:
            sql = "select " + attrs + "\n" + fromSql + "\n" + whereSql[:-5]
        with open(mv_pre_path + "/mv/" + filename, "w", encoding="utf-8") as f:
            f.write(createPre + str(cid) + createENGINE + sql)
        with open(mv_pre_path + "/mv_original/" + filename, "w", encoding="utf-8") as f:
            f.write(sql)
        # id_mv
        with open("resources/data/id_mv.csv", "a+", newline="") as f:
            csv_writer = csv.writer(f, dialect="excel")
            csv_writer.writerow([cid, sql, createPre + str(cid) + createENGINE + sql])
        # 初始列表中的才进行创建
        if cid in initIDS:
            with open(mv_pre_path + "/topmv/" + filename, "w", encoding="utf-8") as f:
                f.write(createPre + str(cid) + createENGINE + sql)
            with open(mv_pre_path + "/topmv_original/" + filename, "w", encoding="utf-8") as f:
                f.write(sql)
        index += 1
    return index


# 从join子树中获取候选视图
def getJoinCandidate_CH(candidate_clusters, mv_pre_path, initIDS, cur_engine):
    cid = None
    createPre = "create materialized view if not exists mv"
    if cur_engine == 'PG':
        createENGINE = " as "
    elif cur_engine == 'CH':
        # createENGINE = " engine = ReplicatedMergeTree() order by tuple() as "
        createENGINE = " engine = MergeTree() order by tuple() POPULATE as \n"
    index = 0
    for cid, cluster in candidate_clusters.items():
        filename = "mv" + str(cid) + ".sql"
        jsonName = "mv" + str(cid) + ".json"
        dsName = "mv" + str(cid) + ".ds"


        if len(cluster["Tree"].relations) == 1:
            continue
        if cluster["Tree"].relations == {}:
            continue
        with open(mv_pre_path + "/json/" + jsonName, "w", encoding="utf-8") as f:
            if cur_engine == 'CH':
                jsonPlan = cluster["Tree"].plan
            elif cur_engine == 'PG':
                jsonPlan = cluster["Tree"].data
            jsonPlan = {"Plan": jsonPlan}
            json.dump(jsonPlan, f, indent=2)
        with open(mv_pre_path + "/ds/" + dsName, "wb") as f:
            pickle.dump(cluster, f)
        # # 如果引用计数小于配置的阈值，不处理这个子树
        # if str(cid) not in idWithCnt:
        #     continue
        conditionList = []
        attrs = ""
        fromSql = " from "
        whereSql = " where "
        for rel, colCondition in cluster["Tree"].relations.items():
            # fromSql += rel + ","
            for col, conditions in colCondition.items():
                if col == "variableNames":
                    continue
                # print("col:{}, condition:{}".format(col, conditions))
                for condition in conditions:
                    attrs += rel + "." + col + ", "
                    patten = re.compile('\w+Of\w+')
                    colOfTables = patten.findall(condition)
                    for colOfTable in list(set(colOfTables)):
                        column, table = colOfTable.split("Of")
                        if table != rel and table not in cluster["Tree"].relations.keys():
                            break
                        # condition = condition.replace(colOfTable, table+"."+column)
                        condition = re.sub(r"\b" + colOfTable + r"\b", table + "." + column, condition)
                    # print("condition:{}, resetCondition:{}".format(condition,resetCondition2Sql(condition)))
                    # print("condition:{}".format(condition))
                    # if len(colOfTables) == 0:
                    # condition = re.sub(r"\b"+col+r"\b", rel + "." + col, condition)
                    condition = resetCondition2Sql(condition)
                    cons = []
                    for c in condition.split("And"):
                        if whereSql.find(c.strip().strip("(").strip(")")) != -1:
                            continue
                        cons.append(c)
                    condition = " And ".join(cons)

                    if condition != '':
                        # if cid == 28:
                        #     print(condition)
                        whereSql += condition + " And "
                        conditionList.append(condition.replace("==", "=").replace("\"", "'"))
        whereSql = whereSql.replace("==", "=").replace("\"", "'")

        new_where_sql = " where "
        con_dict = {}
        for index, condition in enumerate(conditionList):
            pattern = r"([\w]+\.[\w]+)[ ]*=[ ]*\'[\w]+\'[ ]*And[ ]*([\w]+\.[\w]+)[ ]*=[ ]*\'[\w]+\'"
            key_lst = re.findall(pattern, condition)
            if len(key_lst) > 0:
                if key_lst[0][0] == key_lst[0][1]:
                    del conditionList[index]

        # 新的where
        for condition in conditionList:
            new_where_sql += condition + " And "


        for rel in cluster["Tree"].referKeys:
            fromSql += rel + ","
        fromSql = fromSql[:-1]
        # fromSql = sortRelations(fromSql[6:-1])
        # if cluster["Tree"].data.type == NodeType.EXPRESSION:
        #     attrs = cluster["Tree"].data.attribute
        attrs = getReferredAttrs(cluster["Tree"])

        attrType = type(attrs)
        if attrType == str:
            attrs = attrs[:-2]
            tmpAttrs = attrs.split(",")
        elif attrType == list:
            attrs = str(attrs)[1:-2].replace("'", "")
            tmpAttrs = attrs
        tmpAttrs = [a.strip() for a in tmpAttrs]
        newAttrs = removeEqualAttr(tmpAttrs, new_where_sql, cluster["Tree"])
        newAttrs = check_Referred(newAttrs)
        attrs = str(newAttrs)[1:-2].replace("'", "")
        if " where " == new_where_sql:
            sql = "select " + attrs + "\n" + fromSql
        else:
            sql = "select " + attrs + "\n" + fromSql + "\n" + new_where_sql[:-5]
        with open(mv_pre_path + "/mv/" + filename, "w", encoding="utf-8") as f:
            f.write(createPre + str(cid) + createENGINE + sql)
        with open(mv_pre_path + "/mv_original/" + filename, "w", encoding="utf-8") as f:
            f.write(sql)
        # id_mv
        with open("resources/data/id_mv.csv", "a+", newline="") as f:
            csv_writer = csv.writer(f, dialect="excel")
            csv_writer.writerow([cid, sql, createPre + str(cid) + createENGINE + sql])
        # 初始列表中的才进行创建
        if cid in initIDS:
            with open(mv_pre_path + "/topmv/" + filename, "w", encoding="utf-8") as f:
                f.write(createPre + str(cid) + createENGINE + sql)
            with open(mv_pre_path + "/topmv_original/" + filename, "w", encoding="utf-8") as f:
                f.write(sql)
        index += 1
    return index



class Projection_Node:
    def __init__(self, projection_keys, group_keys, table, alias_tables):
        self.projection_keys = projection_keys
        self.group_keys = group_keys
        self.table = table
        self.alias_tables = alias_tables
        self.key_asname = {}

    def __str__(self):
        return f"projection_keys:{str(self.projection_keys)}\n" + f"group_keys:{str(self.group_keys)}\n" + f"table:{self.table}"

    def is_empty(self):
        return self.projection_keys == [] or self.group_keys == []


# 先找到单表对应的那张表
def get_projection_table(sqlFile, p_node):
    key, ext = os.path.splitext(os.path.basename(sqlFile))
    if ext != ".sql":
        return None
    with open(sqlFile, "r", encoding='utf-8') as f:
        sql = f.read()
        sqlStandard = normalizeSql(sql)
        parser = sqlparser.MViewParser(sqlStandard)
        if len(parser.tables) != 1:
            return None
        # 获取table
        table = parser.tables[0]
        alias_tables = []
        for alias_table, original_table in parser.tables_aliases.items():
            if original_table == table:
                alias_tables.append(alias_table)
        # 解析列
        f_as = False
        cur_value = ""
        bracket_num = 0
        f_group = 0
        last_value = ""
        func_list = ["multiIf", "toDate", "sum", "toYear"]
        remove_list = []
        f_not_as = False
        cur_token_value = ""
        cur_token_value_dict = {}
        for sql_token in parser.tokens:
            token_value = sql_token.value
            if token_value.upper() == "SELECT":
                continue
            if token_value.upper() == "FROM":
                f_group = 1
                continue
            if token_value.upper() == "GROUP BY":
                f_group = 2
                continue
            if token_value.upper() == "ORDER BY":
                break
            if token_value.upper() == "WHERE":
                f_not_as = True
                continue
            if token_value.upper() == "AS":
                f_as = True
                continue
            if token_value.upper() == "LIMIT":
                break
            # 去掉别名
            for alias in alias_tables:
                if token_value.find(alias + ".") != -1:
                    token_value = token_value.replace(alias + ".", "")

            if token_value == "(":
                bracket_num += 1
                cur_value += token_value
                last_value = cur_value
                continue
            if token_value == ")":
                bracket_num -= 1
                cur_value += token_value
                last_value = cur_value
                if bracket_num == 0:
                    if token_value != '':
                        if f_group == 0:
                            p_node.projection_keys.append(cur_value)
                        elif f_group == 2:
                            p_node.group_keys.append(cur_value)
                    cur_value = ""
                continue
            if f_as:
                if last_value != table:
                    p_node.key_asname[last_value] = token_value
                    p_node.projection_keys[-1] = last_value + " AS " + token_value
                    remove_list.append(token_value)
                f_as = False
                cur_value = ""
                continue

            if token_value in func_list:
                cur_token_value = token_value
                cur_value += token_value
                last_value = cur_value
                continue
            if bracket_num == 0:
                if token_value != "," and token_value != '':
                    if f_group == 0:
                        p_node.projection_keys.append(token_value)
                    elif f_group == 2:
                        p_node.group_keys.append(token_value)
                last_value = token_value
            else:
                t_field = ""
                if table.find(".") != -1:
                    tables = table.split(".")[-1]
                else:
                    tables = table
                for field in g_table.data[tables.upper()].keys():
                    field = field.strip('`')
                    if token_value.find(field) != -1:
                        t_field = field
                        break
                if t_field != "":
                    if cur_token_value not in cur_token_value_dict:
                        cur_token_value_dict[cur_token_value] = [token_value]
                    else:
                        cur_token_value_dict[cur_token_value].append(token_value)
                if token_value.upper() == "IN":
                    token_value = " " + token_value + " "
                cur_value += token_value
                last_value = cur_value


        columns_aliass = {}
        for k, v in parser.columns_aliases.items():
            if type(v) == str:
                columns_aliass[k] = v.split(".")[-1]
        if 'where' in parser.columns_aliases_dict:
            for projection in parser.columns_aliases_dict['where']:
                if projection in columns_aliass:
                    projection = columns_aliass[projection]
                if projection not in p_node.projection_keys:
                    p_node.projection_keys.append(projection)
                if projection not in p_node.group_keys:
                    p_node.group_keys.append(projection)

        # 解析where里面的列
        if table.find(".") != -1:
            table = table.split(".")[-1]
        f_start = False
        for sql_token in parser.tokens:
            token_value = sql_token.value
            if token_value.upper() == "WHERE":
                f_start = True
                continue
            if token_value.upper() == "GROUP BY":
                break
            if not f_start:
                continue
            # 去掉别名
            for alias in alias_tables:
                if token_value.find(alias + ".") != -1:
                    token_value = token_value.replace(alias + ".", "")
            # 看schema里是否存在列
            for field in g_table.data[table.upper()].keys():
                # if field in p_node.projection_keys:
                #     continue
                field = field.strip('`')
                if token_value.find(field) != -1:
                    if field not in p_node.projection_keys:
                        p_node.projection_keys.append(field)
                    if field not in p_node.group_keys:
                        p_node.group_keys.append(field)

        # 去掉一些不必要的——multiIf
        check_list = ["multiIf"]
        p_node.projection_keys = list(set(p_node.projection_keys))
        p_node.group_keys = list(set(p_node.group_keys))
        for key in p_node.projection_keys:
            if key in remove_list:
                p_node.projection_keys.remove(key)
            for check in check_list:
                if key.find(check) != -1:
                    p_node.projection_keys.remove(key)
                    for field in cur_token_value_dict[check]:
                        if key.find(field) != -1:
                            if field not in p_node.projection_keys:
                                p_node.projection_keys.append(field)
                            if field not in p_node.group_keys:
                                p_node.group_keys.append(field)

                    break
        for key in p_node.group_keys:
            if key in remove_list and key != "year":
                p_node.group_keys.remove(key)
            for check in check_list:
                if key.find(check) != -1:
                    p_node.group_keys.remove(key)
                    for field in cur_token_value_dict[check]:
                        if key.find(field) != -1:
                            if field not in p_node.projection_keys:
                                p_node.projection_keys.append(field.lower())
                            if field not in p_node.group_keys:
                                p_node.group_keys.append(field.lower())
                    break
        p_node.projection_keys = list(set(p_node.projection_keys))
        p_node.group_keys = list(set(p_node.group_keys))

        # 删除聚合函数里的列，如果出现在列中
        aggr = []
        for key in p_node.projection_keys:
            if key.find("toDate") != -1:
                aggr.append(key.split("toDate")[1].split(" AS ")[0].strip("(").strip(")"))
        for agg in aggr:
            for key in p_node.projection_keys:
                if key.upper() == agg.upper():
                    p_node.projection_keys.remove(key)
            for key in p_node.group_keys:
                if key.upper() == agg.upper():
                    p_node.group_keys.remove(key)
        p_node.projection_keys = list(set(p_node.projection_keys))
        p_node.group_keys = list(set(p_node.group_keys))

        # 打补丁：删除toYear里的列
        remove_year_list = []
        for field in p_node.projection_keys:
            pattern = r"toYear\(([\w]+)\)"
            key = re.findall (pattern, field)
            if len(key) > 0:
                remove_year_list.append(key[0])
        for field in p_node.projection_keys:
            if field in remove_year_list:
                p_node.projection_keys.remove(field)
        for field in p_node.group_keys:
            if field in remove_year_list:
                p_node.group_keys.remove(field)
        p_node.projection_keys = list(set(p_node.projection_keys))
        p_node.group_keys = list(set(p_node.group_keys))

        if f_group != 2:
            return None
        return table






# 解决单表的问题
def get_projection(candidate_clusters, parentPath, cur_engine, mv_query_map):
    if cur_engine != 'CH':
        return
    sqlPath = parentPath + "/sql/"
    jsonPath = parentPath + "/json/"
    pipelinePath = parentPath + "/pipeline/"
    fileNames = os.listdir(sqlPath)
    try:
        fileNames = sorted(fileNames, key=lambda x: int(x[1:-4]))
    except:
        pass
    group_num = 0
    for file in fileNames:
        sqlFile = sqlPath + "/" + file
        key, ext = os.path.splitext(os.path.basename(sqlFile))
        if ext != ".sql":
            continue
        pipelineFile = pipelinePath + "/" + key + ".pipeline"
        jsonFile = jsonPath + "/" + key + ".json"
        # sql语句对应的执行计划也需要存在
        if not os.path.exists(jsonFile):
            continue
        # 如果是clickhouse，pipeline文件也需要存在
        if cur_engine == 'CH' and not os.path.exists(pipelineFile):
            continue

        print("--------------------------------------")
        print("fileName:{}".format(file))
        new_node = Projection_Node([], [], "", [])
        # 先找到单表对应的那张表
        table = get_projection_table(sqlFile, new_node)
        if table is None:
            print("{} has not group by or multiply table!".format(file))
            group_num += 1
            continue
        new_node.table = table
        # 解析出字段
        # projection_keys, group_keys = build_projection_tree_CH(jsonFile, sqlFile)
        # 先做简单的合并
        flag = True
        # for key in group_keys:
        #     if key not in new_node.group_keys:
        #         new_node.group_keys.append(key)


        print(new_node)
        print("--------------------------------------")

        for i, old_node in enumerate(candidate_clusters):
            n_p_s = set()
            o_p_s = set()
            n_g_s = set()
            o_g_s = set()
            for key_value in new_node.projection_keys:
                if key_value.find(" AS ") != -1:
                    key_value = key_value.split(" AS ")[0].strip()
                n_p_s.add(key_value)
            for key_value in old_node.projection_keys:
                if key_value.find(" AS ") != -1:
                    key_value = key_value.split(" AS ")[0].strip()
                o_p_s.add(key_value)
            for key_value in new_node.group_keys:
                if key_value.find(" AS ") != -1:
                    key_value = key_value.split(" AS ")[0].strip()
                n_g_s.add(key_value)
            for key_value in old_node.group_keys:
                if key_value.find(" AS ") != -1:
                    key_value = key_value.split(" AS ")[0].strip()
                o_g_s.add(key_value)

            if n_p_s == o_p_s and n_g_s == o_g_s:
                flag = False
                mv_query_map[i].append(key)
                break
            # 子集情况
            # if n_p_s.issubset(o_p_s) and n_g_s.issubset(o_g_s):
            #     mv_query_map[i].append(key)
            #     break
            # if o_p_s.issubset(n_p_s) and o_g_s.issubset(n_g_s):
            #     candidate_clusters[i].projection_keys = new_node.projection_keys
            #     candidate_clusters[i].group_keys = new_node.group_keys
            #     mv_query_map[i].append(key)
            #     break
            if (n_p_s.issubset(o_p_s) or o_p_s.issubset(n_p_s)) \
                and (n_g_s.issubset(o_g_s) or o_g_s.issubset(n_g_s)):
                flag = False
                candidate_clusters[i].projection_keys = new_node.projection_keys if len(new_node.projection_keys) > len(old_node.projection_keys) else old_node.projection_keys
                candidate_clusters[i].group_keys = new_node.group_keys if len(new_node.group_keys) > len(old_node.group_keys) else old_node.group_keys
                mv_query_map[i].append(key)
                break

        if flag and not new_node.is_empty():
            mv_query_map[len(candidate_clusters)] = [key]
            candidate_clusters.append(new_node)

    print(f"group_num:{group_num}")



# 生成单表的视图
def save_projection_mv(candidate_clusters, cur_engine, cid, mv_pre_path, initIDS):
    if cid is None:
        cid = 0
    if cur_engine != 'CH':
        return
    createCubePre = "alter table "
    createCubeInter = " add projection cube"
    id_table_cube_dict = {}
    projection_table_dict = {} # id-table
    for i, node in enumerate(candidate_clusters):
        # select
        select_sql = "SELECT\n"
        for projection in node.projection_keys:
            select_sql += f"    {projection},\n"
        select_sql = select_sql[:-2]
        # from
        from_sql = "\n" + f"FROM {node.table}\n"
        # group by
        group_sql = "GROUP BY\n"
        for key in node.group_keys:
            group_sql += f"    {key},\n"
        group_sql = group_sql[:-2] + ";"
        original_sql = select_sql + from_sql + group_sql
        create_sql = createCubePre + node.table + createCubeInter + str(cid) + "(" + select_sql + "\n" + group_sql[:-1] + ")"
        filename = "mv" + str(cid) + ".sql"
        id_table_cube_dict[cid] = (node.table, f"cube{cid}")
        # 写入文件
        with open(mv_pre_path + "/mv_original/" + filename, "w", encoding="utf-8") as f:
            f.write(original_sql)
        with open(mv_pre_path + "/mv/" + filename, "w", encoding="utf-8") as f:
            f.write(create_sql)
        with open(mv_pre_path + "/topmv_original/" + filename, "w", encoding="utf-8") as f:
            f.write(original_sql)
        with open(mv_pre_path + "/topmv/" + filename, "w", encoding="utf-8") as f:
            f.write(create_sql)
        # id_mv
        with open("resources/data/id_mv.csv", "a+", newline="") as f:
            csv_writer = csv.writer(f, dialect="excel")
            csv_writer.writerow([cid, original_sql, create_sql])
        initIDS.append(cid)
        projection_table_dict[cid] = node.table
        cid = cid + 1

    with open("resources/data/id_table_cube_dict.ds", "wb") as f:
        pickle.dump(id_table_cube_dict, f)
    with open("resources/data/projection_table_dict.ds", "wb") as f:
        pickle.dump(projection_table_dict, f)

# 获取单表候选视图
def getProjectCandidate(q_pre_path, mv_pre_path, cid, cur_engine, initIDS):
    if cid is None:
        cid = 0
    else:
        cid = cid + 1
    if cur_engine == 'CH':
        createCubePre = "alter table "
        createCubeInter = " add projection cube"
        createCubeApd = "type aggregate;"
    elif cur_engine == 'PG':
        createMaterPre = "CREATE MATERIALIZED VIEW IF NOT EXISTS mv"
    cubeDict, condDict = getFMax(q_pre_path, g_table.data)
    tableList = getProjectionList(g_table.data)
    id_table_cube_dict = {}
    for table, attrs in cubeDict.items():
        if len(tableList) != 0 and table.upper() not in tableList:
            continue
        if isAttrCntOver(table, attrs, g_table.data):
            continue
        print("table:{}, attrs:{}".format(table, attrs))

        # 没有聚合字段，明细数据没有必要创建cube
        if str(attrs).find("(") == -1:
            continue
        replacedSep = "'"
        grpAttr = []
        for attr in attrs:
            if attr.find("'") != -1:
                replacedSep = "\""
            if attr.find("(") == -1:
                grpAttr.append(attr)
        for attr in condDict[table]:
            grpAttr.append(attr)
        common_sql = "select " + str(attrs)[1:-2].replace(replacedSep, "") + "," + str(condDict[table])[1:-2].replace(
            "\'", "")
        if len(grpAttr) != 0:
            grpby_sql = " group by " + str(grpAttr)[1:-2].replace("\'", "")
        else:
            grpby_sql = ""
        filename = "mv" + str(cid) + ".sql"
        # key: mv的id(int) value: (table(str), cube(str))
        id_table_cube_dict[cid] = (table, f"cube{cid}")
        # sql/create_sql
        sql = common_sql + "\n" + " from " + table + "\n" + grpby_sql
        if cur_engine == 'CH':
            create_sql = createCubePre + table + createCubeInter + str(cid) + "(" + common_sql + grpby_sql + ")"
        else:
            create_sql = createMaterPre + str(cid) + " AS " + common_sql + " from " + table + grpby_sql + ";"
        with open(mv_pre_path + "/mv_original/" + filename, "w", encoding="utf-8") as f:
            f.write(sql)
        with open(mv_pre_path + "/mv/" + filename, "w", encoding="utf-8") as f:
            f.write(create_sql)
        with open(mv_pre_path + "/topmv_original/" + filename, "w", encoding="utf-8") as f:
            f.write(sql)
        with open(mv_pre_path + "/topmv/" + filename, "w", encoding="utf-8") as f:
            f.write(create_sql)
        # id_mv
        with open("resources/data/id_mv.csv", "a+", newline="") as f:
            csv_writer = csv.writer(f, dialect="excel")
            csv_writer.writerow([cid, sql, create_sql])
        initIDS.append(cid)
        cid = cid + 1

    with open("resources/data/id_table_cube_dict.ds", "wb") as f:
        pickle.dump(id_table_cube_dict, f)


def buildOldCandidate(mvPath):
    clusters = {}
    dsPath = mvPath + "/ds/"
    fileNames = os.listdir(dsPath)
    # maxCid = 0
    for file in fileNames:
        dsFile = dsPath + "/" + file
        key, ext = os.path.splitext(os.path.basename(dsFile))
        if ext != ".ds":
            continue
        cid = int(key[2:])
        print("load last mview file:{}".format(dsFile))
        try:
            with open(dsFile, "rb") as f:
                cluster = pickle.load(f)
                clusters[int(cid)] = cluster
                # if cid in (3, 136):
                # print("cid:{}--{}".format(cid, cluster["Tree"].relations))
        except EOFError:
            pass
    # print("before sorted{}".format(clusters))
    # 将数组按id排序
    clusters = sorted(clusters.items())
    clusters = {x[0]: x[1] for x in clusters}
    # print(clusters)
    # print("after sorted{}".format(clusters))
    return clusters


def remove_directory_file(path):
    try:
        shutil.rmtree(path)
        os.mkdir(path)
    except OSError as e:
        print("error: {0}; path: {1}".format(path, e.strerror))

# 检查文件夹是否存在
def check_file_exists(engine="PG"):
    if engine == "PG":
        mv_path = getRawPath(DataType.MV)
        os.makedirs(mv_path, exist_ok=True)
        os.makedirs(mv_path + "/ds", exist_ok=True)
        os.makedirs(mv_path + "/json", exist_ok=True)
        os.makedirs(mv_path + "/mv", exist_ok=True)
        os.makedirs(mv_path + "/mv_original", exist_ok=True)
        os.makedirs(mv_path + "/topmv", exist_ok=True)
        os.makedirs(mv_path + "/topmv_original", exist_ok=True)
        qmv_path = getRawPath(DataType.Q_MV)
        os.makedirs(qmv_path, exist_ok=True)
        os.makedirs(qmv_path + "/json", exist_ok=True)
        # 清空数据
        remove_directory_file(mv_path + "/mv")
        remove_directory_file(mv_path + "/mv_original")
        remove_directory_file(mv_path + "/topmv")
        remove_directory_file(mv_path + "/topmv_original")
        # 清空csv文件
        with open("resources/data/id_mv.csv", "w") as f:
            pass
    elif engine == "CH":
        mv_path = getRawPath(DataType.MV)
        os.makedirs(mv_path, exist_ok=True)
        os.makedirs(mv_path + "/ds", exist_ok=True)
        os.makedirs(mv_path + "/json", exist_ok=True)
        os.makedirs(mv_path + "/mv", exist_ok=True)
        os.makedirs(mv_path + "/mv_original", exist_ok=True)
        os.makedirs(mv_path + "/topmv", exist_ok=True)
        os.makedirs(mv_path + "/topmv_original", exist_ok=True)
        qmv_path = getRawPath(DataType.Q_MV)
        os.makedirs(qmv_path, exist_ok=True)
        os.makedirs(qmv_path + "/json", exist_ok=True)
        os.makedirs(qmv_path + "/pipeline", exist_ok=True)
        # 清空数据
        remove_directory_file(mv_path + "/mv")
        remove_directory_file(mv_path + "/mv_original")
        remove_directory_file(mv_path + "/topmv")
        remove_directory_file(mv_path + "/topmv_original")
        # 清空csv文件
        with open("resources/data/id_mv.csv", "w") as f:
            pass


def getCandidate(engine="PG"):
    solver = Solver()
    prePath = getRawPath(DataType.Q)
    if prePath is None:
        print("Please check the config file")
        exit(1)

    mvPrePath = getRawPath(DataType.MV)
    if mvPrePath is None:
        print("Please check the config file")
        exit(1)
    # 处理文件路径
    check_file_exists(engine)

    # 根据旧的候选视图生成
    candidate_clusters = {}
    # candidate_clusters = buildOldCandidate(mvPrePath)
    # 生成新的候选视图
    buildPlanTrees(candidate_clusters, prePath, cur_engine=engine)
    if engine == "PG":
        idWithCnt, initIDS = getInitMVs(g_query_subs, candidate_clusters, method="topk")
    else:
        idWithCnt, initIDS = getInitMVs_CH(g_query_subs, candidate_clusters, method="topk")
    saveTmpQueryMap(g_query_subs, initIDS)
    # 多表的处理，生成视图
    if engine == "PG":
        cid = getJoinCandidate(candidate_clusters, mvPrePath, initIDS, engine)
    else:
        cid = getJoinCandidate_CH(candidate_clusters, mvPrePath, initIDS, engine)
    # 将单表的处理追加在后面
    if engine == "CH" and get_use_projection() == "True":
        new_clusters = []
        mv_query_map = {}
        get_projection(new_clusters, prePath, engine, mv_query_map)
        getInitMVs_projection(new_clusters, method="topk")
        with open("./resources/data/mv_query_map.csv", "w") as f:
            f_csv = csv.writer(f)
            for mv, querys in mv_query_map.items():
                f_csv.writerow([mv, querys])
        save_projection_mv(new_clusters, engine, cid, mvPrePath, initIDS)
    # 将topn写入文件
    with open("./resources/data/topn_mv_id.ds", "wb") as f:
        pickle.dump(initIDS, f)


def load_file(file_path):
    f = open(file_path)
    content = f.read()
    f.close()
    return content


def ch_connect(sql_path, use_projection=0, database=getDatabase()):
    # user = "root"
    # host = "43.139.64.58"
    # database = "cbg"
    # pw = "Kanleni1"
    # client = Client(host=host, port=9000, database=database)
    # sql = 'SHOW TABLES'
    # res = client.execute(sql)
    # print(res)
    if use_projection == 0 or use_projection == 1:
        tmp = f"set allow_experimental_projection_optimization = {use_projection};"
    else:
        tmp = ""
    sql_content = tmp + load_file(sql_path)
    sh_query = f"clickhouse client " \
               f"-h {get_CH_IP()} " \
               f"-u {get_CH_USER()} " \
               f"--password {get_CH_PASSWD()} " \
               f"--send_logs_level=trace " \
               f"--port 9000 " \
               f"--database {database} " \
               f"-t " \
               f"--multiquery --query \"{sql_content}\""
    print(sh_query)
    retcode, output = subprocess.getstatusoutput(sh_query)
    re_query_id = r' ] {(.*?)} <'
    # print(output)
    try:
        query_id = re.findall(re_query_id, output)[-1]
    except:
        query_id = ""
    print(query_id)
    if retcode == 0:
        print(f"{sql_path} success!")
        return True, query_id
    else:
        print(f"{sql_path} fail!")
        return False, query_id


def ch_connect_sql(sql_content, database=getDatabase()):
    # user = "root"
    # host = "43.139.64.58"
    # database = "cbg"
    # pw = "Kanleni1"
    # client = Client(host=host, port=9000, database=database)
    # sql = 'SHOW TABLES'
    # res = client.execute(sql)
    # print(res)

    sh_query = f"clickhouse client " \
               f"-h {get_CH_IP()} " \
               f"-u {get_CH_USER()} " \
               f"--password {get_CH_PASSWD()} " \
               f"--port 9000 " \
               f"--database {database} " \
               f"-t " \
               f"--multiquery --query \"{sql_content}\""
    print(sh_query)
    retcode, output = subprocess.getstatusoutput(sh_query)
    print(output)
    try:
        time = float(output.split("\n")[-1])
    except:
        print(f"fail")
        print(output)
        return None, output
    if retcode == 0:
        print(f"success:{time}")
        return time, output
    else:
        print(f"fail:{time}")
        return None, output



def run_ch_all(flag, query_id_dict):
    for root, dirs, files in os.walk(getRawPath(DataType.Q) + "/sql"):
        try:
            files = sorted(files, key=lambda x:x[1:-4])
        except:
            pass
        if flag == 0:
            with open("ch_test.csv", "w") as f:
                pass
        for file in files:
            print(f"file:{file}")
            sql_path = os.path.join(root, file)
            ret, query_id = ch_connect(sql_path, flag)
            if not ret:
                print(f"[run_ch_all] {file} error!")
            if file not in query_id_dict:
                query_id_dict[file] = [query_id]
            else:
                query_id_dict[file].append(query_id)
            # with open("ch_test.csv", "a+") as f:
            #     f_csv = csv.writer(f)
            #     f_csv.writerow([sql_path, res])


def run_ch_query_id():
    query_id_dict = {}
    run_ch_all(0, query_id_dict)
    run_ch_all(1, query_id_dict)
    # 延时，直到生成信息
    time.sleep(20)
    print("------------------------------------------------")
    query_id_cost_dict = {}
    for key, q_l in  query_id_dict.items():
        q_1, q_2 = q_l[0], q_l[1]
        print(key)
        out1, out2 = get_query_log_by_query_id_2(q_1), get_query_log_by_query_id_2(q_2)
        cost1, cost2 = get_cost_by_query_log(out1, 0), get_cost_by_query_log(out2, 0)
        t1, t2 = float(out1[0]), float(out2[0])
        query_id_cost_dict[key.split(".")[0]] = (cost1, cost2, t1, t2)
    with open("./resources/data/query_id_cost_dict.ds", "wb") as f:
        pickle.dump(query_id_cost_dict, f)


def build_projection():
    # query_id_cost_dict
    run_ch_query_id()
    # projection_q_mv_dict
    run_explain_all()


# 删除projection
def remove_projection():
    # 将删除视图的list写入文件
    try:
        with open("resources/data/drop_projection_list.ds", "rb") as f:
            drop_materialized_pro_list = pickle.load(f)
    except:
        print("get remove file error!")
        return
    # 删除projection
    for drop_materialized_pro in drop_materialized_pro_list:
        print(drop_materialized_pro)
        ret, output = ch_connect_sql(drop_materialized_pro)
        if not ret:
            print(f"{drop_materialized_pro} fail!")


def run_ch_list(run_nums):
    query_path = getRawPath(DataType.Q) + "/sql/"
    for num in run_nums:
        sql_path = query_path + f"q{num}.sql"
        ret1, res_before = ch_connect(sql_path, 0)
        if not ret1:
            print(f"[run_ch_list] {num} before fail!")
        ret2, res_after = ch_connect(sql_path, 1)
        if not ret2:
            print(f"[run_ch_list] {num} after fail!")
        print(f"before: {res_before}, after: {res_after}")




# explain看是否被改写
def run_explain_all():
    projection_q_mv_dict = {}
    ch_connect_sql("set allow_experimental_projection_optimization = 1")
    for root, dirs, files in os.walk(getRawPath(DataType.Q) + "/sql"):
        try:
            files = sorted(files, key=lambda x:x[1:-4])
        except:
            pass
        for file in files:
            print(f"file:{file}")
            sql_path = os.path.join(root, file)
            sql_content = "explain " + load_file(sql_path)
            res, output = ch_connect_sql(sql_content)
            # 找到是否被改写的信息
            pattern = r"cube([0-9]+)"
            key = re.findall(pattern, output)
            if len(key) > 0:
                id = key[0]
                cube_name = f"mv{id}"
                projection_q_mv_dict[file.split(".")[0]] = cube_name
    with open("./resources/data/projection_q_mv_dict", "wb") as f:
        pickle.dump(projection_q_mv_dict, f)
    print(f"topn的覆盖率: {len(projection_q_mv_dict)}")
                

def run_recommend_explain_all():
    projection_q_mv_dict = {}
    num = 0
    ch_connect_sql("set allow_experimental_projection_optimization = 1")
    for root, dirs, files in os.walk(getRawPath(DataType.Q) + "/sql"):
        try:
            files = sorted(files, key=lambda x:x[1:-4])
        except:
            pass
        for file in files:
            print(f"file:{file}")
            sql_path = os.path.join(root, file)
            sql_content = "explain " + load_file(sql_path)
            res, output = ch_connect_sql(sql_content)
            # 找到是否被改写的信息
            pattern = r"cube([0-9]+)"
            key = re.findall(pattern, output)
            if len(key) > 0:
                num += 1
    print(f"\n覆盖率： {num}")




def create_projection():
    with open("resources/data/projection_table_dict.ds", "rb") as f:
        projection_table_dict = pickle.load(f)
    materialized_pro_list = []
    drop_materialized_pro_list = []
    for root, dirs, files in os.walk(getRawPath(DataType.Q) + "/mv/topmv"):
        try:
            files = sorted(files, key=lambda x:x[2:-4])
        except:
            pass
        for file in files:
            print("--------------------")
            print(f"file:{file}")

            sql_path = os.path.join(root, file)
            ret, res = ch_connect(sql_path, 2)
            id = int(file.split(".")[0][2:])
            table_name = projection_table_dict[id]
            projection_name = "cube" + file[2:-4]
            materialized_pro = f"alter table {table_name} MATERIALIZE PROJECTION {projection_name}"
            drop_materialized_pro = f"ALTER TABLE {table_name} DROP PROJECTION {projection_name}"
            if ret:
                materialized_pro_list.append(materialized_pro)
                drop_materialized_pro_list.append(drop_materialized_pro)
                # ret, output = ch_connect_sql(materialized_pro)
                # if not ret:
                #     print(f"MATERIALIZE PROJECTION {projection_name} fail!")
            else:
                print(f"create projection {projection_name} fail")
            print("--------------------")

    # 延时一段时间，让它创建成功
    time.sleep(100)
    for materialized_pro in materialized_pro_list:
        ret, output = ch_connect_sql(materialized_pro)
        if not ret:
            print(f"MATERIALIZE PROJECTION {projection_name} fail!")
        time.sleep(1)
    # 将删除视图的list写入文件
    with open("resources/data/drop_projection_list.ds", "wb") as f:
        pickle.dump(drop_materialized_pro_list, f)


def create_recommend_projection():
    with open("resources/data/projection_table_dict.ds", "rb") as f:
        projection_table_dict = pickle.load(f)
    for root, dirs, files in os.walk(getRawPath(DataType.Q) + "/mv/recommend/mv"):
        try:
            files = sorted(files, key=lambda x:x[2:-4])
        except:
            pass
        for file in files:
            print("--------------------")
            print(f"file:{file}")

            sql_path = os.path.join(root, file)
            ret, res = ch_connect(sql_path, 2)
            id = int(file.split(".")[0][2:])
            table_name = projection_table_dict[id]
            projection_name = "cube" + file[2:-4]
            materialized_pro = f"alter table {table_name} MATERIALIZE PROJECTION {projection_name}"
            if ret:
                ret, output = ch_connect_sql(materialized_pro)
                if not ret:
                    print(f"MATERIALIZE PROJECTION {projection_name} fail!")
            else:
                print(f"create projection {projection_name} fail")
            print("--------------------")


# 计算成本
def run_cmd(args_list):
    """
    run linux commands
    """
    # print('Running system command: {0}'.format(' '.join(args_list)))
    proc = subprocess.Popen(args_list, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    s_output, s_err = proc.communicate()
    s_return = proc.returncode
    s_output = s_output.decode('utf-8').split('\n')
    return s_return, s_output, s_err


# def get_query_log_by_query_id(query_id):
#     args_list = ["clickhouse client", "-h", get_CH_IP(), "-u", get_CH_USER(), "--password", get_CH_PASSWD(),"--port", "9000", "--query", f"\"select query_duration_ms, read_rows, read_bytes, result_rows, result_bytes, memory_usage from system.query_log where query_id ='{query_id}';\""]
#     (ret, out, err) = run_cmd(args_list)
#     if ret == 0:
#         info = out[1].split('\t')
#     else:
#         info = None
#     return info
#
#
# def get_query_log_by_query_id(query_id):
#     args_list = ["clickhouse client", "-h", get_CH_IP(), "-u", get_CH_USER(), "--password", get_CH_PASSWD(), "--port", "9000", "--query", f"\"select query_duration_ms, read_rows, read_bytes, result_rows, result_bytes, memory_usage from system.query_log where query_id ='{query_id}';\""]
#     (ret, out, err) = run_cmd(args_list)
#     if ret == 0:
#         info = out[1].split('\t')
#     else:
#         info = None
#     return info


def get_query_log_by_query_id_2(query_id):
    sql_content = f"select query_duration_ms, read_rows, read_bytes, result_rows, result_bytes, memory_usage from system.query_log where query_id ='{query_id}';"
    sh_query = f"clickhouse client " \
               f"-h {get_CH_IP()} " \
               f"-u {get_CH_USER()} " \
               f"--password {get_CH_PASSWD()} " \
               f"--port 9000 " \
               f"-t " \
               f"--multiquery --query \"{sql_content}\""
    retcode, output = subprocess.getstatusoutput(sh_query)
    print(output)
    s_output = output.split('\n')
    info = s_output[1].split('\t')
    return info


def get_cost_by_query_log(ql, operator):
    read_bytes = ql[2]
    read_rows = ql[1]
    memory_usage = ql[5]
    cpu_cost = float(read_bytes) / 4096 + float(read_rows) * 0.01 + float(operator) * 0.0025
    cost = cpu_cost * 0.1 + float(memory_usage) * 0.001
    return cost



if __name__ == '__main__':
    # global g_candidate_clusters
    # global g_query_subs
    # print(sys.getrecursionlimit())
    # sys.setrecursionlimit(100000)
    # print(sys.getrecursionlimit())

    # 解析
    getCandidate(get_engine())

    # 跑第一次
    # run_ch_all(0)

    # 跑第二次
    # run_ch_all(1)

    # 跑所有的两次
    # run_ch_query_id()

    # 看结果
    # with open("./resources/data/query_id_cost_dict.ds", "rb") as f:
    #     query_id_cost_dict = pickle.load(f)
    # print(query_id_cost_dict)

    # 跑指定的sql两次
    # run_nums_cube1 = [38, 7]
    # run_nums_cube4 = [10, 16, 19, 36, 43]
    # run_nums_cube0 = [1, 15, 21, 28, 35]
    # run_ch_list(run_nums_cube0)

    # 创建视图
    # create_projection()

    # 跑看是否改写

    # run_explain_all()
    # with open("./resources/data/projection_q_mv_dict", "rb") as f:
    #     projection_q_mv_dict = pickle.load(f)
    # print(projection_q_mv_dict)


    # 获取信息

    # print('Running system command: {0}'.format(sh_query))

    # query_path = getRawPath(DataType.Q) + "/sql/"
    # sql_path = query_path + f"q1.sql"
    # sql_content = load_file(sql_path)
    # database = "cbg"
    # # 跑sql
    # sh_query = f"clickhouse-client " \
    #            f"--send_logs_level=trace " \
    #            f"--port 9000 " \
    #            f"--database {database} " \
    #            f"-t " \
    #            f"--multiquery --query \"{sql_content}\""
    # retcode, output = subprocess.getstatusoutput(sh_query)
    # re_query_id = r' ] {(.*?)} <'
    # query_id = re.findall(re_query_id, output)[-1]
    # print(query_id)
    #
    #
    # print(get_query_log_by_query_id(query_id))

    # 获取推荐的视图
    # create_recommend_projection()

    # 获取覆盖率
    # run_recommend_explain_all()