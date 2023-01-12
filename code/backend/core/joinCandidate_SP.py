import subprocess
import time

from parsePlanJson import remove_invalid_tokens
from procConf import *
import os.path
from io import open
import re
from parseDotFile_SP import *
from parsePlanFile_SP import *
from parseNodeFile_SP import *
from procSQL import *
import csv
import random
from z3 import *
from procCondition_CH import *
import pickle
from get_physical_plan import *
from view_selection import *
import os
import shutil


def remove_directory_file(path):
    try:
        shutil.rmtree(path)
        os.mkdir(path)
    except OSError as e:
        print("error: {0}; path: {1}".format(path, e.strerror))


# 打印sql
def printMVsql(clusters):
    for cludId, node in enumerate(clusters):
        # 判断哪几个表出现了多次
        '''
        tableCntDict = {} # 记录每个表出现的次数
        tableAliasSet = set()
        for i, table in enumerate(node.filterNode.tableList):
            tableCntDict[table] = tableCntDict[table] + 1
            if tableCntDict[table] >= 2:
                tableAliasSet.add(table)
        '''
        selectSql = "SELECT\n"
        for i, field in enumerate(node.filterNode.fieldList):
            if i != 0:
                selectSql = selectSql + ",\n"
            selectSql = selectSql + "\t" + field
        fromSql = "\nFROM\n"
        for i, table in enumerate(node.filterNode.tableList):
            if i != 0:
                fromSql = fromSql + ",\n"
            fromSql = fromSql + "\t" + table
        whereSql = "\nWHERE\n   "
        orderbyStr = ""
        limitStr = ""
        groupBySet = set()
        for i, filter in enumerate(node.filterNode.filterList):
            if filter.find("limit") != -1:
                limitStr = "\n\t" + filter
                continue
            if filter.find("order by") != -1:
                orderbyStr = "\n\t" + filter
                continue
            if filter.find("group by") != -1:
                tmpList = filter.split("group by")[1].strip().split(",")
                for tmp in tmpList:
                    groupBySet.add(tmp.strip())
                continue
            if i != 0:
                whereSql = whereSql + "\n\tAND"
            whereSql = whereSql + " " + filter
        # 生成group by
        if len(groupBySet) == 0:
            groupbyStr = ""
        else:
            groupbyStr = "\n\t" + "group by"
        for groubyStr in list(groupBySet):
            groupbyStr = groupbyStr + " " + groubyStr + ","
        sql = selectSql + fromSql + whereSql + orderbyStr + groupbyStr[:-1] + limitStr
        print(cludId)
        print(sql)


# 输出sql到文件
def writeMVsql(mvPath, clusters):
    id = 0
    for cludId, node in clusters.items():
        # 判断哪几个表出现了多次
        '''
        tableCntDict = {} # 记录每个表出现的次数
        tableAliasSet = set()
        for i, table in enumerate(node.filterNode.tableList):
            tableCntDict[table] = tableCntDict[table] + 1
            if tableCntDict[table] >= 2:
                tableAliasSet.add(table)
        '''
        # 错误信息监测
        if node.filterNode.fieldList == [] or node.filterNode.tableList == [] or node.filterNode.filterList == []:
            continue
        # 前缀
        # aggrSql = "use tpcds_bin_orc_5;\n"
        aggrSql = "use imdb_orc;\n"
        aggrSql = aggrSql + "set spark.sql.orc.impl=hive;\n"

        selectSql = "SELECT\n"
        for i, field in enumerate(node.filterNode.fieldList):
            if i != 0:
                selectSql = selectSql + ",\n"
            selectSql = selectSql + "\t" + field
        fromSql = "\nFROM\n"
        for i, table in enumerate(node.filterNode.tableList):
            if i != 0:
                fromSql = fromSql + ",\n"
            fromSql = fromSql + "\t" + table
        whereSql = "\nWHERE\n   "
        orderbyStr = ""
        limitStr = ""
        groupBySet = set()
        groupByFlag = False
        for i, filter in enumerate(node.filterNode.filterList):
            if filter.find("limit") != -1:
                limitStr = "\n\t" + filter
                continue
            if filter.find("order by") != -1:
                orderbyStr = "\n\t" + filter
                continue
            if filter.find("group by") != -1:
                groupByFlag = True
                tmpList = filter.split("group by")[1].strip().split(",")
                for tmp in tmpList:
                    groupBySet.add(tmp.strip())
                continue
            if i != 0:
                whereSql = whereSql + "\n\tAND"
            whereSql = whereSql + " " + filter
            groupBySet.add(filter)
        # 生成group by
        if groupByFlag:
            if len(groupBySet) == 0:
                groupbyStr = ""
            else:
                groupbyStr = "\n\t" + "group by"
            for groubyStr in list(groupBySet):
                groupbyStr = groupbyStr + " " + groubyStr + ","
        else:
            groupbyStr = ""
        sql = aggrSql + selectSql + fromSql + whereSql + orderbyStr + groupbyStr[:-1] + limitStr
        # 输出sql
        filename = "mv" + str(id) + ".sql"
        with open(mvPath + "/sql/" + filename, "w", encoding="utf-8") as f:
            f.write(sql)

        id = id + 1


# 将已知的sql提取出字段、表、过滤条件
# 在tpc-ds数据集上有些问题
def parseSQLFromExist(mvPath):
    sqlList = []
    mvSqlPath = mvPath + "/sql/"
    fileNames = os.listdir(mvSqlPath)
    for file in fileNames:
        fieldList = []
        tableList = []
        filterList = []
        flag1 = False
        flag2 = False
        with open(mvSqlPath + file, encoding="utf-8") as f:
            while True:
                line = f.readline()
                if not line:
                    break

                line = line.strip().strip("AND").strip().strip(",").strip()
                # select
                if line == "SELECT":
                    continue
                if line == "FROM":
                    flag1 = True
                    continue
                elif line == "WHERE":
                    flag2 = True
                    continue

                if not flag1:
                    fieldList.append(line)
                    continue
                if not flag2:
                    tableList.append(line)
                    continue
                # 暂不对比group by/order by/limit，可拆解对比
                if line.find("group by") != -1 or line.find("order by") != -1 or line.find("limit") != -1:
                    continue
                filterList.append(line)

        node = FilterNode_SP(fieldList, tableList, filterList, [])
        sqlList.append(node)

    return sqlList


# 将node的信息转化为sql语句
def transformNodeToSql(filterNode):
    selectSql = "SELECT\n"
    groupBySet = set()
    for field in filterNode.fieldList:
        selectSql += "\t" + field + ",\n"
        if field.find("(") == -1:
            groupBySet.add(field)
    if len(filterNode.fieldList) > 0:
        selectSql = selectSql[:-2]

    fromSql = "\nFROM\n"
    for table in filterNode.tableList:
        fromSql += "\t" + table + ",\n"
    if len(filterNode.tableList) > 0:
        fromSql = fromSql[:-2]

    whereSql = "\nWHERE\n\t"
    orderbyStr = ""
    limitStr = ""
    limitFlag = False
    orderbyFlag = False
    groupByFlag = False
    filterList = list(set(filterNode.joinFilterList) | set(filterNode.commonFilterList))

    for filter in filterList:
        if filter.find("limit") != -1:
            limitFlag = True
            limitStr = "\n\t" + filter
            continue
        if filter.find("order by") != -1:
            orderbyFlag = True
            orderbyStr = "\n\t" + filter
            continue
        if filter.find("group by") != -1 or filter.find("group b") != -1:
            groupByFlag = True
            continue
        whereSql += filter + "\n\tAND "
    if len(filterList) > 0:
        whereSql = whereSql[:-6]

    # 生成group by
    groupbyStr = ""
    if groupByFlag and len(list(groupBySet)) > 0:
        groupbyStr = "\n\t" + "group by"
        for groubyStr in list(groupBySet):
            groupbyStr += " " + groubyStr + ","
        groupbyStr = groupbyStr[:-1]

    sql = selectSql + fromSql + whereSql
    if groupByFlag:
        sql += groupbyStr
    if orderbyFlag:
        sql += orderbyStr
    if limitFlag:
        sql += limitStr

    return sql


def join_outer_check_equal(condition, tableSet):
    if tableSet == set():
        return True
    pattern = r"([\w.]+)[ ]*=[ ]*([\w.]+)"
    key = re.findall(pattern, condition)
    if len(key) == 0:
        return True
    table1, table2 = key[0][0], key[0][1]
    if table1 in tableSet and table2 in tableSet:
        return False

    return True


def remove_equal_join_condition(joinFilterList):
    key_all_set = []
    new_joinFilterList = []
    for key in joinFilterList:
        key_set = set()
        k1, k2 = key.split(" = ")[0:2]
        key_set.add(k1)
        key_set.add(k2)
        if key_set not in key_all_set:
            new_joinFilterList.append(key)
            key_all_set.append(key_set)
    return new_joinFilterList


# 将filterNode和relations结合生成sql语句
def generate_node_relations_sql(node, cludId):
    # 生成带group by的sql
    if node.isGroupBy:
        select_sql = "SELECT\n"
        for key in node.group_by_list:
            select_sql += "\t" + key + ",\n"
        agg_list = []
        for agg in node.agg_list:
            if agg.find("#") == -1:
                agg_list.append(agg)
        for i, agg in enumerate(agg_list):
            select_sql += "\t" + agg + f" AS AGG{i}" + ",\n"
        select_sql = select_sql[:-2]

        from_sql = "\nFROM\n"
        for table in node.filterNode.tableList:
            from_sql += "\t" + table + ",\n"
        from_sql = from_sql[:-2]
        where_sql = "\nWHERE\n"
        joinFilterList = remove_equal_join_condition(node.filterNode.joinFilterList)
        for key in joinFilterList:
            where_sql += "\t" + key + "\n\tAND"
        where_sql = where_sql[:-4]
        group_sql = "GROUP BY\n"
        for key in node.group_by_list:
            group_sql += "\t" + key + ",\n"
        group_sql = group_sql[:-2]
        # last sql
        sql = select_sql + from_sql + where_sql + group_sql
        create_sql = f"CREATE MATERIALIZED VIEW IF NOT EXISTS mv{cludId}\n" + "AS\n" + sql + ";"
        return sql, create_sql

    # 2.from
    fromSql = "\nFROM\n"
    # 先处理left/right join的情况
    flag = False
    tableSet = set()
    fieldSet = set()
    on_filter_list = []
    rightTable = ""
    join_type = ""
    if len(node.filterNode.outerJoinFilterList) > 0:
        if len(node.filterNode.outerJoinFilterList) == 1:
            fromSql += "\t" + node.filterNode.outerJoinFilterList[0][4] + " " + \
                       node.filterNode.outerJoinFilterList[0][3] + " " + node.filterNode.outerJoinFilterList[0][5] + \
                       " ON (" + node.filterNode.outerJoinFilterList[0][0] + ")"
            tableSet.add(node.filterNode.outerJoinFilterList[0][4])
            tableSet.add(node.filterNode.outerJoinFilterList[0][5])
            fieldSet.add(node.filterNode.outerJoinFilterList[0][1])
            fieldSet.add(node.filterNode.outerJoinFilterList[0][2])
            on_filter_list.append(node.filterNode.outerJoinFilterList[0][0])
            rightTable = node.filterNode.outerJoinFilterList[0][5]
            join_type = node.filterNode.outerJoinFilterList[0][3]
            flag = True
        elif len(node.filterNode.outerJoinFilterList) == 2:
            if node.filterNode.outerJoinFilterList[0][4] == node.filterNode.outerJoinFilterList[1][4] and node.filterNode.outerJoinFilterList[0][5] == node.filterNode.outerJoinFilterList[1][5]:
                fromSql += "\t" + node.filterNode.outerJoinFilterList[0][4] + " " + \
                           node.filterNode.outerJoinFilterList[0][3] + " " + node.filterNode.outerJoinFilterList[0][5] + \
                           " ON (" + node.filterNode.outerJoinFilterList[0][0] + " AND " + node.filterNode.outerJoinFilterList[1][0] + ")"
                tableSet.add(node.filterNode.outerJoinFilterList[0][4])
                tableSet.add(node.filterNode.outerJoinFilterList[0][5])
                fieldSet.add(node.filterNode.outerJoinFilterList[0][1])
                fieldSet.add(node.filterNode.outerJoinFilterList[0][2])
                on_filter_list.append(node.filterNode.outerJoinFilterList[0][0])
                on_filter_list.append(node.filterNode.outerJoinFilterList[1][0])
                rightTable = node.filterNode.outerJoinFilterList[0][5]
                join_type = node.filterNode.outerJoinFilterList[0][3]
                flag = True
    if flag:
        table_list = list(set(node.filterNode.tableList) - tableSet)
        for table in table_list:
            fromSql += "\t" + "Join " + table + "\n"
        if len(table_list) > 0:
            fromSql = fromSql[:-1]
    else:
        for table in node.filterNode.tableList:
            fromSql += "\t" + table + ",\n"
        if len(node.filterNode.tableList) > 0:
            fromSql = fromSql[:-2]

    # 1.select
    selectSql = "SELECT\n"
    groupBySet = set()
    for field in node.filterNode.fieldList:
        if flag and (join_type == "SEMI JOIN" or join_type == "ANTI JOIN") and field.find(rightTable) != -1:
            continue
        selectSql += "\t" + field + ",\n"
        if field.find("(") == -1:
            groupBySet.add(field)
    if len(node.filterNode.fieldList) > 0:
        selectSql = selectSql[:-2]

    # 3.where
    whereSql = "\nWHERE\n\t"
    orderbyStr = ""
    limitStr = ""
    limitFlag = False
    orderbyFlag = False
    groupByFlag = False
    conditionSet = set()
    for rel, colCondition in node.relations.items():
        for col,conditions in colCondition.items():
            if col == "variableNames":
                continue
            for condition in conditions:
                if condition.find("limit") != -1:
                    limitFlag = True
                    limitStr = "\n\t" + condition
                    continue
                if condition.find("order by") != -1:
                    orderbyFlag = True
                    orderbyStr = "\n\t" + condition
                    continue
                pattern = r"\w+Of\w+"
                colOfTables = re.findall(pattern, condition)
                for colOfTable in list(set(colOfTables)):
                    column, table = colOfTable.split("Of")
                    condition = re.sub(r"\b" + colOfTable + r"\b", table + "." + column, condition)
                # 将condition从z3适应的结构切换回来
                condition = reset_condition_z3(condition)
                # 将带日期格式的转化回来
                condition = transform_date_format(condition)
                # 处理括号匹配问题
                if not check_bracket_equal(condition):
                    continue
                if condition not in conditionSet and condition not in on_filter_list and join_outer_check_equal(condition, fieldSet):
                    whereSql += condition + "\n\tAND "
                    conditionSet.add(condition)
    if len(conditionSet) > 0:
        whereSql = whereSql[:-6]
    else:
        whereSql = whereSql[:-8]

    # 4.生成group by
    groupbyStr = ""
    if groupByFlag and len(list(groupBySet)) > 0:
        groupbyStr = "\n\t" + "group by"
        for groubyStr in list(groupBySet):
            groupbyStr += " " + groubyStr + ","
        groupbyStr = groupbyStr[:-1]

    # 5.查看表是否有分区字段
    partitioned_by = ""
    distribute_by = ""
    p_key = []
    for rel, colCondition in node.relations.items():
        rel = rel.upper()
        if rel not in g_table.data:
            continue
        if "infoOfPartition" in g_table.data[rel]:
            for keyList in g_table.data[rel]["infoOfPartition"]["key"]:
                for key in keyList:
                    if key not in p_key:
                        p_key.append(key.lower())
    if len(p_key) == 1:
        partitioned_by = "PARTITIONED BY (" + p_key[0] + ")"
        distribute_by = "DISTRIBUTE BY " + p_key[0]

    # 带上create
    create_sql = f"CREATE MATERIALIZED VIEW IF NOT EXISTS mv{cludId}\n"
    if partitioned_by != "" and distribute_by != "":
        create_sql += partitioned_by + "\n" + "AS\n"
    else:
        create_sql += "AS\n"

    sql = selectSql + fromSql + whereSql
    if node.isGroupBy:
        sql += groupbyStr
    if orderbyFlag:
        sql += orderbyStr
    if limitFlag:
        sql += limitStr

    create_sql += sql
    if partitioned_by != "" and distribute_by != "":
        create_sql += "\n" + distribute_by + ";"
    else:
        create_sql += ";"

    return sql, create_sql




# 和原本文件夹里存在的sql对比
def CheckAndDeleteEqualSql(mvPath, clusters, cntDict, sqlList):
    # 写入sql
    id = len(sqlList)
    writeNum = 0
    unWriteNum = 0
    cnt = 0
    writeCntDict = {}
    writeClusters = {}
    for cludId, node in clusters.items():
        # # 得到完整的sql语句
        # sql = transformNodeToSql(node.filterNode)

        # 生成结点来对比
        tmpFilterNode = FilterNode_SP(node.filterNode.fieldList, node.filterNode.tableList, [], [])
        for filter in node.filterNode.joinFilterList:
            tmpFilterNode.joinFilterList.append(filter)
        for filter in node.filterNode.commonFilterList:
            if filter.find("group by") != -1 or filter.find("group b") != -1 or filter.find("order by") != -1 or filter.find("limit") != -1:
                continue
            tmpFilterNode.joinFilterList.append(filter)

        # 判断文件夹下是否已经存在相同的sql，否则写入文件中
        if not EqualSqlDetection(sqlList, tmpFilterNode):
            # 写入文件
            # filename = "mv" + str(id) + ".sql"
            # with open(mvPath + "/sql/" + filename, "w", encoding="utf-8") as f:
            #     f.write(sql)
            writeCntDict[cnt] = cntDict[cnt]
            writeClusters[cnt] = node
            id += 1
            writeNum += 1
        else:
            unWriteNum += 1
        cnt += 1

    print("write: {0}, unWrite: {1}".format(writeNum, unWriteNum))
    return writeCntDict, writeClusters


# 生成一条sql并写入文件
def WriteOneSql(id, mvPath, sql):
    # 写入文件
    filename = "mv" + str(id) + ".sql"
    with open(mvPath + filename, "w", encoding="utf-8") as f:
        f.write(sql)


'''
生成物化视图
'''
def generate_all_mvs(clusters, offset, mvPath, initIDS, realMVPath):
    mv_path = getRawPath(DataType.MV)
    # 删除原有的所有sql
    # mv mv_original topmv topmv_original
    mv_path_mv = mv_path + "/mv/"
    mv_path_mv_original = mv_path + "/mv_original/"
    mv_path_topmv = mv_path + "/topmv/"
    mv_path_topmv_original = mv_path + "/topmv_original/"
    remove_directory_file(mv_path_mv)
    remove_directory_file(mv_path_mv_original)
    remove_directory_file(mv_path_topmv)
    remove_directory_file(mv_path_topmv_original)
    with open("resources/data/id_mv.csv", "w", newline="") as f:
        csv_writer = csv.writer(f, dialect="excel")
        for cludId, node in clusters.items():
            # 得到sql语句
            sql, create_sql = generate_node_relations_sql(node, cludId)
            WriteOneSql(cludId + offset, mv_path_mv, create_sql)
            WriteOneSql(cludId + offset, mv_path_mv_original, sql)
            if cludId in initIDS:
                WriteOneSql(cludId + offset, mv_path_topmv, create_sql)
                WriteOneSql(cludId + offset, mv_path_topmv_original, sql)
            csv_writer.writerow([cludId, sql, create_sql])





'''
检查路径文件夹是否存在
'''
def check_file_path_exists(url="resources"):
    # spark
    spark_path = url + "/spark"
    mv_path = spark_path + "/mv"
    os.makedirs(mv_path, exist_ok=True)
    # spark-mv
    os.makedirs(mv_path + "/mv", exist_ok=True)
    os.makedirs(mv_path + "/mv_original", exist_ok=True)
    os.makedirs(mv_path + "/topmv", exist_ok=True)
    os.makedirs(mv_path + "/topmv_original", exist_ok=True)
    os.makedirs(mv_path + "/ds", exist_ok=True)
    os.makedirs(mv_path + "/log", exist_ok=True)
    os.makedirs(mv_path + "/recommend", exist_ok=True)
    # data
    os.makedirs(url + "/data", exist_ok=True)
    # 检查文件
    with open(getCandidateQMVMappings(), "w") as f:
        pass



# 根据一个clusters生成一系列sql
def WriteMVSql(clusters, offset, mvPath):
    #  删除文件夹下的所有文件
    remove_directory_file(mvPath)
    if type(clusters) == dict:
        for cludId, node in clusters.items():
            # 得到sql语句
            sql, create_sql = generate_node_relations_sql(node, cludId)
            WriteOneSql(cludId + offset, mvPath, sql)
    else:
        for cludId, node in enumerate(clusters):
            # 得到sql语句
            sql, create_sql = generate_node_relations_sql(node, cludId)
            WriteOneSql(cludId + offset, mvPath, sql)


# 生成真正的物化视图
def WriteRealMVSql(clusters, offset, initIDS, mvPath):
    #  删除文件夹下的所有文件
    remove_directory_file(mvPath + "/topMV-original/")
    remove_directory_file(mvPath + "/topMV/")
    for cludId, node in clusters.items():
        if cludId in initIDS:
            # 得到sql语句
            sql, create_sql = generate_node_relations_sql(node)
            WriteOneSql(cludId + offset, mvPath + "/topMV-original/", sql)
            sql = "CREATE MATERIALIZED VIEW IF NOT EXISTS mv{} AS\n".format(cludId) + sql
            WriteOneSql(cludId + offset, mvPath + "/topMV/", sql)


# 测试单个sql的文件输出
def WriteOneMVsql_SP(mvPath, clusters):
    # 1.先读取原来的所有sql
    sqlList = parseSQLFromExist(mvPath)
    sqlList = []

    # 2.写sql
    id = len(sqlList)
    writeNum = 0
    unWriteNum = 0
    cnt = 0
    writeCntDict = {}
    for cludId, node in enumerate(clusters):
        # 得到完整的sql语句
        sql = transformNodeToSql(node.filterNode)

        # 判断文件夹下是否已经存在相同的sql，否则写入文件中
        tmpFilterNode = FilterNode_SP(node.filterNode.fieldList, node.filterNode.tableList, [], [])
        for filter in node.filterNode.joinFilterList:
            tmpFilterNode.joinFilterList.append(filter)
        for filter in node.filterNode.commonFilterList:
            if filter.find("group by") != -1 or filter.find("order by") != -1 or filter.find("limit") != -1:
                continue
            tmpFilterNode.joinFilterList.append(filter) # 全部加入joinFilterList方便后面对比
        if not EqualSqlDetection(sqlList, tmpFilterNode):
            # 写入文件
            filename = "mv" + str(id) + ".sql"
            with open(mvPath + "/mvTest/" + filename, "w", encoding="utf-8") as f:
                f.write(sql)
            id += 1
            writeNum += 1
        else:
            unWriteNum += 1
        cnt += 1

    print("write: {0}, unWrite: {1}".format(writeNum, unWriteNum))


# 判断filterNode是否和sqlList里的某个node
def EqualSqlDetection(sqlList, filterNode):
    for node in sqlList:
        if set(node.fieldList) == set(filterNode.fieldList) \
                and set(node.tableList) == set(filterNode.tableList) \
                    and set(node.joinFilterList) == set(filterNode.joinFilterList) \
                        and set(node.commonFilterList) == set(filterNode.commonFilterList):
                return True
    return False


# 读取sql文件并解析为md5作为queryId
def getQueryId_SP(sqlFile):
    with open(sqlFile, "r") as f:
        sql = f.read()
        sqlStandard = normalizeSql(sql)

    return sql2md5(sqlStandard)


'''
三个文件的解析联合在一起
'''
def buildPlanTrees_SP(parentPath, clusters, g_query_subs, cntDict, solver,  g_table_common, q_time_map):
    sqlPath = parentPath + "/sql/"
    planPath = parentPath + "/plan/"
    dotPath = parentPath + "/dot/"
    nodePath = parentPath + "/node/"
    fileNames = os.listdir(sqlPath)
    try:
        fileNames = sorted(fileNames, key=lambda x: int(x[1:-4]))
    except:
        print("[buildPlanTrees_SP] sort error!")
    i = 0
    for file in fileNames:
        print(i)
        i += 1
        sqlFile = sqlPath + file
        key, ext = os.path.splitext(os.path.basename(sqlFile))
        planFile = planPath + "plan-" + key + ".lz4"
        dotFile = dotPath + "dot-" + key + ".lz4"
        nodeFile = nodePath + "node-" + key + ".lz4"
        if not os.path.exists(dotFile) or not os.path.exists(nodeFile) or not os.path.exists(sqlFile):
            print(sqlFile)
            print("dotFile or nodeFile must exists!")
            continue
        print(sqlFile)

        # if i == 2:
        #     print(i)
        #     print(i)
        with open("parse_sort_aggregate.txt", "a+") as f:
            f.write(f"q{i}" + "\n")
        # 处理sqlFile得到单独的id，处理planFile获取字段对应的表名，
        # 处理dotFile得到subGraph关系，最后处理nodeFile得到树的结构和过滤条件等
        queryId, planTrees, tableField = buildOnePlanTree_SP(sqlFile, planFile, dotFile, nodeFile, g_table_common)

        # 合并planTrees
        generate_relations_z3(planTrees, tableField)
        myIds = []
        start = time.time()
        for planTree in planTrees:
            # 不处理单表的情况，单表的情况后序FPMAX处理
            if len(planTree.filterNode.tableList) <= 1:
                continue
            # 合并操作
            myIds.append(classifySubtree_SP(clusters, planTree, cntDict, solver))

        end = time.time()
        q_time_map[i - 1] = end - start
        if len(myIds) != 0:
            myIds = list(set(myIds))
            myIds = sorted(myIds, key=lambda x: int(x))
            g_query_subs[key] = myIds





# 适配接口的解析流程
def build_plan_trees_new_SP(clusters, g_query_subs, cntDict, g_table_common, solver, q_time_map, is_incre):
    # 1.调用接口获取日志信息
    # 几个重要的路径
    logparser_jar_path = get_logparser_path()
    hdfs_spark_history_path = get_hdfs_inputpath()
    hdfs_output_path = get_hdfs_outputpath()
    cache_path = get_cache_path()
    database = getDatabase()
    # 获取所有的日志信息
    yarn_logs = get_all_yarn_logs()
    # 调用shell脚本获取一段时间内query
    if is_incre:
        start_time, end_time = get_query_incre_log_timestamp()
    else:
        start_time, end_time = get_query_log_timestamp()
    # 选择出时间内的日志
    interval_logs = get_yarn_logs_interval(yarn_logs, start_time, end_time)
    print(f"yarn logs between {start_time} and {end_time}\n{interval_logs}")

    if is_incre:
        query_md5 = get_table_file_from_exist(get_tpcds_path())
    else:
        query_md5 = get_table_file_from_exist(get_increment_path())
    print(f"all_log = {len(interval_logs)}")
    # 遍历所有的日志
    i = 1
    for app_id, value in interval_logs.items():
        print(i)
        i += 1
        # 获取一个query的执行计划
        hdfs_json_file = f"{hdfs_output_path}/{app_id}.json"
        original_query, node_metrics, physical_plan, dot_metrics, materialized_views = parse_json_logs(hdfs_json_file)
        if materialized_views != "":
            print("warn: [build_plan_trees_new_SP] not a common query but a q-mv query")
            continue
        if original_query == "" or node_metrics == "" or physical_plan == "" or dot_metrics == "":
            print(app_id)
            print(original_query)
            print(node_metrics)
            print(physical_plan)
            print(dot_metrics)
            print("warn: [build_plan_trees_new_SP] all 4 file must exists!")
            continue
        print(app_id)


        # 将query使用MD5编码
        # queryMD5Id = sql2md5(original_query)
        # 将四个文件写入临时文件
        tmpPath = "resources/tmp/"
        tmpQueryPath = tmpPath + "tmp_query.sql"
        tmpNodePath = tmpPath + "tmp_node.lz4"
        tmpPlanPath = tmpPath + "tmp_plan.lz4"
        tmpDotPath = tmpPath + "tmp_dot.lz4"
        with open(tmpQueryPath, "w", encoding="utf-8") as f:
            f.write(original_query)
        with open(tmpNodePath, "w", encoding="utf-8") as f:
            f.write(node_metrics)
        with open(tmpPlanPath, "w", encoding="utf-8") as f:
            f.write(physical_plan)
        with open(tmpDotPath, "w", encoding="utf-8") as f:
            f.write(dot_metrics)

        # 写入本地文件——用于本地测试
        # file_name = ""
        # if sql2md5(original_query) not in query_md5:
        #     print("error")
        #     file_name = "error"
        # else:
        #     print(query_md5[sql2md5(original_query)])
        #     file_name = query_md5[sql2md5(original_query)]
        # tmpPath = "resources/spark/plan-log/"
        # tmpQueryPath1 = tmpPath + "sql/" + file_name + ".sql"
        # tmpNodePath1 = tmpPath + "node/" + "node-" + file_name + ".lz4"
        # tmpPlanPath1 = tmpPath + "plan/" + "plan-" + file_name + ".lz4"
        # tmpDotPath1 = tmpPath + "dot/" + "dot-" + file_name + ".lz4"
        # with open(tmpQueryPath1, "w", encoding="utf-8") as f:
        #     f.write(original_query)
        # with open(tmpNodePath1, "w", encoding="utf-8") as f:
        #     f.write(node_metrics)
        # with open(tmpPlanPath1, "w", encoding="utf-8") as f:
        #     f.write(physical_plan)
        # with open(tmpDotPath1, "w", encoding="utf-8") as f:
        #     f.write(dot_metrics)

        # 处理sqlFile得到单独的id，处理planFile获取字段对应的表名，
        # 处理dotFile得到subGraph关系，最后处理nodeFile得到树的结构和过滤条件等
        queryId, planTrees, tableField = buildOnePlanTree_SP(tmpQueryPath, tmpPlanPath, tmpDotPath, tmpNodePath, g_table_common)

        # 给planTrees的每个结点添加relations，用于z3合并
        generate_relations_z3(planTrees, tableField)

        # 合并操作
        # 存储q-mv的对应关系
        myIds = []
        start = time.time()
        for planTree in planTrees:
            # 不处理单表的情况单表的情况后序FPMAX处理，
            if len(planTree.filterNode.tableList) <= 1:
                continue
            # # 根据表的大小来决定是否加入clusters
            # if not remove_table_with_big(planTree.filterNode.tableList):
            #     continue
            myIds.append(classifySubtree_SP(clusters, planTree, cntDict, solver))
        end = time.time()
        q_time_map[i - 1] = end - start
        if len(myIds) != 0:
            myIds = list(set(myIds))
            myIds = sorted(myIds, key=lambda x:int(x))
            g_query_subs[original_query] = myIds
            # 生成candidate_q_mv_csv
            mapping_path = getCandidateQMVMappings()
            with open(mapping_path, "a+") as f:
                f_csv = csv.writer(f)
                for id in myIds:
                    f_csv.writerow([original_query, id])



# 根据表的大小来决定是否加入clusters
def remove_table_with_big(tableList):
    bigTable = 0
    for table in tableList:
        if table.find(" AS ") != -1:
            table = table.split(" AS ")[0]
        table = table.upper()
        if tables_bytes[table]["type"] == "big":
            bigTable += 1
        if bigTable > 1:
            return False

    return True


# 解析一条执行计划，用于生成候选视图
def buildOnePlanTree_SP(sqlFile, planFile, dotFile, nodeFile, g_table_common, analyze=True):
    # 解析plan文件
    relationDict, tableDict, subQueryDict, tableField, aliasDict = getOneRelationTable(planFile)
    # 解析dot文件
    subIdMap = getOneDotFile(dotFile)
    # 解析node文件
    planTrees = parseOneNodeFile(nodeFile, relationDict, subIdMap, tableDict, subQueryDict, tableField, aliasDict, g_table_common)
    queryId = None
    if analyze:
        queryId = getQueryId_SP(sqlFile)
    return queryId, planTrees, tableField


# 解析一条执行计划，用于encode部分,已经废弃
def build_encode_one_planTree_SP(sqlFile, planFile, dotFile, nodeFile, analyze=True):
    # 解析plan文件
    relationDict, tableDict, subQueryDict, tableField, aliasDict = getOneRelationTable(planFile)
    # 解析dot文件
    subIdMap = getOneDotFile(dotFile)
    # 解析node文件
    planTrees = parse_encode_one_node_file(nodeFile, relationDict, subIdMap, tableDict, subQueryDict, tableField, aliasDict)
    queryId = None
    if analyze:
        queryId = getQueryId_SP(sqlFile)
    return queryId, planTrees, tableField


# 解析q-mv的执行计划，只需要获取时间信息
def build_encode_qmv_one_planTree_SP(sqlFile, planFile, dotFile, nodeFile, analyze=True):
    # 解析plan文件
    relationDict, tableDict, subQueryDict, tableField, aliasDict = getOneRelationTable(planFile)
    # 解析dot文件
    subIdMap = getOneDotFile(dotFile)
    # 解析node文件
    planTrees = parse_encode_qmv_one_node_file(nodeFile, relationDict, subIdMap, tableDict, subQueryDict, tableField,
                                           aliasDict)
    queryId = None
    if analyze:
        queryId = getQueryId_SP(sqlFile)
    return queryId, planTrees, tableField

group_clusters = {}
# 将newSubTree和clusters一一对比，看能否合并
def classifySubtree_SP(clusters, newSubTree, cntDict, solver):
    for cluId, minTree in clusters.items():
        if can_merge_z3_SP(cluId, minTree, newSubTree, solver):
            if newSubTree.totalTime < minTree.totalTime: # 更小的cost
                clusters[cluId] = newSubTree
            # 对应的field也要合并
            clusters[cluId].filterNode.fieldList = \
                list(set(newSubTree.filterNode.fieldList) | set(minTree.filterNode.fieldList))
            if clusters[cluId].isGroupBy:
                group_clusters[cluId] = clusters[cluId]
            # 引用计数增加
            cntDict[cluId] += 1
            return str(cluId)
    cluId = len(clusters)
    clusters[cluId] = {}
    # 保留最小cost的tree
    clusters[cluId] = newSubTree
    cntDict[cluId] = 1
    # clusters[cluId]["Cnt"] = 1
    return str(cluId)


# 判断orgTree和newTree能否合并
def canMerge_SP(clusterId, orgTree, newTree):
    # table
    if set(orgTree.filterNode.tableList) != set(newTree.filterNode.tableList):
        return False

    # joinFilter
    joinFilterSet1 = set(orgTree.filterNode.joinFilterList)
    joinFilterSet2 = set(newTree.filterNode.joinFilterList)
    if joinFilterSet1 != joinFilterSet2:
        if joinFilterSet1 - joinFilterSet2 != set() and joinFilterSet2 - joinFilterSet1 != set():
            return False

    # commonFilter
    commonFilterSet1 = set(orgTree.filterNode.commonFilterList)
    commonFilterSet2 = set(newTree.filterNode.commonFilterList)
    if commonFilterSet1 != commonFilterSet2:
        if commonFilterSet1 - commonFilterSet2 != set() and commonFilterSet2 - commonFilterSet1 != set():
            return False

    # field
    fieldSet1 = set(orgTree.filterNode.fieldList)
    fieldSet2 = set(newTree.filterNode.fieldList)
    if fieldSet1 != fieldSet2:
        if fieldSet1 - fieldSet2 != set() and fieldSet2 - fieldSet1 != set():
            return False

    return True


# 利用z3来实现合并
def can_merge_z3_SP(clusterId, orgTree, newTree, solver):
    # 对应的表不同
    if set(newTree.relations.keys()) != set(orgTree.relations.keys()):
        return False
    # 对比group by
    if orgTree.isGroupBy and newTree.isGroupBy:
        new_group_by_list = set(orgTree.group_by_list) | set(newTree.group_by_list)
        new_common_filter_list = set(orgTree.filterNode.commonFilterList) | set(newTree.filterNode.commonFilterList)
        new_join_filter_list = set(orgTree.filterNode.joinFilterList) | set(newTree.filterNode.joinFilterList)
        new_agg_list = set(orgTree.agg_list) | set(newTree.agg_list)
        orgTree.group_by_list = new_group_by_list
        newTree.group_by_list = new_group_by_list
        orgTree.filterNode.commonFilterList = new_common_filter_list
        newTree.filterNode.commonFilterList = new_common_filter_list
        orgTree.filterNode.joinFilterList = new_join_filter_list
        newTree.filterNode.joinFilterList = new_join_filter_list
        orgTree.agg_list = new_agg_list
        newTree.agg_list = new_agg_list
        print("[can_merge_z3_SP] group_by success!")
        return True
    if orgTree.isGroupBy or newTree.isGroupBy:
        return False

    # 先对比outerjoin条件
    outer_join_items_org = orgTree.relations["outer_join"]
    outer_join_items_new = newTree.relations["outer_join"]
    if len(outer_join_items_org.keys()) != len(outer_join_items_new.keys()):
        return False
    if set(outer_join_items_org.keys()) != set(outer_join_items_new.keys()):
        return False
    for k, v in outer_join_items_org.items():
        if set(outer_join_items_org[k]) != set(outer_join_items_new[k]):
            return False

    # 比较每个表的筛选条件和Join条件
    for rel in orgTree.relations:
        # 首先，两个树过滤条件对应的表的列应该相等
        if "variableNames" not in orgTree.relations[rel] and "variableNames" not in newTree.relations[rel]:
            continue
        if "variableNames" not in orgTree.relations[rel] and "variableNames" in newTree.relations[rel]:
            return False
        if "variableNames" in orgTree.relations[rel] and "variableNames" not in newTree.relations[rel]:
            return False
        if set(orgTree.relations[rel].keys()) != set(newTree.relations[rel].keys()):
            return False

        names = {}
        names1 = orgTree.relations[rel]["variableNames"]
        names2 = newTree.relations[rel]["variableNames"]

        # 二者涉及的属性不相等，则不需要比较具体条件，肯定不等价，直接返回False
        if set(names1["Literal"] + names1["Number"]) != set(names2["Literal"] + names2["Number"]):
            return False

        names["Literal"] = list(set(names1["Literal"] + names2["Literal"]))
        names["Number"] = list(set(names1["Number"] + names2["Number"]))

        if len(names["Literal"]) == 0 and len(names["Number"]) == 0:
            continue
        for name in list(set(names["Literal"])):
            tmp = String(name)
            exec('{}=tmp'.format(name))
        for name in list(set(names["Number"])):
            tmp = Real(name)
            exec('{}=tmp'.format(name))

        # 对比两棵树的每个过滤条件
        for col, condition in orgTree.relations[rel].items():
            if col == "variableNames":
                continue
            cond1 = condition
            cond2 = newTree.relations[rel][col]
            # 先进行字符串比较，如果字符串比较不同的话，再进行条件逻辑比较
            if cond2 == cond1:
                continue
            if col + "Of" + rel in names["Literal"]:
                # z3对多个字符串条件的比较有问题,先用字符串处理函数比较
                if cond1 in cond2:
                    orgTree.relations[rel][col] = cond2
                    continue
                if cond2 in cond1:
                    continue
                return False
            # 对同样的变量，有满足原始条件，不满足新条件的解，说明cond1包含了cond2
            try:
                solver.reset()
                solver.add(eval(cond1[0]))
                solver.add(eval(cond2[0]))
            except Exception as r:
                print("Exception raised!!!")
                print(r)
                print(rel)
                print(cond1)
                print(cond2)
                print(names)
                return False
            if str(solver.check()) == "unsat":
                # 如果没有同时满足两个条件的解，说明两个条件完全无关，可以将两个条件合并
                if cond1[0].find("Or") != -1 and cond2[0].find("Or") != -1:
                    orgTree.relations[rel][col] = [cond1[0][:-1] + "," + cond2[0][3:-1] + ")"]
                    newTree.relations[rel][col] = [cond1[0][:-1] + "," + cond2[0][3:-1] + ")"]
                elif cond1[0].find("Or") != -1:
                    orgTree.relations[rel][col] = [cond1[0][:-1] + "," + cond2[0] + ")"]
                    newTree.relations[rel][col] = [cond1[0][:-1] + "," + cond2[0] + ")"]
                elif cond2[0].find("Or") != -1:
                    orgTree.relations[rel][col] = [cond2[0][:-1] + "," + cond1[0] + ")"]
                    newTree.relations[rel][col] = [cond2[0][:-1] + "," + cond1[0] + ")"]
                else:
                    orgTree.relations[rel][col] = ["Or(" + cond1[0] + "," + cond2[0] + ")"]
                    newTree.relations[rel][col] = ["Or(" + cond1[0] + "," + cond2[0] + ")"]
                continue
            else:
                lapFlag = False
                # 如果两个条件有交集,则检查谁包含谁,或者是否没有包含关系
                solver.reset()
                solver.add(eval(cond1[0]))
                solver.add(Not(eval(cond2[0])))
                if str(solver.check()) == "sat":
                    # 对同样的变量，有满足原始条件，不满足新条件的解，说明cond1包含cond2或者两者相交
                    lapFlag = True
                solver.reset()
                solver.add(Not(eval(cond1[0])))
                solver.add(eval(cond2[0]))
                # 对同样的变量，有不满足原始条件，但是满足新条件的解，说明cond2包含cond1
                if str(solver.check()) == "sat":
                    if lapFlag is True:
                        return False
                    else:
                        # 说明cond2包含cond1
                        print("    __canMerge__newCondition:{}, orgCondition:{}".format(cond2, cond1))
                        orgTree.relations[rel][col] = cond2
                        continue
                else:
                    # 两个条件相等或者cond1包含cond2
                    continue

    return True


# 将普通的filter处理为z3能处理的格式
def transform_filter_z3(filterString):
    # 将普通的等号=转换为==
    if filterString.find(">") == -1 and \
            filterString.find("<") == -1 and \
                filterString.find("=") != -1 and \
                    filterString.find("==") == -1:
        filterString = filterString.replace("=", "==")
    # 处理OR的情况，OR需要前置
    if filterString.find("OR") != -1:
        tmpString = "Or("
        keyList = filterString.split("OR")
        for key in keyList:
            key = key.strip()
            tmpString += "(" + key + "),"
        filterString = tmpString[:-1] + ")"
    if filterString.find("AND") != -1:
        tmpString = "And("
        keyList = filterString.split("AND")
        for key in keyList:
            key = key.strip()
            tmpString += "(" + key + "),"
        filterString = tmpString[:-1] + ")"

    # 处理like/not like/!=
    filterString = re.sub(r"([\w.]+)[ ]*not like[ ]*\'([%()\- :\w]+)\'", r"(\1 == '__NOTLIKE__\2')", filterString)
    filterString = re.sub(r"([\w.]+)[ ]*like[ ]*\'([%()\- :\w]+)\'", r"(\1 == '__LIKE__\2')", filterString)
    filterString = re.sub(r"([\w.]+)[ ]*LIKE[ ]*\'([%()\- :\w]+)\'", r"(\1 == '__LIKE__\2')", filterString)
    filterString = re.sub(r"([\w.]+)[ ]*!=[ ]*\'([%()\- :\w]*)\'", r"(\1 == '__NOTEQUAL__\2')", filterString)

    # 处理in
    if filterString.find(" in ") != -1 or filterString.find(" IN ") != -1:
        patten = r"([\w.]+)[ ]*[Ii][Nn][ ]*\(([\w'(),:\- ]+)\)"
        key = re.findall(patten, filterString)
        if len(key) > 0:
            leftValue, rightCondition = key[0][0], key[0][1]
            rightValueList = rightCondition.split(",")
            condition = "Or("
            for rightValue in rightValueList:
                rightValue = rightValue.strip()
                condition += "(" + leftValue + " == " + rightValue + "),"
            filterString = condition[:-1] + ")"

    # 处理日期的格式：cast('1970-01-01' as date) + interval 11048 days
    if filterString.find("cast('1970-01-01' as date) + interval ") != -1:
        filterString = filterString.replace("cast('1970-01-01' as date) + interval ", "")
        filterString = filterString.replace(" days", "")

    return filterString


# 将condition从z3适应的结构切换回来
def reset_condition_z3(condition):
    condition = condition.replace("\'", "\"")

    # 将等号==转换为普通=
    if condition.find("==") != -1:
        condition = condition.replace("==", "=")

    # 处理OR的情况，OR后置
    if condition.find("Or") != -1:
        condition = condition[2:]
        condition = condition.replace(",", " OR ")
    # 处理AND的情况，AND后置
    if condition.find("And") != -1:
        condition = condition[3:]
        condition = condition.replace(",", " AND ")

    # 处理like/not like/!=
    condition = condition.replace("= \"__LIKE__", "like \"")
    condition = condition.replace("= \"__NOTLIKE__", "not like \"")
    condition = condition.replace("= \"__NOTEQUAL__", "!= \"")

    condition = condition.replace("\"", "\'")

    return condition


# 给planTrees的每个结点添加relations，用于z3合并
def generate_relations_z3(planTrees, tableField):

    for i, planTree in enumerate(planTrees):
        node = planTree.filterNode
        # 先处理tableList
        tableList = []
        for table in node.tableList:
            if table.find(" AS ") != -1:
                table = table.split(" AS ")[1].strip()
            tableList.append(table)
        # 找出filterList里包含的所有类似于*.*的
        relations = defaultdict()
        variableDict = {"Literal": [], "Number": []}
        # 将outerJoinFilterList的信息取出
        # outerJoinFilterList = []
        # for outerJoinFilter in node.outerJoinFilterList:
        #     outerJoinFilterList.append(outerJoinFilter[0])
        filterList = list(set(node.joinFilterList) | set(node.commonFilterList))

        # outerjoin条件
        relations["outer_join"] = defaultdict()
        for outerJoinFilter in node.outerJoinFilterList:
            filter = outerJoinFilter[0]
            table1 = outerJoinFilter[4]
            table2 = outerJoinFilter[5]
            # 将filter处理为z3处理的格式
            pattern = r"([\w]+)\.([\w]+)"
            keyList = re.findall(pattern, filter)
            if len(keyList) == 0:
                continue
            for key in keyList:
                table, field = key[0], key[1]
                if is_number(table) or is_number(field):
                    continue
                if table not in tableList:
                    print("[generate_relations_z3] current table not exist! {}".format(table))
                    continue
                # 先处理filter
                filter = filter.replace(table + "." + field, field + "Of" + table)
                filter = transform_filter_z3(filter)
            if table1 not in relations["outer_join"]:
                relations["outer_join"][table1] = [filter]
            else:
                relations["outer_join"][table1].append(filter)
            if table2 not in relations["outer_join"]:
                relations["outer_join"][table2] = [filter]
            else:
                relations["outer_join"][table2].append(filter)

        # 普通条件
        for filter in filterList:
            # order by和limit不参与
            if filter.find("limit") != -1 and filter.find("order by") != -1:
                continue
            pattern = r"([\w]+)\.([\w]+)"
            keyList = re.findall(pattern, filter)
            if len(keyList) == 0:
                continue
            # 先处理filter
            for key in keyList:
                table, field = key[0], key[1]
                if is_number(table) or is_number(field):
                    continue
                if table not in tableList:
                    print("[generate_relations_z3] current table not exist! {}".format(table))
                    continue
                # 先处理filter
                filter = filter.replace(table + "." + field, field + "Of" + table)
                filter = transform_filter_z3(filter)

            for key in keyList:
                table, field = key[0], key[1]
                # 判断
                if is_number(table) or is_number(field):
                    continue
                # 去掉别名的table
                pattern = r"[^0-9]+"
                key = re.findall(pattern, table)
                originalTable = key[0]

                if table not in tableList:
                    print("[generate_relations_z3] current table not exist! {}".format(table))
                    continue

                # 对应的表加入对应的列
                if table not in relations:
                    relations[table] = defaultdict()
                if field not in relations[table]:
                    relations[table][field] = []
                relations[table][field].append(filter)

                # 处理variableNames
                if tableField[originalTable][field]["type"] == "string":
                    variableDict["Literal"].append(field + "Of" + table)
                else:
                    variableDict["Number"].append(field + "Of" + table)

        # 去重
        variableDict["Literal"] = list(set(variableDict["Literal"]))
        variableDict["Number"] = list(set(variableDict["Number"]))
        for table, values in relations.items():
            for field, valueList in values.items():
                relations[table][field] = list(set(relations[table][field]))
            if len(variableDict["Literal"]) != 0 or len(variableDict["Number"]) != 0:
                relations[table]["variableNames"] = variableDict

        # 最后赋值给planTree
        planTrees[i].relations = relations


# 找到对应的sql
def get_table_file_from_exist(url="resources/tpcds/"):
    fileNames = os.listdir(url)
    query_md5 = {}
    for file in fileNames:
        filePath = url + file
        key, ext = os.path.splitext(os.path.basename(filePath))
        sql = ""
        with open(filePath, "r") as f:
            lines = f.readlines()
            for line in lines:
                sql += line
        sql_md5 = sql2md5(sql)
        query_md5[sql_md5] = key
    return query_md5

def get_table_file_from_exist_incre():
    tpcds_path = get_tpcds_path()
    fileNames = os.listdir(tpcds_path)
    query_md5 = {}
    for file in fileNames:
        filePath = tpcds_path + file
        key, ext = os.path.splitext(os.path.basename(filePath))
        sql = ""
        with open(filePath, "r") as f:
            lines = f.readlines()
            for line in lines:
                sql += line
        sql_md5 = sql2md5(sql)
        query_md5[sql_md5] = key
    incre_path = get_increment_path()
    fileNames = os.listdir(incre_path)
    for file in fileNames:
        filePath = incre_path + file
        key, ext = os.path.splitext(os.path.basename(filePath))
        sql = ""
        with open(filePath, "r") as f:
            lines = f.readlines()
            for line in lines:
                sql += line
        sql_md5 = sql2md5(sql)
        query_md5[sql_md5] = key
    return query_md5


# 将q-mv的对应关系输入到打印并输入到csv文件
def saveTmpQueryMap_SP(query_mv_map, initIDS, url="resources/tpcds/", is_incre=False):
    file = getTmpPath(DataType.Q_MV)
    # file = "resources/data/tmp_query-mvdata.csv"
    if file is None:
        print("[saveTmpQueryMap_SP] Please check the config file")
        return
    if is_incre:
        query_md5 = get_table_file_from_exist_incre()
    else:
        query_md5 = get_table_file_from_exist(url)
    with open(file, "w", newline="") as f:
        csv_writer = csv.writer(f, dialect="excel")
        # csv_writer.writerow(['file', 'id'])
        for fileName, ids in query_mv_map.items():
            key = re.findall(r"^q[0-9]*$", fileName)
            if len(key) > 0:
                print(fileName)
            else:
                if sql2md5(fileName) not in query_md5:
                    print("error")
                else:
                    print(query_md5[sql2md5(fileName)])
            print(ids)
            if not ids:
                continue
            for mvId in list(ids):
                if not mvId:
                    continue
                # 只取topn视图
                if int(mvId) in initIDS:
                    csv_writer.writerow([fileName, mvId])
    # 所有mv
    topFile = "./resources/data/tmp_query_all-mvdata.csv"
    with open(topFile, "w", newline="") as f:
        csv_writer = csv.writer(f, dialect="excel")
        # csv_writer.writerow(['file', 'id'])
        for fileName, ids in query_mv_map.items():
            # print(fileName)
            # print(ids)
            if not ids:
                continue
            for mvId in list(ids):
                if not mvId:
                    continue
                csv_writer.writerow([fileName, mvId])


# 将每个query出现的query和次数写进csv
def saveTmpQueryCntMap_SP(query_mv_map):
    file = "resource/data/tmp_query_cnt-mvdata3.csv"
    if file is None:
        print("[saveTmpQueryCntMap_SP] Please check the config file")
        return
    with open(file, "a+", newline="") as f:
        csv_writer = csv.writer(f, dialect="excel")
        csv_writer.writerow(['file', 'cnt', 'mvs'])
        for fileName, ids in query_mv_map.items():
            if not ids:
                continue
            csv_writer.writerow([fileName, len(ids), str(ids)])


# 推荐的数据中取出topn
def get_topn_from_study(q_mv_maps, clusters):
    cidList = []
    tmpDict = {}
    # refThreshold = getRefThreshold()

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
        for mv_id in mv:
            mv_q_map[int(mv_id)].append(q)
        cidList += list(set(mv))
    # 获取每个cid被使用的次数及每个cid涉及的表的个数
    for id in set(cidList):
        if len(clusters[int(id)].relations) > 1:
            tmpDict.update({id: (cidList.count(id), len(clusters[int(id)].relations))})

    cntLimit = int(getMvCntLimit())
    candDict = {k: v for k, v in tmpDict.items()}
    if len(tmpDict) < cntLimit:
        cntLimit = len(tmpDict)

    # 匹配尽可能多的query
    mv_list = [(k, v) for k, v in mv_q_map.items()]
    tmpList = sorted(mv_list, key=lambda x: len(x[1]))
    tmpList = reversed(tmpList)
    querySet = set()
    output = []
    for x in tmpList:
        mvId, mvList = x[0], x[1]
        if int(mvId) not in recommend_list:
            continue
        mvSet = set(mvList)
        if mvSet.issubset(querySet):
            continue
        output.append(mvId)
        querySet = querySet.union(mvSet)
        if len(output) >= cntLimit:
            break

    print(output)
    return candDict, output[:cntLimit]


# 将clusters/g_query_subs/cntDict写入ds文件
def save_all_gv_SP(clusters, g_query_subs, cntDict, mv_pre_path):
    # clusters
    save_candidate_gv_SP(clusters, mv_pre_path)
    g_query_subs_path = get_g_query_subs_path()
    cnt_dict_path = get_cnt_dict_path()
    # g_query_subs
    with open(g_query_subs_path, "wb") as f:
        pickle.dump(g_query_subs, f)
    # cntDict
    with open(cnt_dict_path, "wb") as f:
        pickle.dump(cntDict, f)


# 将clusters信息存入二进制ds文件
def save_candidate_gv_SP(clusters, mv_pre_path):
    #  删除文件夹下的所有文件
    remove_directory_file(mv_pre_path)
    for cludId, node in clusters.items():
        dsName = "mv" + str(cludId) + ".ds"
        with open(mv_pre_path + dsName, "wb") as f:
            pickle.dump(node, f)


# 从ds文件中读取clusters信息
def get_candidate_gv_SP(mv_path):
    clusters = {}
    dsPath = mv_path + "/ds/"
    fileNames = os.listdir(dsPath)
    if not fileNames:
        return dict()
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
        except EOFError:
            pass
    # 将数组按id排序
    clusters = sorted(clusters.items())
    clusters = {x[0]: x[1] for x in clusters}
    return clusters


# check树的错误情况
def check_clusters_common_SP(clusters, g_table_common):
    for id, node in clusters.items():
        for table in clusters[id].filterNode.tableList:
            clusters[id].filterNode.fieldList = list(set(clusters[id].filterNode.fieldList) | set(g_table_common[table]))


if __name__ == "__main__":

    # 1.单条sql测试
    url = "resource/queries/job/q/plan-log"
    sqlFile = url + "/sql/" + "application_1656680820985_0019.sql"
    planFile = url + "/plan/" + "application_1656680820985_0019.lz4"
    dotFile = url + "/dot/" + "DotFile-application_1656680820985_0019.lz4"
    nodeFile = url + "/node/" + "node-application_1656680820985_0019.lz4"
    queryId, clusters, tableField = buildOnePlanTree_SP(sqlFile, planFile, dotFile, nodeFile)
    generate_relations_z3(clusters, tableField)
    # 写sql
    mvPath = url + "/mv/sql/"
    # 4.生成候选视图
    WriteMVSql(clusters, 0, mvPath)

    generate_relations_z3(clusters, tableField)

    print(clusters)

    # 2.全局测试
    # clusters = {}
    # g_query_subs = {}
    # cntDict = {}
    # url = "./resource/queries/job/q/plan-log"
    # mvPath = url + "/mv/"
    # # typePath = getRawPath(DataType.Q, False)
    #
    # # 1.从文本解析sql，以便后续去重
    # sqlList = parseSQLFromExist(mvPath)
    #
    # # 2.将所有文件解析成树并合并——关键
    # buildPlanTrees_SP(url, clusters, g_query_subs, cntDict)
    #
    # # 3.去重相同的sql
    # writeCntDict, writeClusters = CheckAndDeleteEqualSql(mvPath, clusters, cntDict, sqlList)
    #
    # # 4.调用方法选出候选视图
    # idWithCnt, initIDS = getInitMVs_SP(g_query_subs, writeClusters, writeCntDict, method="topk")
    #
    # # 5.生成q-mv的对应关系
    # saveTmpQueryMap_SP(g_query_subs)
    #
    # # 6.生成候选视图
    # WriteMVSql(clusters, len(sqlList), mvPath)
    #
    # # 7.生成物化视图
    # realMVPath = url + "mvSql"
    # WriteRealMVSql(clusters, len(sqlList), realMVPath, initIDS)
    #
    #
    # # 打印id出现的次数
    # for id, cnt in writeCntDict.items():
    #     print("{0} : {1}".format(id, cnt))


    # checkTwoFile("./resource/queries/job/q/plan-log/mv/sql/mv4.sql", "./resource/queries/job/q/plan-log/mv/sql/mv32.sql")

    # 3.全局测试不考虑去重
    # solver = Solver()
    # clusters = {}
    # g_query_subs = {}
    # cntDict = {}
    # url = "resource/queries/job/q/plan-log"
    # mvPath = url + "/mv/sql/"
    # realMVPath = url + "/mv"
    # # 1.解析执行计划生成多棵树
    # buildPlanTrees_SP(url, clusters, g_query_subs, cntDict)
    # # 2.找到真正物化的树
    # idWithCnt, initIDS = getInitMVs_SP(g_query_subs, clusters, cntDict, method="topk")
    # # 3.生成q-mv的关系
    # saveTmpQueryMap_SP(g_query_subs, initIDS)
    # # 4.生成候选视图
    # WriteMVSql(clusters, 0, mvPath)
    # # 5.生成物化视图——带create materialized和不带都生成
    # WriteRealMVSql(clusters, 0, initIDS, realMVPath)


