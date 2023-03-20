from parsePlanJson_CH import *
from procCSVFile import *
import os.path
import gc
from z3 import *
from FPMAX import *


def canMerge(clusterId, orgTree, newTree):
    # 对应的表不相同
    # print("referred Relations.new:{},org:{}".format(set(newTree.relations.keys()), set(orgTree.relations.keys())))
    # 20220516需求：join顺序也需要做判断
    if set(newTree.relations.keys()) != set(orgTree.relations.keys()):
        return False
    # print("        compare with cluster " + str(clusterId))
    # 比较每个表的筛选条件和Join条件
    for rel in orgTree.relations:
        names = {}
        # print("            relation " + rel)
        # print("            relative info:{} ".format(orgTree.relations[rel]))
        if "variableNames" not in orgTree.relations[rel] and "variableNames" not in newTree.relations[rel]:
            continue
        if "variableNames" not in orgTree.relations[rel] and "variableNames" in newTree.relations[rel]:
            return False
        if "variableNames" in orgTree.relations[rel] and "variableNames" not in newTree.relations[rel]:
            return False

        if orgTree.relations.keys() != newTree.relations.keys():
            return False

        # cond1 = orgTree.relations[rel]["condition"]
        names1 = orgTree.relations[rel]["variableNames"]
        # print("            cond of cluster " + cond1)

        # cond2 = newTree.relations[rel]["condition"]
        names2 = newTree.relations[rel]["variableNames"]

        if set(names1["Literal"] + names1["Number"]) != set(names2["Literal"] + names2["Number"]):
            # 二者涉及的属性不相等，则不需要比较具体条件，肯定不等价，直接返回False
            return False

        names["Literal"] = list(set(names1["Literal"] + names2["Literal"]))
        names["Number"] = list(set(names1["Number"] + names2["Number"]))
        # print("names{}".format(names))
        # print(names2)
        # varString = list(set(names2["Literal"]))
        if len(names["Literal"]) == 0 and len(names["Number"]) == 0:
            # print(rel + " no Condition!!!")
            # orgTree.printTree(rel + ".gv")
            # exit(1)
            continue
        for name in list(set(names["Literal"])):
            # if name.find(".") != -1:
            #     name = name.split(".")[1]
            tmp = String(name)
            exec('{}=tmp'.format(name))
        for name in list(set(names["Number"])):
            tmp = Real(name)
            exec('{}=tmp'.format(name))

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
                # print("            before judge ")
                solver.reset()
                solver.add(eval(cond1[0]))
                solver.add(eval(cond2[0]))
                # print("            input condition ")
            except Exception as r:
                print("Exception raised!!!")
                print(r)
                print(rel)
                print(cond1)
                print(cond2)
                # print(names1)
                # print(names2)
                print(names)
                return False
                # exit(1)
            # print(solver.assertions())
            if str(solver.check()) == "unsat":
                # 如果没有同时满足两个条件的解，说明两个条件完全无关，可以将两个条件合并
                orgTree.relations[rel][col] = "Or(" + cond1[0] + "," + cond2[0] + ")"
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


def classifySubtree(clusters, newSubTree):
    # getTreeConditions(subTree)
    for cluId, minTree in clusters.items():
        # print("        compare with cluster " + str(cluId))
        if canMerge(cluId, minTree["Tree"], newSubTree):
            # print("        merge to cluster " + str(cluId))
            # subTree.clusterID = cid
            if newSubTree.data.cost < minTree["Tree"].data.cost:
                # print("        ====replace  tree of cluster{}====".format(str(cluId)))
                clusters[cluId]["Tree"] = newSubTree
            clusters[cluId]["Cnt"] += 1
            # clusters[cluId]["Tree"].printTree(save_path="cluster " + str(cluId) + ".gv")
            return str(cluId)
    cluId = len(clusters)
    clusters[cluId] = {}
    # 保留最小cost的tree
    clusters[cluId]["Tree"] = newSubTree
    clusters[cluId]["Cnt"] = 1
    return str(cluId)


def buildPlanTrees_CH(parentPath):
    sqlPath = parentPath + "/sql/"
    jsonPath = parentPath + "/json/"
    pipelinePath = parentPath + "/pipeline/"
    fileNames = os.listdir(sqlPath)
    threshold = getThreshold()
    for file in fileNames:
        sqlFile = sqlPath + "/" + file
        key, ext = os.path.splitext(os.path.basename(sqlFile))
        if ext != ".sql":
            continue
        pipelineFile = pipelinePath + "/" + key + ".pipeline"
        jsonFile = jsonPath + "/" + key + ".json"

        # sql语句对应的执行计划也需要存在
        # if not os.path.exists(jsonFile) or not os.path.exists(pipelineFile):
        if not os.path.exists(jsonFile):
            continue
        # if key.find("300c") == -1:
        #     continue

        # print("fileName:{}".format(key))
        queryId, planTrees = buildOnePlanTree_CH(sqlFile, jsonFile, pipelineFile, analyze=True, threshold=threshold)

        # pipeTree = buildPipelineTree(pipelineFile)
        # queryId, planTrees = buildPlanTree_CH(jsonFile, sqlFile)
        if queryId is None:
            continue
        # mergeTree_CH(planTrees[-1], pipeTree)
        myIds = [queryId]
        i = 0
        for planTree in planTrees:
            # print("    Processing subtree " + str(i))
            if len(planTree.relations) > 1:
                myIds.append(classifySubtree(g_candidate_clusters, planTree))
            i += 1
        # g_query_subs[queryId] = myIds
        if i != 0:
            g_query_subs[key] = myIds


def sortRelations(fromRels):
    relOrder = ["cast_info", "movie_info", "movie_keyword", "name", "char_name", "person_info", "aka_name",
                "movie_companies", "title", "movie_info_idx", "aka_title", "company_name", "complete_cast", "keyword",
                "movie_link", "info_type", "link_type", "role_type", "kind_type", "comp_cast_type", "company_type"]
    oldRels = fromRels.split(",")
    oldCnt = len(oldRels)
    newRels = " from "
    cnt = 0
    for rel in relOrder:
        if rel in oldRels:
            newRels += rel + ","
            cnt += 1
            if cnt == oldCnt:
                break
    return newRels[:-1]


if __name__ == '__main__':
    g_candidate_clusters = {}
    g_query_subs = {}
    solver = Solver()
    prePath = getRawPath(DataType.Q)
    if prePath is None:
        print("Please check the config file")
        exit(1)
    buildPlanTrees_CH(prePath)

    # print(g_query_subs)
    saveTmpQueryMap(g_query_subs)

    # print(g_candidate_clusters)
    mvPrePath = getRawPath(DataType.MV)
    if mvPrePath is None:
        print("Please check the config file")
        exit(1)
    cid = None
    createPre = "create materialized view if not exists mv"
    createENGINE = " engine = ReplicatedMergeTree() order by tuple() as "
    for cid, cluster in g_candidate_clusters.items():
        filename = "mv" + str(cid) + ".sql"
        # print("\ncid:{}".format(cid))
        # if cluster["Cnt"] > 1:
        # if len(cluster["Tree"].relations) > 1 and cluster["Cnt"] > 1:
        if len(cluster["Tree"].relations) > 1:
            attrs = ""
            fromSql = " from "
            whereSql = " where "
            for rel, colCondition in cluster["Tree"].relations.items():
                fromSql += rel + ","
                # print("rel:{}".format(rel))
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
                        condition = resetCondition2Sql(condition)
                        cons = []
                        for c in condition.split("And"):
                            if whereSql.find(c.strip()) != -1:
                                continue
                            cons.append(c)
                        condition = " And ".join(cons)

                        if condition != '':
                            whereSql += condition + " And "

            # fromSql = fromSql[:-1]
            fromSql = sortRelations(fromSql[6:-1])
            if cluster["Tree"].data.type == NodeType.EXPRESSION:
                attrs = cluster["Tree"].data.attribute
            attrType = type(attrs)
            if attrType == str:
                attrs = attrs[:-2]
            elif attrType == list:
                attrs = str(attrs)[1:-2].replace("'", "")
            if " where " == whereSql:
                sql = "select " + attrs + fromSql
            else:
                sql = "select " + attrs + fromSql + whereSql[:-5]
            with open(mvPrePath + "/sql/" + filename, "w", encoding="utf-8") as f:
                f.write(sql)
            with open(mvPrePath + "/createSql/" + filename, "w", encoding="utf-8") as f:
                f.write(createPre + str(cid) + createENGINE + sql)
            # print("filename:{}, sql:{}".format(mvPrePath + "/sql/" + filename, sql))
        #     cluster["Tree"].printTree(save_path="./tree/cluster" + str(cid) + "-" + str(cluster["Cnt"]) + ".gv")
    # 将单表的处理追加在后面
    createCubePre = "alter table "
    createCubeInter = " add projection cube"
    # createCubeApd = "type aggregate;"

    if cid is None:
        cid = 0
    else:
        cid = cid + 1
    cubeDict, condDict = getFMax(prePath, g_table.data)
    for table, attrs in cubeDict.items():
        filename = "mv" + str(cid) + ".sql"
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

        print("================={}=================".format(table))
        print("{}".format(str(attrs)[1:-2].replace(replacedSep, "") + "," + str(condDict[table])[1:-2].replace(
            "\'", "")))
        common_sql = "select " + str(attrs)[1:-2].replace(replacedSep, "") + "," + str(condDict[table])[1:-2].replace(
            "\'", "")
        if len(grpAttr) != 0:
            grpby_sql = " group by " + str(grpAttr)[1:-2].replace("\'", "")
        else:
            grpby_sql = ""
        with open(mvPrePath + "/sql/" + filename, "w", encoding="utf-8") as f:
            f.write(common_sql + " from " + table + grpby_sql)
        with open(mvPrePath + "/createSql/" + filename, "w", encoding="utf-8") as f:
            # f.write(createCubePre + table + createCubeInter + str(cid) + "(" + sql + ")" + createCubeApd)
            f.write(createCubePre + table + createCubeInter + str(cid) + "(" + common_sql + grpby_sql + ")")
        cid = cid + 1
    for table, attrs in condDict.items():
        if table in list(cubeDict.keys()):
            continue
        print("================={}=================".format(table))
        print("{}".format(attrs))
