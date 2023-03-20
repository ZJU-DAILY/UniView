import os.path
from parsePlanJson_CH import *
from parsePlanJson_PG import *

g_candidate_clusters = {}
g_query_subs = {}


def getReferredKeys(alias2table, sqlFile, relations, schemaDict):
    itemAll = defaultdict(list)
    with open(sqlFile) as file:
        data = file.read()
        sql = normalizeSql(data)
        parser = sqlparser.MViewParser(sql)
        for col in parser.columns:
            if col.find(".") != -1:
                column = changeAlias2Table(col, alias2table)
                col = column.split(".")[-1]
                curTable = ".".join(column.split(".")[:-1])
                if curTable.upper() not in schemaDict.keys():
                    continue
            else:
                curTable = getTableFromItem(col, relations, schemaDict)
            if curTable is not None:
                itemAll[curTable].append(col)
    return itemAll


def classifySubtree(clusters, newSubTree, cur_engine, projection_flag):
    # getTreeConditions(subTree)
    maxCid = 0
    for cluId, minTree in clusters.items():
        if projection_flag:
            maxCid = len(clusters)
            break
        if cluId > maxCid:
            maxCid = cluId
        # print("        compare with cluster " + str(cluId))
        if canMerge_PG(cluId, minTree["Tree"], newSubTree):
            # if canMerge(cluId, minTree["Tree"], newSubTree):
            # print("        merge to cluster " + str(cluId))
            # subTree.clusterID = cid
            if cur_engine == 'CH':
                newCost = newSubTree.data.cost
                oldCost = minTree["Tree"].data.cost
            elif cur_engine == 'PG':
                newCost = newSubTree.data['Total Cost']
                oldCost = minTree["Tree"].data['Total Cost']

            # 如果能够合并的两个子树，除了条件合并之外，涉及的属性列也进行合并
            for rel, attrs in clusters[cluId]["Tree"].referKeys.items():
                clusters[cluId]["Tree"].referKeys[rel] = \
                    list(set(minTree["Tree"].referKeys[rel]).union(set(newSubTree.referKeys[rel])))
                newSubTree.referKeys[rel] = \
                    list(set(minTree["Tree"].referKeys[rel]).union(set(newSubTree.referKeys[rel])))
            # 如果在这里替换，那么可能导致minTree和clusters[cluId]是同一个，丢失列
            # 所以需要先合并列
            if newCost < oldCost:
                clusters[cluId]["Tree"] = newSubTree
            clusters[cluId]["Cnt"] += 1
            # clusters[cluId]["Tree"].printTree(save_path="cluster " + str(cluId) + ".gv")
            return str(cluId)
    # cluId = len(clusters)
    cluId = maxCid+1
    clusters[cluId] = {}
    # 保留最小cost的tree
    clusters[cluId]["Tree"] = newSubTree
    clusters[cluId]["Cnt"] = 1
    return str(cluId)


def buildPlanTrees(clusters, parentPath, cur_engine='PG'):
    global g_candidate_clusters
    global g_query_subs
    sqlPath = parentPath + "/sql/"
    jsonPath = parentPath + "/json/"
    pipelinePath = parentPath + "/pipeline/"
    fileNames = os.listdir(sqlPath)
    try:
        fileNames = sorted(fileNames, key=lambda x: int(x[1:-4]))
    except:
        try:
            fileNames = sorted(fileNames, key=lambda x: (int(x.split(".")[0][:-1]), x.split(".")[0][-1]))
        except:
            pass
    # run_file = ["1a","1b","1c","1d","2c","3a","3b","3c","4a","4b","4c","5a","5b","5c","10b","10c","11a","11b","12a","12b","12c","13a","13c","13d","14a","14b","14c","18b","18c","21a","21b","21c","27a","27b","27c","30c","32a"]
    # print(f"CH imdb can run len: {len(run_file)}")
    for file in fileNames:
        sqlFile = sqlPath + "/" + file
        key, ext = os.path.splitext(os.path.basename(sqlFile))
        if ext != ".sql":
            continue
        # if key not in run_file and cur_engine == "CH" and get_use_projection() == "False":
        #     print(f"this file is not recommend to run! {key}")
        #     continue
        pipelineFile = pipelinePath + "/" + key + ".pipeline"
        jsonFile = jsonPath + "/" + key + ".json"
        # sql语句对应的执行计划也需要存在
        if not os.path.exists(jsonFile):
            continue
        # 如果是clickhouse，pipeline文件也需要存在
        if cur_engine == 'CH' and not os.path.exists(pipelineFile):
            continue

        print("fileName:{}".format(file))
        if cur_engine == 'CH':
            queryId, planTrees, projection_flag = buildOnePlanTree_CH(sqlFile, jsonFile, pipelineFile, analyze=True,
                                                     threshold=getThreshold())

        elif cur_engine == 'PG':
            queryId, planTrees = buildOnePlanTree_PG(sqlFile, jsonFile, analyze=True)

        # pipeTree = buildPipelineTree(pipelineFile)
        # queryId, planTrees = buildPlanTree_CH(jsonFile, sqlFile)
        if queryId is None or len(planTrees) == 0:
            continue

        myIds = []
        # clickhouse且是单表
        if cur_engine == 'CH' and not projection_flag:
            myIds.append(classifySubtree(clusters, planTrees[-1], cur_engine, True))
        else:
            i = 0
            for planTree in planTrees:
                if len(planTree.relations) > 1:
                    myIds.append(classifySubtree(clusters, planTree, cur_engine, False))
                i += 1

        if len(myIds) != 0:
            g_query_subs[key] = list(set(myIds))