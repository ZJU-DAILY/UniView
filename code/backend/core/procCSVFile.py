# import os
import random
import csv
from encodeFeatures import *

alpha = 1.67e-5
gama = 1e-1
ms2s = 1e-3
byte2gShift = 30


class Query(object):
    def __init__(self, id, cost, time):
        self.id = id
        self.cost = cost
        self.time = time


class MV(object):
    def __init__(self, id, cost, storage, time):
        self.id = id
        self.cost = cost
        self.storage = storage
        self.time = time

    def getOverhead(self):
        return self.cost + self.storage


class Query_MV(object):
    def __init__(self, query_id, mv_id, cost, time):
        self.query_id = query_id
        self.mv_id = mv_id
        self.cost = cost
        self.time = time


def getBuildCSVPath(dataType):
    csvFilePath = None
    if DataType.Q == dataType:
        csvFilePath = g_table.config.get("csv", "temp_query_csv")
    elif DataType.MV == dataType:
        csvFilePath = g_table.config.get("csv", "temp_mv_csv")
    elif DataType.Q_MV == dataType:
        # csvFilePath = g_table.config.get("csv", "temp_q_mv_csv")
        csvFilePath = g_table.config.get("csv", "q_mv_csv")

    return csvFilePath


def buildCSVFile(wdRstFile, dataType=DataType.Q_MV):
    jsonPath = getJsonPath(dataType)
    timeDict = {}
    if DataType.Q == dataType:
        timeDict = parsePath(jsonPath[0], isLatency=True)
    elif DataType.MV == dataType:
        timeDict = parsePath(jsonPath[1], isLatency=True)
    elif DataType.Q_MV == dataType:
        timeDict = parsePath(jsonPath[2], True, isLatency=True)
    csvFile = getBuildCSVPath(dataType)
    wFile = open(csvFile, 'a', encoding='utf-8', newline='')

    with open(wdRstFile, 'r', encoding='utf-8') as r:
        for line in r.readlines():
            if DataType.Q_MV == dataType:
                keys = []
                for tkey in line.split(")")[0].split("(")[-1].strip().split(","):
                    keys.append(tkey.strip().strip('\''))
                key = tuple(keys)
                cost = line.split(")")[1].split(",")[1].strip()
            else:
                key = line.split("'")[1].strip()
                cost = line.split(",")[1].strip()
            if key in timeDict:
                time = timeDict[(key)]
            else:
                continue
            if DataType.MV == dataType:
                # size = random.randint(4096, 21474836480)  # 4K--20G
                # wFile.write("mv{},{},{},{}\n".format(key, cost, time, size))
                wFile.write("mv{},{},{}\n".format(key, cost, time))
            elif DataType.Q == dataType:
                wFile.write("{},{},{}\n".format(key, cost, time))
            else:
                wFile.write("{}-{},{},{}\n".format(key[0], key[1], cost, time))
    wFile.close()
    return


def loadCSVFile_CH(query_file, mv_file, query_mv_file, recommend_mv_file):
    queries = {}
    mvs = {}
    recommend_mvs = {}
    q_mvs = {}
    mv_edge = {}
    q_dict = {}
    mv_dict = {}
    recommend_mv_dict = {}
    q_mv_dict = {}
    # ./resources/data/mvdata.csv
    with open(mv_file, 'r') as r:
        for line in r.readlines():
            id, cost = line.strip().split(',')[0:2]
            cost = float(cost)
            # id = id[2:]
            mvs[id] = MV(id, float(cost), 0, 0)
            mv_dict[id] = {"cost": float(cost), "time": 0, "size": 0}
    # ./resources/data/recommend-mvdata.csv
    with open(recommend_mv_file, 'r') as r:
        for line in r.readlines():
            id, cost = line.strip().split(',')[0:2]
            cost = float(cost)
            # id = id[2:]
            recommend_mvs[id] = MV(id, float(cost), 0, 0)
            recommend_mv_dict[id] = {"cost": float(cost), "time": 0, "size": 0}
    # ./resources/data/querydata.csv
    with open(query_file, 'r') as r:
        for line in r.readlines():
            id, cost = line.strip().split(",")[0:2]
            cost = float(cost)
            queries[id] = Query(id, float(cost), 0)
            # queries[id + "2"] = Query(id + "2", float(cost), 0)
            q_dict[id] = {"cost": float(cost), "time": 0}
            # q_dict[id + "2"] = {"cost": float(cost), "time": 0}
    # ./resources/data/query-mvdata.csv
    with open(query_mv_file, 'r') as r:
        for line in r.readlines():
            id, cost = line.strip().split(',')[0:2]
            cost = float(cost)
            id = id.split('-')
            if id[0] in queries and id[1] in mvs:
                q_mvs[(id[0], id[1])] = Query_MV(id[0], id[1], float(cost), 0)
                # q_mvs[(id[0] + "2", id[1])] = Query_MV(id[0] + "2", id[1], float(cost), 0)
                q_mv_dict[(id[0], id[1])] = {"cost": float(cost), "time":0}
                # q_mv_dict[(id[0] + "2", id[1])] = {"cost": float(cost), "time": 0}
                temp = mv_edge.get(id[1], [])
                temp.append(id[0])
                # temp.append(id[0] + "2")
                mv_edge[id[1]] = temp
    return queries, mvs, q_mvs, recommend_mvs, mv_edge, q_dict, mv_dict, recommend_mv_dict, q_mv_dict


def loadCSVFile(query_file, mv_file, query_mv_file, recommend_mv_file):
    queries = {}
    mvs = {}
    recommend_mvs = {}
    q_mvs = {}
    mv_edge = {}
    q_dict = {}
    mv_dict = {}
    recommend_mv_dict = {}
    q_mv_dict = {}
    with open(mv_file, 'r') as r:
        for line in r.readlines():
            id, cost, time, size = line.strip().split(',')
            cost = float(cost) * gama
            time = float(time) * ms2s
            size = (int(size) >> byte2gShift) * alpha
            id = id[2:]
            mvs[id] = MV(id, float(cost), float(size), float(time))
            mv_dict[id] = {"cost": float(cost), "time": float(time), "size": float(size)}

    with open(recommend_mv_file, 'r') as r:
        for line in r.readlines():
            id, cost, time, size = line.strip().split(',')
            cost = float(cost) * gama
            time = float(time) * ms2s
            size = (int(size) >> byte2gShift) * alpha
            id = id[2:]
            recommend_mvs[id] = MV(id, float(cost), float(size), float(time))
            recommend_mv_dict[id] = {"cost": float(cost), "time": float(time), "size": float(size)}

    with open(query_file, 'r') as r:
        for line in r.readlines():
            id, cost, time = line.strip().split(",")
            cost = float(cost) * gama
            time = float(time) * ms2s
            queries[id + "1"] = Query(id + "1", float(cost), float(time))
            queries[id + "2"] = Query(id + "2", float(cost), float(time))
            q_dict[id + "1"] = {"cost": float(cost), "time": float(time)}
            q_dict[id + "2"] = {"cost": float(cost), "time": float(time)}

    with open(query_mv_file, 'r') as r:
        for line in r.readlines():
            id, cost, time = line.strip().split(',')
            cost = float(cost) * gama
            time = float(time) * ms2s
            id = id.split('-')
            assert len(id) == 2
            if id[0] + "1" in queries and id[1] in mvs:
                q_mvs[(id[0] + "1", id[1])] = Query_MV(id[0] + "1", id[1], float(cost), float(time))
                q_mvs[(id[0] + "2", id[1])] = Query_MV(id[0] + "2", id[1], float(cost), float(time))
                q_mv_dict[(id[0] + "1", id[1])] = {"cost": float(cost), "time": float(time)}
                q_mv_dict[(id[0] + "2", id[1])] = {"cost": float(cost), "time": float(time)}
                temp = mv_edge.get(id[1], [])
                temp.append(id[0] + "1")
                temp.append(id[0] + "2")
                mv_edge[id[1]] = temp
    return queries, mvs, recommend_mvs, q_mvs, mv_edge, q_dict, mv_dict, recommend_mv_dict, q_mv_dict

def rebuildCSVFile(dataType):
    queries = {}
    final_queries = {}
    filePath = getBuildCSVPath(dataType)
    with open(filePath, 'r') as r:
        for line in r.readlines():
            id, cost, time = line.strip().split(",")
            if id not in final_queries.keys():
                final_queries[id] = {"cost": float(0), "time": float(0)}
            for idx in range(20):
                if id + str(idx) not in queries.keys():
                    queries[id + str(idx)] = {"cost": float(cost)}
                    if DataType.Q_MV != dataType:
                        if float(final_queries[id]["cost"]) < float(cost):
                            mxcost = float(cost)
                        else:
                            mxcost = float(final_queries[id]["cost"])
                    else:
                        if float(final_queries[id]["cost"]) > float(cost):
                            mxcost = float(cost)
                        else:
                            mxcost = float(final_queries[id]["cost"])
                    final_queries[id] = {"cost": float(mxcost), "time": float(time)}
                    # total = float(final_queries[id]["cost"]) * idx + float(cost)
                    # final_queries[id] = {"cost": float(total/(idx+1)), "time": float(time)}
                    break
    rFilePath = getResultPath(dataType)
    wFile = open(rFilePath, 'w', encoding='utf-8', newline='')
    if DataType.MV == dataType:
        for key, value in final_queries.items():
            size = random.randint(4096, 2147483648)  # 4K--2G
            wFile.write("{},{},{},{}\n".format(key, value["cost"], value["time"], size))
    else:
        for key, value in final_queries.items():
            wFile.write("{},{},{}\n".format(key, value["cost"], value["time"]))
    wFile.close()


# 只保存ID和时间
def appendBaseCSVFile(data, file):
    header = ['id', 'cost']
    with open(file, "a", newline="") as f:
        if isinstance(data[0], dict):
            # csv_writer = csv.writer(f, dialect="excel")
            csv_writer = csv.DictWriter(f, header)
        else:
            csv_writer = csv.writer(f, dialect="excel")
        if len(data) > 1:
            csv_writer.writerows(data)
        elif len(data) == 1:
            csv_writer.writerow(data[0])
    return


def appendQueryFile(data):
    rstFile = getResultPath(DataType.Q)
    if rstFile is None:
        print("Please check the config file")
        return
    appendBaseCSVFile(data, rstFile)


def appendCSVFile(data, dataType):
    rstFile = getResultPath(dataType)
    if rstFile is None:
        print("Please check the config file")
        return
    return appendBaseCSVFile(data, rstFile)


def saveTmpQueryMap(query_mv_map, initIDS=[]):
    # ./resources/data/tmp_query-mvdata.csv
    file = getTmpPath(DataType.Q_MV)
    if file is None:
        print("Please check the config file")
        return
    with open(file, "w", newline="") as f:
        csv_writer = csv.writer(f, dialect="excel")
        # csv_writer.writerow(['file', 'id'])
        for fileName, ids in query_mv_map.items():
            print(fileName)
            print(list(set(ids)))
            for mvId in list(set(ids)):
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

def loadQueryMap():
    file = getTmpPath(DataType.Q_MV)
    if file is None:
        print("Please check the config file")
        return
    with open(file, "r", newline="") as f:
        csv_reader = csv.DictReader(f)
        keys = [row['id'] for row in csv_reader]
    return keys

# if __name__ == '__main__':
# appendCSVFile([{'id': '1', 'cost': '12.12'}, {'id': '1', 'cost': '12.34'}], dataType=DataType.Q)
# appendCSVFile([[2, 444]], dataType=DataType.Q)
# rebuildCSVFile(dataType=DataType.Q)
# tempFile = getTmpPath(DataType.Q_MV)
# if tempFile is None:
#     print("Please check the config file")
#     exit(1)
# with open(tempFile, "r", encoding="utf-8") as f:
#     reader = csv.DictReader(f)
#     # print(reader)
#     column = [row for row in reader]

# print(column)
