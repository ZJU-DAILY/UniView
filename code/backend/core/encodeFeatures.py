# encoding: utf-8
import os
from parsePlanJson import *
from procSQL import *

dir_prefix = "resource/"
static_info = dir_prefix + "data/"
mvdata = static_info + "mvdata.csv"
query_mvdata = static_info + "query-mvdata.csv"
querydata = static_info + "querydata.csv"
import pickle


'''
mvdata:
    mv_id, cost, time, size
querydata:
    query_id, cost, time
query-mvdata:
    query_mv_id, cost, time
'''

queries_prefix = dir_prefix + "queries/"
job_prefix = queries_prefix + "join-order-benchmark/"
mv_q_prefix = queries_prefix + "job-mv-q-mv/"
# query_json = queries_prefix + "job/q/"
# mv_json = queries_prefix + "job/mv/"
# q_mv_json = queries_prefix + "job/q-mv/"


def extractKeywords(seq):
    key_list = []
    for node in seq:
        if isinstance(node, tuple):
            continue
        for att in node:
            if type(att) == str:
                continue
            if att[1] == 1:  # json2seq函数中设置的operator对应的value为1
                key_list.append(att[0])
    return list(set(key_list))


def getKeywordOfTables(table_list):
    table_set = list(set(table_list))
    res = []
    for table in table_set:
        t = table.upper()
        res += [t + "." + key for key in g_table.data[t]]
    return res


# 从plan转换回来的操作序列、操作耗费的时间和涉及的行数、表名和列名
def parseQuery(file, need_input, use_cost=False):
    print(file)
    f_open = open(file, "r")
    plan = json.load(f_open)
    f_open.close()
    try:
        plan = plan['Plan']
    except:
        plan = plan[0]['Plan']

    alias2table = {}
    buildAlias2Table(plan, alias2table)

    subPlan, cost, cardinality = getPlanSum(plan, use_cost)
    # json 2 class sequence
    seq, _, static_seq, input_tables = getPlanSeq(subPlan, alias2table)
    # print(seq)
    seqs = PlanInSeq(seq, cost, cardinality)
    js = json.loads(class2json(seqs))
    json_seq = json2seq(js)

    if need_input:
        input_keywords = getKeywordOfTables(input_tables)
        input_keywords += input_tables
    else:
        input_keywords = None

    return json_seq, static_seq, input_keywords


def getLatency(file):
    f_open = open(file, 'r')
    plan = json.load(f_open)['Plan']
    f_open.close()
    return plan['Actual Total Time']


# 所有从plan转换回来的操作序列、操作耗费的时间和涉及的行数、表名和列名
# 只需要json文件即可
def parsePath(path, splitName=False, isLatency=False, use_cost=False):
    fileNames = os.listdir(path+"/json")
    r_d = {}
    for file in fileNames:
        if not file.endswith("json"):
            continue
        key = file[:-5]
        if "mv" not in key:
            if "query" in key:
                key = key[5:]
            # if -1 != file.find("_"):
            #     key = key.split('_')[1]
        # mv需要保留mv，q_mv也需要加上mv
        if splitName:
            if "-" in key:
                keys = key.split('-')[0:2]
            else:
                keys = key.split('_')[0:2]
            mv_name = keys[1]
            if "mv" not in mv_name:
                mv_name = "mv" + mv_name
            key = (keys[0], mv_name)
        file = path + "/json/" + file
        need_input = splitName is False
        if isLatency is False:
            json_seq, static_seq, keywords = parseQuery(file, need_input, use_cost)
            r_d[key] = (json_seq, static_seq, keywords)
        else:
            r_d[key] = getLatency(file)
    return r_d


def recoverCondition(condition):
    # print("pre--condition:{}".format(condition))
    if not condition:
        return condition
    patten = re.compile('\w+Of\w+')
    colOfTables = patten.findall(condition)
    for colOfTable in list(set(colOfTables)):
        column, table = colOfTable.split("Of")
        condition = re.sub(r"\b" + colOfTable + r"\b", table + "." + column, condition)
    condition = resetCondition2Sql(condition, pyp=True)
    condition = condition.replace("!= \'", "= \'__NOTEQUAL__")
    # print("after--condition:{}".format(condition))
    return condition


# 从plan转换回来的操作序列、操作耗费的时间和涉及的行数、表名和列名
def parseTreeSeq_CH(root):
    # 遍历plan树，获取操作序列
    sequence = []
    join_conditions = []
    static_sequence = []
    input_tables = []

    def parseNodeSeq(node):
        data = copy.copy(node.data)
        if hasattr(node.data, "condition"):
            join_conditions.append(node.data.condition)
            if data.condition is not None:
                condMerged, _, conds = transSingleCond(None, data.condition, analyze=False)
                if len(conds) == 0:
                    condition = recoverCondition(condMerged)
                    data.condition = pre2seq(condition, {}, None, None)
                else:
                    condSeq = []
                    for cond in list(conds.values()):
                        condition = recoverCondition(cond)
                        condSeq += (pre2seq(condition, {}, None, None))
                    data.condition = condSeq
        if hasattr(node.data, "cost"):
            static_sequence.append([node.data.cost, 0.0])
        else:
            static_sequence.append([0.0, 0.0])
        if hasattr(node.data, "type") and node.data.type == NodeType.RELATION:
            input_tables.append(node.data.relation)
        sequence.append(data)
        if node.left is not None:
            parseNodeSeq(node.left)
        if node.right is not None:
            parseNodeSeq(node.right)

    if root is not None:
        parseNodeSeq(root)

    return sequence, join_conditions, static_sequence, input_tables


def parseQuery_CH(planTree, needInput=False):
    cost = planTree.data.cost
    cardinality = 0
    # json 2 class sequence
    seq, _, static_seq, input_tables = parseTreeSeq_CH(planTree)
    # print(seq)
    seqs = PlanInSeq(seq, cost, cardinality)
    js = json.loads(class2json(seqs))
    json_seq = json2seq(js)
    # print(json_seq)
    # print(list(set(extractKeywords(json_seq["seq"]))))
    if needInput is True:
        input_keywords = getKeywordOfTables(input_tables)
        input_keywords += input_tables
    else:
        input_keywords = None

    return json_seq, static_seq, input_keywords


def parseData_CH(dataType, isInc=False):
    typePath = getRawPath(dataType, isInc)
    if typePath is None:
        return
    if DataType.Q_MV == dataType:
        needInput = False
    else:
        needInput = True

    sqlPath = typePath + "/sql/"
    pipelinePath = typePath + "/pipeline/"
    jsonPath = typePath + "/json/"
    fileNames = os.listdir(sqlPath)
    q_dict = {}
    # run_file = ["1a", "1b", "1c", "1d", "2c", "3a", "3b", "3c", "4a", "4b", "4c", "5a", "5b", "5c", "10b", "10c", "11a",
    #             "11b", "12a", "12b", "12c", "13a", "13c", "13d", "14a", "14b", "14c", "18b", "18c", "21a", "21b", "21c",
    #             "27a", "27b", "27c", "30c", "32a"]
    for file in fileNames:
        if not file.endswith(".sql"):
            continue
        if file.find("mv132") != -1:
            continue
        if file.find("mv131") != -1:
            continue

        sqlFile = sqlPath + "/" + file
        key, ext = os.path.splitext(os.path.basename(sqlFile))
        pipelineFile = pipelinePath + "/" + key + ".pipeline"
        jsonFile = jsonPath + "/" + key + ".json"

        # if dataType == DataType.Q and get_use_projection() == "False" and key not in run_file:
        #     print(f"this file is not recommend to run! {key}")
        #     continue
        # 保留mv
        if DataType.Q_MV == dataType:
            if '-' in key:
                keys = key.split('-')[0:2]
            else:
                keys = key.split('_')[0:2]
            key = (keys[0], "mv" + keys[1])
        # sql语句对应的执行计划也需要存在
        if not os.path.exists(jsonFile) or not os.path.exists(pipelineFile):
            continue
        print(file)

        queryId, planTrees, projection_flag = buildOnePlanTree_CH(sqlFile, jsonFile, pipelineFile, threshold=getThreshold())
        if queryId is None or len(planTrees) == 0:
            continue

        json_seq, static_seq, keywords = parseQuery_CH(planTrees[-1], needInput)

        q_dict[key] = (json_seq, static_seq, keywords)
        if DataType.Q_MV == dataType:
            key = "-".join(key)

    return q_dict



# 传DataType.Q，返回query_json；传DataType.MV，返回mv_json；传DataType.Q_MV，三个都返回。
def getJsonPath(dataType):
    query_json = None
    mv_json = None
    q_mv_json = None
    if DataType.MV != dataType:
        query_json = getRawPath(DataType.Q)
        # query_json = g_table.config.get("json", "query_json")
    if DataType.Q != dataType:
        mv_json = getRawPath(DataType.MV)
        # mv_json = g_table.config.get("json", "mv_json")
    if DataType.Q_MV == dataType:
        q_mv_json = getRawPath(DataType.Q_MV)
        # q_mv_json = g_table.config.get("json", "q_mv_json")

    return [query_json, mv_json, q_mv_json]


def getJsonPath_Inc(dataType):
    query_json = None
    mv_json = None
    q_mv_json = None
    if DataType.MV != dataType:
        query_json = g_table.config.get("json", "inc_query_json")
    if DataType.Q != dataType:
        mv_json = g_table.config.get("json", "inc_mv_json")
    if DataType.Q_MV == dataType:
        q_mv_json = g_table.config.get("json", "inc_q_mv_json")

    return [query_json, mv_json, q_mv_json]


# PG的老版本
def buildData(dataType=DataType.Q_MV):
    jsonPath = getJsonPath(dataType)
    keywords = []
    r_l = []
    if DataType.Q == dataType:
        q_dict = parsePath(jsonPath[0], use_cost=True)
        q_time_dict = parsePath(jsonPath[0], isLatency=True)
        for key, value in q_dict.items():
            # print(key)
            # print(value)
            # exit()
            y = value[0]['cost']
            q = q_dict[key]
            q_expression = q[0]['seq']
            keywords += extractKeywords(q_expression)
            keywords += q[2]
            others = [q[0]['cost'], q[0]['cardinality']]
            x = (q_expression, q[1], q[2], others)
            r_l.append((x, y, key))
        return r_l, list(set(keywords))
    elif DataType.MV == dataType:
        mv_dict = parsePath(jsonPath[1], use_cost=True)
        for key, value in mv_dict.items():
            y = value[0]['cost']
            mv = mv_dict[key]
            mv_expression = mv[0]['seq']
            keywords += extractKeywords(mv_expression)
            keywords += mv[2]
            others = [mv[0]['cost'], mv[0]['cardinality']]
            x = (mv_expression, mv[1], mv[2], others)
            r_l.append((x, y, key))
        return r_l, list(set(keywords))
    elif DataType.Q_MV == dataType:
        q_dict = parsePath(jsonPath[0], use_cost=True)
        mv_dict = parsePath(jsonPath[1], use_cost=True)
        q_mv_dict = parsePath(jsonPath[2], True, use_cost=True)
        # print(len(q_dict),len(mv_dict),len(q_mv_dict))
        for key, value in q_mv_dict.items():
            y = value[0]['cost']
            key1, key2 = key[0], key[1]
            if key1 not in q_dict or key2 not in mv_dict:
                print(key1, key2)
                continue
            q = q_dict[key1]
            mv = mv_dict[key2]
            q_expression = q[0]['seq']
            mv_expression = mv[0]['seq']
            keywords += extractKeywords(q_expression)
            keywords += extractKeywords(mv_expression)
            keywords += q[2]
            keywords += mv[2]
            others = [q[0]['cost'], q[0]['cardinality'], mv[0]['cost'], mv[0]['cardinality']]
            x = (q_expression, q[1], q[2], mv_expression, mv[1], mv[2], others)
            r_l.append((x, y, key))
        return r_l, list(set(keywords))


# PG的新版本
def build_data(engine="PG", dataType=DataType.Q_MV):
    q_r_l = []
    mv_r_l = []
    qmv_r_l = []
    candidate_mv_l = []
    keywords = []

    if engine == "PG":
        jsonPath = getJsonPath(dataType)
        q_dict = parsePath(jsonPath[0], use_cost=True)
        mv_dict = parsePath(jsonPath[1], use_cost=True)
        q_mv_dict = parsePath(jsonPath[2], True, use_cost=True)
    elif engine == "CH":
        q_dict = parseData_CH(DataType.Q)
        mv_dict = parseData_CH(DataType.MV)
        q_mv_dict = parseData_CH(DataType.Q_MV)
    print(f"q_dict:{len(q_dict)},mv_dict:{len(mv_dict)},q_mv_dict:{len(q_mv_dict)}")
    # 获取topmv的id
    with open("./resources/data/topn_mv_id.ds", "rb") as f:
        initIDS = pickle.load(f)
    # 获得mv_bytes_dict大小
    # 如果存在该文件，使用存储；反之使用cost
    memory_flag = True
    path = get_mv_bytes_dict_path()
    try:
        with open(path, "rb") as f:
            mv_bytes_dict = pickle.load(f)
        print(mv_bytes_dict)
    except:
        memory_flag = False

    for key, value in q_dict.items():
        # print(key)
        # print(value)
        # exit()
        y = value[0]['cost']
        q = q_dict[key]
        q_expression = q[0]['seq']
        keywords += extractKeywords(q_expression)
        keywords += q[2]
        others = [q[0]['cost'], q[0]['cardinality']]
        x = (q_expression, q[1], q[2], others)
        q_r_l.append((x, y, key))

    for key, value in mv_dict.items():
        y = value[0]['cost']
        mv = mv_dict[key]
        mv_expression = mv[0]['seq']
        keywords += extractKeywords(mv_expression)
        keywords += mv[2]
        others = [mv[0]['cost'], mv[0]['cardinality']]
        x = (mv_expression, mv[1], mv[2], others)
        if memory_flag:
            if key in mv_bytes_dict and mv_bytes_dict[key]:
                mv_r_l.append((x, int(mv_bytes_dict[key]), key))
            else:
                candidate_mv_l.append((x, 0, key))
        else:
            if int(key[2:]) in initIDS:
                mv_r_l.append((x, y, key))
            else:
                candidate_mv_l.append((x, 0, key))
        # if int(key[2:]) in initIDS:
        #     mv_r_l.append((x, y, key))
        # else:
        #     candidate_mv_l.append((x, 0, key))

    for key, value in q_mv_dict.items():
        y = value[0]['cost']
        key1, key2 = key[0], key[1]
        if key1 not in q_dict or key2 not in mv_dict:
            print(key1, key2)
            continue
        q = q_dict[key1]
        mv = mv_dict[key2]
        q_expression = q[0]['seq']
        mv_expression = mv[0]['seq']
        # keywords += extractKeywords(q_expression)
        # keywords += extractKeywords(mv_expression)
        # keywords += q[2]
        # keywords += mv[2]
        others = [q[0]['cost'], q[0]['cardinality'], mv[0]['cost'], mv[0]['cardinality']]
        x = (q_expression, q[1], q[2], mv_expression, mv[1], mv[2], others)
        qmv_r_l.append((x, y, key))

    return q_r_l, mv_r_l, qmv_r_l, candidate_mv_l, list(set(keywords))


# CH的单表encoding
def build_projection_data():
    q_r_l = []
    mv_r_l = []
    qmv_r_l = []
    candidate_mv_l = []
    keywords = []

    q_dict = parseData_CH(DataType.Q)
    mv_dict = parseData_CH(DataType.MV)

    print(f"q_dict:{len(q_dict)},mv_dict:{len(mv_dict)}")
    # 获取topmv的id
    with open("./resources/data/topn_mv_id.ds", "rb") as f:
        initIDS = pickle.load(f)
    # query_id : (cost1, cost2, t1, t2)
    with open("./resources/data/query_id_cost_dict.ds", "rb") as f:
        query_id_cost_dict = pickle.load(f)
    print(query_id_cost_dict)
    # query_id : mv_id
    with open("./resources/data/projection_q_mv_dict", "rb") as f:
        projection_q_mv_dict = pickle.load(f)
    print(projection_q_mv_dict)
    projection_mv_dict = {}
    for k, v in projection_q_mv_dict.items():
        if v not in projection_mv_dict:
            projection_mv_dict[v] = query_id_cost_dict[k][1]

    for key, value in q_dict.items():
        # print(key)
        # print(value)
        # exit()
        if key not in query_id_cost_dict:
            # print(f"q encode error: {key}")
            continue
        y = query_id_cost_dict[key][2]
        q = q_dict[key]
        q_expression = q[0]['seq']
        keywords += extractKeywords(q_expression)
        keywords += q[2]
        others = [q[0]['cost'], q[0]['cardinality']]
        x = (q_expression, q[1], q[2], others)
        q_r_l.append((x, y, key))

    index = 0
    for key, value in mv_dict.items():
        y = value[0]['cost']
        mv = mv_dict[key]
        mv_expression = mv[0]['seq']
        keywords += extractKeywords(mv_expression)
        keywords += mv[2]
        others = [mv[0]['cost'], mv[0]['cardinality']]
        x = (mv_expression, mv[1], mv[2], others)
        if key not in projection_mv_dict:
            print(f"mv encode error: {key}")
            continue
        if index < len(mv_dict) / 3:
            mv_r_l.append((x, float(projection_mv_dict[key]), key))
        else:
            candidate_mv_l.append((x, 0, key))
        index += 1
        # if int(key[2:]) in initIDS:
        #     mv_r_l.append((x, y, key))
        # else:
        #     candidate_mv_l.append((x, 0, key))

    for key1, key2 in projection_q_mv_dict.items():
        if key1 not in query_id_cost_dict:
            continue
        y = query_id_cost_dict[key1][3]
        if key1 not in q_dict or key2 not in mv_dict:
            print(key1, key2)
            continue
        q = q_dict[key1]
        mv = mv_dict[key2]
        q_expression = q[0]['seq']
        mv_expression = mv[0]['seq']
        # keywords += extractKeywords(q_expression)
        # keywords += extractKeywords(mv_expression)
        # keywords += q[2]
        # keywords += mv[2]
        others = [q[0]['cost'], q[0]['cardinality'], mv[0]['cost'], mv[0]['cardinality']]
        x = (q_expression, q[1], q[2], mv_expression, mv[1], mv[2], others)
        qmv_r_l.append((x, y, (key1, key2)))

    return q_r_l, mv_r_l, qmv_r_l, candidate_mv_l, list(set(keywords))


# PG和多表CH训练数据生成
def generate_build_data(engine="PG"):
    q_data, mv_data, qmv_data, candidate_mv_data, q_keyword = build_data(engine)
    # 导入文件
    with open('./resources/data/q_data.txt', 'wb') as f:
        pickle.dump(q_data, f)
    with open('./resources/data/mv_data.txt', 'wb') as f:
        pickle.dump(mv_data, f)
    with open('./resources/data/qmv_data.txt', 'wb') as f:
        pickle.dump(qmv_data, f)
    with open('./resources/data/candidate_mv_data.txt', 'wb') as f:
        pickle.dump(candidate_mv_data, f)
    with open('./resources/data/q_keyword.txt', 'wb') as f:
        pickle.dump(q_keyword, f)

    # print(q_data)
    # print(mv_data)
    # print(qmv_data)
    # print(candidate_mv_data)
    # print(q_keyword)


# 生成CH单表训练数据
def generate_projection_data():
    q_data, mv_data, qmv_data, candidate_mv_data, q_keyword = build_projection_data()
    # 导入文件
    with open('./resources/data/q_data.txt', 'wb') as f:
        pickle.dump(q_data, f)
    with open('./resources/data/mv_data.txt', 'wb') as f:
        pickle.dump(mv_data, f)
    with open('./resources/data/qmv_data.txt', 'wb') as f:
        pickle.dump(qmv_data, f)
    with open('./resources/data/candidate_mv_data.txt', 'wb') as f:
        pickle.dump(candidate_mv_data, f)
    with open('./resources/data/q_keyword.txt', 'wb') as f:
        pickle.dump(q_keyword, f)



def buildPlans(file, need_input):
    f_open = open(file, "r")
    plan = json.load(f_open)['Plan']
    f_open.close()

    alias2table = {}
    buildAlias2Table(plan, alias2table)

    subPlan, cost, cardinality = getPlanSum(plan, False)
    # json 2 class sequence
    subPlans, input_tables = getSubPlans(subPlan, alias2table)

    # if need_input:
    input_keywords = getKeywordOfTables(input_tables)
    input_keywords += input_tables
    # else:
    #     input_keywords = None

    return subPlans, input_keywords


def buildData_Inc(dataType=DataType.Q_MV):
    jsonPath = getJsonPath_Inc(dataType)
    keywords = []
    r_l = []
    if DataType.Q == dataType:
        q_dict = parsePath(jsonPath[0], use_cost=True)
        q_time_dict = parsePath(jsonPath[0], isLatency=True)
        for key, value in q_dict.items():
            # print(key)
            # print(value)
            # exit()
            y = value[0]['cost']
            q = q_dict[key]
            q_expression = q[0]['seq']
            keywords += extractKeywords(q_expression)
            keywords += q[2]
            others = [q[0]['cost'], q[0]['cardinality']]
            x = (q_expression, q[1], q[2], others)
            r_l.append((x, y, key))
        return r_l, list(set(keywords))
    elif DataType.MV == dataType:
        mv_dict = parsePath(jsonPath[1], use_cost=True)
        for key, value in mv_dict.items():
            y = value[0]['cost']
            mv = mv_dict[key]
            mv_expression = mv[0]['seq']
            keywords += extractKeywords(mv_expression)
            keywords += mv[2]
            others = [mv[0]['cost'], mv[0]['cardinality']]
            x = (mv_expression, mv[1], mv[2], others)
            r_l.append((x, y, key))
        return r_l, list(set(keywords))
    elif DataType.Q_MV == dataType:
        q_dict = parsePath(jsonPath[0], use_cost=True)
        mv_dict = parsePath(jsonPath[1], use_cost=True)
        q_mv_dict = parsePath(jsonPath[2], True, use_cost=False)
        # print(len(q_dict),len(mv_dict),len(q_mv_dict))
        for key, value in q_mv_dict.items():
            y = value[0]['cost']
            key1, key2 = key[0], key[1]
            if key1 not in q_dict or key2 not in mv_dict:
                print(key1, key2)
                continue
            q = q_dict[key1]
            mv = mv_dict[key2]
            q_expression = q[0]['seq']
            mv_expression = mv[0]['seq']
            keywords += extractKeywords(q_expression)
            keywords += extractKeywords(mv_expression)
            keywords += q[2]
            keywords += mv[2]
            others = [q[0]['cost'], q[0]['cardinality'], mv[0]['cost'], mv[0]['cardinality']]
            x = (q_expression, q[1], q[2], mv_expression, mv[1], mv[2], others)
            r_l.append((x, y, key))
        return r_l, list(set(keywords))


if __name__ == '__main__':
    generate_build_data(get_engine())
    # generate_projection_data()