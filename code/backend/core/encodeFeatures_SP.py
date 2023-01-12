import re

from joinCandidate_SP import *
from joinCandidate import *
import subprocess
import json


# 从tmp_query-mvdata.csv读取q-mv对应关系并执行shell脚本
def readcsv_execute_shell_SP(dataType):
    file = getTmpPath(dataType)
    qFile = getRawPath(DataType.Q)
    mvFile = getRawPath(DataType.MV)
    qmvFile = getRawPath(DataType.Q_MV)
    with open(file) as f:
        for line in f:
            if line.find("file") != -1 or line.find("id") != -1:
                continue
            q, mv = line.split(",")[0:2]
            q, mv = q.strip(), mv.strip()
            # 去掉query
            pattern = r"[^0-9]+([0-9]+)"
            key = re.findall(pattern, q)
            q = key[0]
            shellCommand = "source /etc/profile; bash /home/tmp/train_data.sh {0} {1} {2} {3} {4}".format(qFile, mvFile, qmvFile, q, mv)
            subprocess.call(shellCommand, shell=True)


def buildData_SP(isInc=False, is_incre=False):
    # 因为需要获取mv的大小，所以需要启动spark_session
    # spark_session = init_pyspark(getDatabase())
    # os.environ['SPARK_HOME'] = '/usr/local/spark'
    # os.environ['HADOOP_HOME'] = '/usr/local/hadoop'
    q_dict = parseData_SP(DataType.Q, isInc, is_incre)
    mv_dict = parse_mv_data_SP(DataType.MV)
    q_mv_dict = parse_qmv_data_SP(DataType.Q_MV, isInc, is_incre)
    candidate_q_mv_dict = parse_candidate_qmv_data_SP()

    if len(q_dict) == 0:
        print("[buildData_SP] q_dict is empty! please check front steps!")
        exit(-1)
    if len(mv_dict) == 0:
        print("[buildData_SP] mv_dict is empty! please check front steps!")
        exit(-1)
    if len(q_mv_dict) == 0:
        print("[buildData_SP] q_mv_dict is empty! please check front steps!")
        exit(-1)
    print(f"q_dict:{len(q_dict)},mv_dict:{len(mv_dict)},q_mv_dict:{len(q_mv_dict)}")

    # 获得mv_bytes_dict大小
    path = get_mv_bytes_dict_path()
    with open(path, "rb") as f:
        mv_bytes_dict = pickle.load(f)
    print(mv_bytes_dict)

    keywords = []
    q_r_l = []
    candidate_mv_l = []
    qmv_r_l = []
    candidate_qmv_r_l = []
    mv_r_l = []
    for key, q in q_dict.items():
        q_expression = q[0]['seq']
        keywords += extractKeywords(q_expression)
        keywords += q[2]
        # 新增
        others = [0, q[0]['cardinality']]
        x = (q_expression, q[1], q[2], others)
        if q[2]:
            q_r_l.append((x, q[0]['cost'], key))

    for key, mv in mv_dict.items():
        mv_expression = mv[0]['seq']
        keywords += extractKeywords(mv_expression)
        keywords += mv[2]
        # 新增
        others = [mv[0]['cost'], mv[0]['cardinality']]
        x = (mv_expression, mv[1], mv[2], others)
        if mv[2]:
            mv_name = key.split(".")[1]
            if mv_name in mv_bytes_dict and mv_bytes_dict[mv_name]:
                mv_r_l.append((x, int(mv_bytes_dict[mv_name]), key))
            else:
                candidate_mv_l.append((x, 0, key))

    for key, value in q_mv_dict.items():
        y = value
        key = eval(key)
        key1, key2 = key[0], key[1]
        if key1 not in q_dict or key2 not in mv_dict:
            print("[buildData_SP] q_mv_dict not found!")
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

    for key, value in candidate_q_mv_dict.items():
        y = value
        # key = eval(key)
        key1, key2 = key[0], key[1]
        if key1 in q_dict and key2 in mv_dict:
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
            candidate_qmv_r_l.append((x, y, key))



    return q_r_l, qmv_r_l, mv_r_l, candidate_mv_l, candidate_qmv_r_l, list(set(keywords))


# 处理mv的转换操作
def parse_mv_data_SP(dataType, isInc=False):
    typePath = getRawPath(dataType, isInc)
    if typePath is None:
        return
    if DataType.Q_MV == dataType:
        needInput = False
    else:
        needInput = True

    q_dict = {}
    # 从临时文件mv目录下面获取所有的cluster, 对mv的执行计划进行编码
    clusters = get_candidate_gv_SP(typePath)
    for cluid, node in clusters.items():
        json_seq, static_seq, keywords = parseQuery_SP(node, needInput)

        q_dict[getDatabase() + ".mv" + str(cluid)] = (json_seq, static_seq, keywords)
        # if DataType.Q_MV == dataType:
        #     key = "-".join(key)
        # appendCSVFile([[key, json_seq['cost']]], dataType)
    return q_dict


# 所有从plan转换回来的操作序列、操作耗费的时间和涉及的行数、表名和列名
def parseData_SP(dataType, isInc=False, is_incre=False):

    if DataType.Q_MV == dataType:
        needInput = False
    else:
        needInput = True

    q_dict = {}
    # 1.调用接口获取日志信息
    # 几个重要的路径
    # TODO: configure the path
    hdfs_output_path = get_hdfs_outputpath()
    hdfs_spark_history_path = get_hdfs_inputpath()
    # 获取所有的日志信息
    yarn_logs = get_all_yarn_logs()
    # 调用shell脚本获取一段时间内query
    if is_incre:
        start_time_tpcds, end_time_tpcds = get_query_log_timestamp()
        interval_logs_tpcds = get_yarn_logs_interval(yarn_logs, start_time_tpcds, end_time_tpcds)
        start_time_incre, end_time_incre = get_query_incre_log_timestamp()
        interval_logs_incre = get_yarn_logs_interval(yarn_logs, start_time_incre, end_time_incre)
        interval_logs = dict(interval_logs_tpcds, **interval_logs_incre)
        print(f"yarn logs (1)between {start_time_tpcds} and {end_time_tpcds} (2)between {start_time_incre} and {end_time_incre}\n{interval_logs}")
    else:
        start_time, end_time = get_query_log_timestamp()
        interval_logs = get_yarn_logs_interval(yarn_logs, start_time, end_time)
        print(f"yarn logs between {start_time} and {end_time}\n{interval_logs}")

    # 遍历所有的日志
    for app_id, value in interval_logs.items():
        # 获取一个query的执行计划
        hdfs_json_file = f"{hdfs_output_path}/{app_id}.json"
        original_query, node_metrics, physical_plan, dot_metrics, materialized_views = parse_json_logs(
            hdfs_json_file)
        if "MATERIALIZED VIEW" in original_query:
            print("this is physical plan of create/drop materialzed view")
            continue
        if materialized_views != "":
            print("warn: [build_plan_trees_new_SP] not a common query but a q-mv query")
            continue
        if original_query == "" or node_metrics == "" or physical_plan == "" or dot_metrics == "":
            print(app_id)
            print("warn: [build_plan_trees_new_SP] all 4 file must exists!")
            continue
        print(app_id)

        # 将query使用MD5编码
        queryMD5Id = sql2md5(original_query)
        # 将MD5值和原始sql的对应关系写入文件
        with open("./resources/data/md5-query.csv", "a+", newline="") as f:
            csv_writer = csv.writer(f, dialect="excel")
            csv_writer.writerow([str(queryMD5Id), original_query])

        # 将四个文件写入临时文件
        tmpPath = "resources/tmp/"
        os.makedirs(tmpPath, exist_ok=True)
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

        queryId, planTrees, tableField = buildOnePlanTree_SP(tmpQueryPath, tmpPlanPath, tmpDotPath, tmpNodePath, {})
        if len(planTrees) == 0:
            continue
        # 使用MD5
        if DataType.Q == dataType or DataType.MV == dataType:
            key = queryMD5Id

        # json_seq: 列名，常量，变量，输入, 字符串等编码（算子，参数，连接条件）, dict, {seq: _, cost: _, cardinality: _, time: _}
        # json_seq里面的sql：list, e.g. [('Compare', 1), ('=', 1), ('customer_address.ca_address_sk', 1)]
        # static_seq: 成本，基数（行数），主要是时间，pg里面有cost, list, e.g. [[333.0, 0.0], [3144.0, 0.0]
        # keywords: 表名和列名
        json_seq, static_seq, keywords = parseQuery_SP(planTrees[-1], needInput)

        key = queryMD5Id
        q_dict[key] = (json_seq, static_seq, keywords)
        if DataType.Q_MV == dataType:
            key = "-".join(key)
        # appendCSVFile([[key, json_seq['cost']]], dataType)

    return q_dict

def parse_qmv_csv_data_SP(dataType, isInc=False):
    q_dict = {}
    with open("./resources/data/query-mv-topdata.csv") as f:
        for line in f:
            print(f)



def parse_qmv_data_SP(dataType, isInc=False, is_incre=False):

    q_dict = {}
    # 1.调用接口获取日志信息
    # 几个重要的路径
    hdfs_output_path = get_hdfs_outputpath()
    logparser_jar_path = get_logparser_path()
    hdfs_spark_history_path = get_hdfs_inputpath()
    cache_path = get_cache_path()
    if is_incre:
        start_time, end_time = get_query_mv_incre_log_timestamp()
    else:
        start_time, end_time = get_query_mv_log_timestamp()
    # 获取所有的日志信息
    yarn_logs = get_all_yarn_logs()
    interval_logs = get_yarn_logs_interval(yarn_logs, start_time, end_time)
    print(f"yarn logs between {start_time} and {end_time}\n{interval_logs}")
    # 获取所有的日志
    if isInc:
        for app_id, value in interval_logs.items():
            generate_json_logs(logparser_jar_path, hdfs_spark_history_path, app_id, hdfs_output_path, cache_path)

    # 遍历所有的日志
    for app_id, value in interval_logs.items():
        # 获取一个query的执行计划
        hdfs_json_file = f"{hdfs_output_path}/{app_id}.json"
        original_query, node_metrics, physical_plan, dot_metrics, materialized_views = parse_json_logs(
            hdfs_json_file)
        if "MATERIALIZED VIEW" in original_query:
            print("this is physical plan of create/drop materialzed view")
            continue
        if materialized_views == "":
            print("warn: [parse_qmv_data_SP] not a q-mv query but a common query")
            continue
        if original_query == "" or node_metrics == "" or physical_plan == "" or dot_metrics == "":
            print(app_id)
            print("warn: [parse_qmv_data_SP] all 4 file must exists!")
            continue
        print(app_id)

        # 将query使用MD5编码
        queryMD5Id = sql2md5(original_query)
        # 将MD5值和原始sql的对应关系写入文件
        with open("./resources/data/md5-query.csv", "a+", newline="") as f:
            csv_writer = csv.writer(f, dialect="excel")
            csv_writer.writerow([str(queryMD5Id), original_query])

        # 将四个文件写入临时文件
        tmpPath = "resources/tmp/"
        os.makedirs(tmpPath, exist_ok=True)
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

        queryId, planTrees, tableField = build_encode_qmv_one_planTree_SP(tmpQueryPath, tmpPlanPath, tmpDotPath, tmpNodePath, False)
        if len(planTrees) == 0:
            continue

        # q-mv不需要编码
        # json_seq, static_seq, keywords = parseQuery_SP(planTrees[-1], needInput)

        # 处理materialized_views是list
        # materialized_views_list = []
        # viewList = materialized_views.split(";")
        # for view in viewList:
        #     if view.find(".") != -1:
        #         view = view.split(".")[1]
        #     view = view.strip()
        #     materialized_views_list.append(view)

        key = (queryMD5Id, materialized_views)
        q_dict[str(key)] = planTrees[-1].totalTime

    return q_dict

def parse_candidate_qmv_data_SP():
    mapping_path = getCandidateQMVMappings()
    query_mv_mapping = pd.read_csv(mapping_path)
    query_mv_mapping.columns = ['query_sql', "mv_id"]
    q_dict = {}
    for index, row in query_mv_mapping.iterrows():
        q_dict[(str(sql2md5(row['query_sql'])), getDatabase() + ".mv" + str(row['mv_id']))] = 0
    return q_dict


# 暂时使用此方法：99 * 20
def parseQMVData_SP(dataTypeQ, dataTypeMV, isInc=False):
    typePathQ = getRawPath(dataTypeQ, isInc)
    typePathMV = getRawPath(dataTypeMV, isInc)
    if typePathQ is None or typePathMV is None:
        return

    q_mv_dict = {}

    planPathQ = typePathQ + "/plan/"
    planPathMV = typePathMV + "/plan/"
    fileNamesQ = os.listdir(planPathQ)
    fileNamesMV = os.listdir(planPathMV)
    for fileQ in fileNamesQ:
        for fileMV in fileNamesMV:
            planFileQ = planPathQ + fileQ
            planFileMV = planPathMV + fileMV
            keyQ, extQ = os.path.splitext(os.path.basename(planFileQ))
            keyMV, extMV = os.path.splitext(os.path.basename(planFileMV))
            q_mv_dict[(keyQ, keyMV)] = 1.0

    return q_mv_dict


def parseQuery_SP(planTree, needInput=False):
    cost = planTree.totalTime
    cardinality = 0
    # json 2 class sequence
    seq, _, static_seq = parseTreeSeq_SP(planTree)
    seqs = PlanInSeq(seq, cost, cardinality)
    js = json.loads(class2json(seqs)) # js: {seq, cost, cardinality, time}, seq是操作序列, e.g. PG Comparasion object
    json_seq = json2seq(js) # {seq, cost, cardinality, time}, seq里面代表了每个关键词出现的次数, e.g [('Compare', 1), ('=', 1)]

    if needInput is True:
        input_keywords = getKeywordOfTables_SP(planTree.filterNode.tableList) # 解析schema得到column name，作为keyword
        input_keywords += planTree.filterNode.tableList
    else:
        input_keywords = None

    return json_seq, static_seq, input_keywords


# 从plan转换回来的操作序列、操作耗费的时间和涉及的行数、表名和列名
def parseTreeSeq_SP(root):
    # 遍历plan树，获取操作序列
    sequence = []
    join_conditions = []
    static_sequence = []

    def parseNodeSeq(node):
        data = FilterNode([])
        # condition
        if hasattr(node.filterNode, "joinFilterList") and hasattr(node.filterNode, "commonFilterList"):
            join_conditions.append(node.filterNode.joinFilterList)  # 没用到
            join_conditions.append(node.filterNode.commonFilterList) # 没用到
            if node.filterNode.joinFilterList != [] or node.filterNode.commonFilterList != []:
                # 将condition处理，因为之前已经处理过，这里不再处理
                condSeq = []
                filterList = list(set(node.filterNode.joinFilterList) | set(node.filterNode.commonFilterList))
                for condition in filterList:
                    try:
                        condSeq += (pre2seq_SP(condition, {}, None, None))
                    except:
                        print(condition)
                data.condition = condSeq
        # cost
        static_sequence.append([node.totalTime, 0.0])
        # squence
        sequence.append(data)
        # 遍历
        if node.left is not None:
            parseNodeSeq(node.left)
        if node.right is not None:
            parseNodeSeq(node.right)

    # 执行
    if root is not None:
        parseNodeSeq(root)

    return sequence, join_conditions, static_sequence


def getKeywordOfTables_SP(table_list):
    table_set = list(set(table_list))
    res = []
    for table in table_set:
        t = table.upper()
        if t.find(" AS ") == -1:
            res += [t + "." + key for key in g_table.data[t]]
        else:
            table, aliasTable = t.split(" AS ")[0:2]
            table, aliasTable = table.strip(), aliasTable.strip()
            res += [aliasTable + "." + key for key in g_table.data[table]]

    return res


# seq编码，写成类似PG Comparasion的object类
def pre2seq_SP(predicates, alias2table, relation_name, index_name):
    pr = remove_invalid_tokens_SP(predicates)
    pr = pr.replace("''", " ")
    p = pypred.Predicate(pr)
    try:
        predicates = predicates2seq(p.description().strip('\n').split('\n'), alias2table, relation_name, index_name)
    except:
        predicates = None
    return predicates

def remove_invalid_tokens_SP(predicate):
    x = re.sub(r"([\w.]+)[ ]*not like[ ]*\'([%()\- :\w]+)\'", r"(\1 = '__NOTLIKE__\2')", predicate)
    x = re.sub(r"([\w.]+)[ ]*like[ ]*\'([%()\- :\w]+)\'", r"(\1 = '__LIKE__\2')", x)
    x = re.sub(r"([\w.]+)[ ]*LIKE[ ]*\'([%()\- :\w]+)\'", r"(\1 = '__LIKE__\2')", x)
    x = re.sub(r"([\w.]+)[ ]*!=[ ]*\'([%()\- :\w]*)\'", r"(\1 = '__NOTEQUAL__\2')", x)
    x = remove_takons_in(x)

    # x = re.sub(r'\(\(([a-zA-Z_]+)\)::text !~~ \'(((?!::text).)*)\'::text\)', r"(\1 = '__NOTLIKE__\2')", x)
    # x = re.sub(r'\(\(([a-zA-Z_]+)\)::text <> \'(((?!::text).)*)\'::text\)', r"(\1 = '__NOTEQUAL__\2')", x)
    # x = re.sub(r'\(([a-zA-Z_]+) ~~ \'(((?!::text).)*)\'::text\)', r"(\1 = '__LIKE__\2')", x)
    # x = re.sub(r'\(([a-zA-Z_]+) !~~ \'(((?!::text).)*)\'::text\)', r"(\1 = '__NOTLIKE__\2')", x)
    # x = re.sub(r'\(([a-zA-Z_]+) <> \'(((?!::text).)*)\'::text\)', r"(\1 = '__NOTEQUAL__\2')", x)
    # x = re.sub(r'(\'[^\']*\')::[a-z_]+', r'\1', x)
    # x = re.sub(r'\(([^\(]+)\)::[a-z_]+', r'\1', x)
    # x = re.sub(r'\(([a-z_0-9A-Z\-]+) = ANY \(\'(\{.+\})\'\[\]\)\)', r"(\1 = '__ANY__\2')", x)
    # print(predicate,x)
    return x


def remove_takons_in(predicate):
    if predicate.find(" in ") == -1 and predicate.find(" IN ") == -1:
        return predicate
    patten = r"([\w.]+)[ ]*[Ii][Nn][ ]*\(([\w'(),:\- ]+)\)"
    key = re.findall(patten, predicate)
    if len(key) == 0:
        return predicate
    leftValue, rightCondition = key[0][0], key[0][1]
    rightValueList = rightCondition.split(",")
    condition = "("
    for rightValue in rightValueList:
        rightValue = rightValue.strip()
        condition = condition + leftValue + " = " + rightValue + " or "
    condition = condition[:-4] + ")"
    return condition


if __name__ == "__main__":
    buildData_SP()