import json
from parsePlanJson import *
# from encodeFeatures import *
from joinCandidate import *
import copy


def recoverCondition(condition):
    # print("pre--condition:{}".format(condition))
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








def buildData_CH():
    q_dict = parseData_CH(DataType.Q)
    mv_dict = parseData_CH(DataType.MV)
    q_mv_dict = parseData_CH(DataType.Q_MV)
    # print(q_dict)
    # return
    print("q_dict:{},mv_dict:{},q_mv_dict:{}".format(len(q_dict), len(mv_dict), len(q_mv_dict)))

    keywords = []
    r_l = []
    for q in q_dict.values():
        q_expression = q[0]['seq']
        keywords += extractKeywords(q_expression)
        keywords += q[2]
    for mv in mv_dict.values():
        mv_expression = mv[0]['seq']
        keywords += extractKeywords(mv_expression)
        keywords += mv[2]


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
        r_l.append((x, y, key))

    return r_l, list(set(keywords))


def buildData_CH_Inc():
    q_dict = parseData_CH(DataType.Q, True)
    mv_dict = parseData_CH(DataType.MV, True)
    q_mv_dict = parseData_CH(DataType.Q_MV, True)
    # print(q_dict)
    # return
    print("q_dict:{},mv_dict:{},q_mv_dict:{}".format(len(q_dict), len(mv_dict), len(q_mv_dict)))

    keywords = []
    r_l = []
    for q in q_dict.values():
        q_expression = q[0]['seq']
        keywords += extractKeywords(q_expression)
        keywords += q[2]
    for mv in mv_dict.values():
        mv_expression = mv[0]['seq']
        keywords += extractKeywords(mv_expression)
        keywords += mv[2]

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
        r_l.append((x, y, key))

    return r_l, list(set(keywords))


def buildData_Test():
    q_dict = parseData_CH(DataType.Q)
    mv_dict = parseData_CH(DataType.MV)
    q_mv_dict = loadQueryMap()
    print("q_dict:{},mv_dict:{},q_mv_dict:{}".format(len(q_dict), len(mv_dict), len(q_mv_dict)))

    keywords = []
    r_l = []
    for q in q_dict.values():
        q_expression = q[0]['seq']
        keywords += extractKeywords(q_expression)
        keywords += q[2]
    for mv in mv_dict.values():
        mv_expression = mv[0]['seq']
        keywords += extractKeywords(mv_expression)
        keywords += mv[2]

    for key in q_mv_dict:
        y = 0
        key1, key2 = key.split("-")
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
        r_l.append((x, y, key))

    return r_l, list(set(keywords))
# buildData_CH()
