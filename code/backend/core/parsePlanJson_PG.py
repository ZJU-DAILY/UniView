import os
import json
import re
# import pandas as pd
import pypred
import numpy as np
from z3 import *
# from procSQL import keyword_columns
from procCondition_PG import *
from procSQL import *
from basePlan import *
#
# DATA_TYPE_Q = 0
# DATA_TYPE_MV = 1
# DATA_TYPE_Q_MV = 2

ms2s = 1.e-3  # json文件中的时间单位是ms，转换为s


solver = Solver()


class Set(object):
    def __init__(self, command):
        self.node_type = 'SetOp'
        self.command = command

    def __str__(self):
        return 'SetOp:' + self.command


class WindowAgg(object):
    def __init__(self):
        self.node_type = 'WindowAgg'

    def __str__(self):
        return 'WindowAgg'


class Gather(object):
    def __init__(self):
        self.node_type = 'Gather'

    def __str__(self):
        return 'Gather'


class Unique(object):
    def __init__(self):
        self.node_type = 'Unique'

    def __str__(self):
        return 'Unique'


class Append(object):
    def __init__(self):
        self.node_type = 'Append'

    def __str__(self):
        return 'Append'


class Limit(object):
    def __init__(self):
        self.node_type = 'Limit'

    def __str__(self):
        return 'Limit'


class Materialize(object):
    def __init__(self):
        self.node_type = 'Materialize'

    def __str__(self):
        return 'Materialize'


class Aggregate(object):
    def __init__(self, strategy, keys):
        self.node_type = 'Aggregate'
        self.strategy = strategy
        self.group_keys = keys

    def __str__(self):
        return 'Aggregate ON: ' + ','.join(self.group_keys)


class Sort(object):
    def __init__(self, sort_keys):
        self.sort_keys = sort_keys
        self.node_type = 'Sort'

    def __str__(self):
        return 'Sort by: ' + ','.join(self.sort_keys)


class Group(object):
    def __init__(self, group_keys):
        self.group_keys = group_keys
        self.node_type = 'Group'

    def __str__(self):
        return 'Group by: ' + ','.join(self.group_keys)


class Hash(object):
    def __init__(self):
        self.node_type = 'Hash'

    def __str__(self):
        return 'Hash'


class Join(object):
    def __init__(self, node_type, condition_seq):
        self.node_type = node_type
        if condition_seq is None:
            self.condition = []
        else:
            self.condition = condition_seq

    def __str__(self):
        return 'Join ON ' + ','.join([str(i) for i in self.condition])


class Scan(object):
    def __init__(self, node_type, condition_seq_filter, condition_seq_index, relation_name, index_name):
        self.node_type = node_type
        if condition_seq_filter is None:
            self.condition_filter = []
        else:
            self.condition_filter = condition_seq_filter
        self.condition_index = condition_seq_index
        self.relation_name = relation_name
        self.index_name = index_name

    def __str__(self):
        return 'Scan ' + (
            self.relation_name if (self.relation_name is not None) else self.index_name) + ' ON ' + ' ,'.join(
            [str(i) for i in self.condition_filter]) + '; ' + ' ,'.join([str(i) for i in self.condition_index])


class BitmapCombine(object):
    def __init__(self, operator):
        self.node_type = operator

    def __str__(self):
        return self.node_type


class Result(object):
    def __init__(self):
        self.node_type = 'Result'

    def __str__(self):
        return 'Result'


class Operator(object):
    def __init__(self, opt, type='Bool'):
        self.op_type = type
        self.operator = opt

    def __str__(self):
        return 'Operator: ' + self.operator


class Comparison(object):
    def __init__(self, opt, left_value, right_value):
        self.op_type = 'Compare'
        self.operator = opt
        self.left_value = left_value
        self.right_value = right_value

    def __str__(self):
        return 'Comparison: ' + self.left_value + ' ' + self.operator + ' ' + self.right_value


class Computation(object):
    def __init__(self, opt, left_value, right_value):
        self.op_type = 'Computation'
        self.operator = opt
        self.left_value = left_value
        self.right_value = right_value

    def __str__(self):
        return 'Computation: ' + self.left_value + ' ' + self.operator + ' ' + self.right_value


class InnerFunction(object):
    def __init__(self, opt, left_value, mid_value, right_value):
        self.op_type = 'InnerFunction'
        self.operator = opt
        self.left_value = left_value
        self.mid_value = mid_value
        self.right_value = right_value

    def __str__(self):
        return 'InnerFunction: ' + self.operator + '(' + self.left_value + self.mid_value + self.right_value + ')'


class PlanInSeq(object):
    def __init__(self, seq, cost, cardinality, time=None):
        self.seq = seq
        self.cost = cost
        self.cardinality = cardinality
        self.time = time


def buildAlias2Table(root, alias2table):
    if 'Relation Name' in root and 'Alias' in root:
        alias2table[root['Alias']] = root['Relation Name']
    if 'Plans' in root:
        for child in root['Plans']:
            buildAlias2Table(child, alias2table)


def getSubPlan(root):
    results = []
    if 'Actual Rows' in root and 'Actual Total Time' in root and root['Actual Rows'] > 0:
        results.append((root, root['Actual Total Time'], root['Actual Rows']))
    if 'Plans' in root:
        for plan in root['Plans']:
            results += getSubPlan(plan)
    return results


def getPlanSum(root, use_cost=False, return_time=False):
    if not use_cost:
        if return_time:
            return root, root['Actual Total Time'], root['Actual Rows'], root['Actual Total Time']
        else:
            return root, root['Actual Total Time'], root['Actual Rows']
    else:
        if return_time:
            return root, root['Total Cost'], root['Plan Rows'], root['Actual Total Time']
        else:
            return root, root['Total Cost'], root['Plan Rows']


def class2json(instance):
    if instance is None:
        return json.dumps({})
    else:
        return json.dumps(toDict(instance))


def toDict(obj, classkey=None):
    if isinstance(obj, dict):
        data = {}
        for (k, v) in obj.items():
            data[k] = toDict(v, classkey)
        return data
    elif hasattr(obj, "_ast"):
        return toDict(obj._ast())
    elif hasattr(obj, "__iter__") and not isinstance(obj, str):
        return [toDict(v, classkey) for v in obj]
    elif hasattr(obj, "__dict__"):
        data = dict([(key, toDict(value, classkey))
                     for key, value in obj.__dict__.items()
                     if not callable(value) and not key.startswith('_')])
        if classkey is not None and hasattr(obj, "__class__"):
            data[classkey] = obj.__class__.__name__
        return data
    else:
        return obj


def json2seq(root_json):
    r_d = {}
    for key, value in root_json.items():
        if key == "seq" and isinstance(value, list):
            res = []
            for v in value:
                # join_type = True if isinstance(v,dict) and 'Join' in v['node_type'] else False
                res.append(toseq(v))
            r_d[key] = res
        else:
            r_d[key] = value
    return r_d


def toseq(node_json):
    # print(type(node_json))
    if node_json is None:
        return None, 1
    if isinstance(node_json, list):
        res = []
        for child_json in node_json:
            child_seq = toseq(child_json)
            if isinstance(child_seq, list):
                res += child_seq
            else:
                res.append(child_seq)
        return res
    if isinstance(node_json, dict):
        res = []
        for key in node_json:
            if key in ['cost', 'type']:
                continue
            value = node_json[key]
            if key == "attribute":
                for v in value:
                    res.append((v.strip(), 1))
            elif key == 'condition':
                if value is None:
                    continue
                res += toseq(value)
            else:
                value = toseq(node_json[key])
                if isinstance(value, list):
                    res += value
                elif isinstance(value, tuple):
                    res.append(value)
                else:
                    tp = 2 if (
                                      key == "right_value" or key == "left_value") and value.upper() not in keyword_columns else 1
                    res.append((value.strip(), tp))
        return res
    return node_json


# ===========================================================
# 返回当前层级及子层级的操作&参数、连接条件、执行时间&返回行数、涉及的表格
# ===========================================================
def getPlanSeq(root, alias2table):
    sequence = []
    join_conditions = []
    static_sequence = []
    input_tables = []
    node, join_condition = getNodeinfo(root, alias2table)
    if join_condition is not None:
        join_conditions += join_condition
    sequence.append(node)
    if 'Actual Rows' in root and 'Actual Total Time' in root \
            and root['Actual Rows'] > 0 and root['Actual Total Time'] > 0:
        cost = np.log(root['Actual Total Time'])  # scale-down cost (actual cost is too big...) for building features
        static_sequence.append([cost, root['Actual Rows']])
    else:
        static_sequence.append([0, 0])
    if 'Plans' in root:
        for plan in root['Plans']:
            next_sequence, next_join_conditions, next_static_sequence, next_input_table = getPlanSeq(plan, alias2table)
            sequence += next_sequence
            static_sequence += next_static_sequence
            join_conditions += next_join_conditions
            input_tables += next_input_table
    else:
        if 'Relation Name' in root:
            input_tables.append(root['Relation Name'])
    return sequence, join_conditions, static_sequence, input_tables




def nestedPreSeq(s, res, alias2table, relationName, indexName, level=0):
    sequence = []
    i = 0
    # level = 0
    while i < len(res):
        token = res[i]
        if not s.isOper(token):
            if len(sequence) != 0:
                return sequence, i
            seq_value = res[i]
            if re.match(r'^[a-z][a-z0-9_]*\.[a-z][a-z0-9_]*$', seq_value) is not None:
                seq_value = changeAlias2Table(seq_value, alias2table)
            else:
                if relationName is None:
                    if indexName is not None:
                        relation = indexName.replace(seq_value + '_', '')
                        seq_value = relation + '.' + seq_value
                else:
                    relation = relationName
                    seq_value = relation + '.' + seq_value
            i = i + 1
            return seq_value, i

        if token == "(":
            level += 1
            i = i + 1
            continue
        if token == ")":
            level -= 1
            if level != 0:
                break
            i = i + 1
            continue
        operOder = s.getOperOrder(token)
        if operOder in [6, 7, 8]:
            sequence.append(Operator(token))
            i = i + 1
            continue
        if operOder in [2, 3, 4]:
            opToken = token
            i = i + 1
            left_seq,  cnt = nestedPreSeq(s, res[i:], alias2table, relationName, indexName, level)
            i = i + cnt
            right_seq, cnt = nestedPreSeq(s, res[i:], alias2table, relationName, indexName, level)
            i = i + cnt

            if operOder == 4:
                sequence.append(Comparison(opToken, left_seq, right_seq))
            else:
                sequence.append(Computation(opToken, left_seq, right_seq))
        if operOder in [5]:
            opToken = token
            i = i + 1
            left_seq,  cnt = nestedPreSeq(s, res[i:], alias2table, relationName, indexName, level)
            i = i + cnt
            mid_seq = None
            right_seq = None
            if s.isTripOper(token):
                mid_seq,  cnt = nestedPreSeq(s, res[i:], alias2table, relationName, indexName, level)
                i = i + cnt
            if not s.isSingleOper(token):
                right_seq, cnt = nestedPreSeq(s, res[i:], alias2table, relationName, indexName, level)
                i = i + cnt
            sequence.append(InnerFunction(opToken, left_seq, mid_seq, right_seq))
            # return sequence, i
            # i = i + 1
    return sequence, i


def mid2PreSeq(condition, alias2table, relationName, indexName):
    sequence = []
    data = condition.split()
    s = Solution()
    # t = s.createTree()
    t = s.InExpTree(data)
    res = []
    s.preOrderTree(t, res)
    sequence, _ = nestedPreSeq(s, res, alias2table, relationName, indexName)
    # print(sequence)
    return sequence
    # res = map(str, res)
    # s.PreExpTree()


# '''
# 获取本层级的操作信息：操作&参数、连接条件
# '''
def getNodeinfo(node, alias2table):
    relationName, indexName, cteName = None, None, None
    if 'Relation Name' in node:
        relationName = node['Relation Name']
    if 'Index Name' in node:
        indexName = node['Index Name']
    if 'CTE Name' in node:
        cteName = node['CTE Name']
    nodeType = node['Node Type']

    if 'Gather' == nodeType:
        return  Gather(), None
    if 'SetOp' == nodeType:
        return Set(node['Command']), None
    if 'WindowAgg' == nodeType:
        return WindowAgg(), None
    if 'Unique' == nodeType:
        return Unique(), None
    if 'Append' == nodeType:
        return Append(), None
    if 'Limit' == nodeType:
        return Limit(), None
    if 'Materialize' == nodeType:
        return Materialize(), None
    if 'Hash' == nodeType:
        return Hash(), None
    if 'Sort' == nodeType:
        keys = [changeAlias2Table(key, alias2table) for key in node['Sort Key']]
        return Sort(keys), None
    if 'Group' == nodeType:
        keys = [changeAlias2Table(key, alias2table) for key in node['Group Key']]
        return Group(keys), None
    if 'BitmapAnd' == nodeType:
        return BitmapCombine('BitmapAnd'), None
    if 'BitmapOr' == nodeType:
        return BitmapCombine('BitmapOr'), None
    if 'Result' == nodeType:
        return Result(), None
    if 'Hash Join' == nodeType:
        return Join('Hash Join', pre2seq(node["Hash Cond"], alias2table, relationName, indexName)), None
    if 'Merge Join' == nodeType:
        return Join('Merge Join', pre2seq(node["Merge Cond"], alias2table, relationName, indexName)), None
    if 'Nested Loop' == nodeType:
        if 'Join Filter' in node:
            condition = pre2seq(node['Join Filter'], alias2table, relationName, indexName)
        else:
            condition = []
        return Join('Nested Loop', condition), None
    if 'Aggregate' == nodeType:
        if 'Group Key' in node:
            keys = [changeAlias2Table(key, alias2table) for key in node['Group Key']]
        else:
            keys = []
        return Aggregate(node['Strategy'], keys), None
    if 'Seq Scan' == nodeType:
        if 'Filter' in node:
            condition_seq_filter = pre2seq(node['Filter'], alias2table, relationName, indexName)
        else:
            condition_seq_filter = []
        condition_seq_index = []
        return Scan('Seq Scan', condition_seq_filter, condition_seq_index, relationName, indexName), None
    if 'Bitmap Heap Scan' == nodeType:
        if 'Filter' in node:
            condition_seq_filter = pre2seq(node['Filter'], alias2table, relationName, indexName)
        else:
            condition_seq_filter = []
        condition_seq_index = []
        return Scan('Bitmap Heap Scan', condition_seq_filter, condition_seq_index, relationName, indexName), None
    if 'Index Scan' == nodeType:
        if 'Filter' in node:
            condition_seq_filter = pre2seq(node['Filter'], alias2table, relationName, indexName)
        else:
            condition_seq_filter = []
        if 'Index Cond' in node:
            condition_seq_index = pre2seq(node['Index Cond'], alias2table, relationName, indexName)
            if condition_seq_index is None:
                condition_seq_index = []
        else:
            condition_seq_index = []
        if len(condition_seq_index) == 1 \
                and re.match(r'[a-zA-Z]+', condition_seq_index[0].right_value) is not None:
            return Scan('Index Scan', condition_seq_filter, condition_seq_index, relationName, indexName), \
                   condition_seq_index
        else:
            return Scan('Index Scan', condition_seq_filter, condition_seq_index, relationName, indexName), None
    if 'Bitmap Index Scan' == nodeType:
        if 'Index Cond' in node:
            condition_seq_index = pre2seq(node['Index Cond'], alias2table, relationName, indexName)
        else:
            condition_seq_index = []
        condition_seq_filter = [], None
        if len(condition_seq_index) == 1 \
                and re.match(r'[a-zA-Z]+', condition_seq_index[0].right_value) is not None:
            return Scan('Bitmap Index Scan', condition_seq_filter, condition_seq_index, relationName, indexName), \
                   condition_seq_index
        else:
            return Scan('Bitmap Index Scan', condition_seq_filter, condition_seq_index, relationName, indexName), None
    if 'Index Only Scan' == nodeType:
        if 'Index Cond' in node:
            condition_seq_index = pre2seq(node['Index Cond'], alias2table, relationName, indexName)
        else:
            condition_seq_index = []
        condition_seq_filter = []
        if len(condition_seq_index) == 1 \
                and re.match(r'[a-zA-Z]+', condition_seq_index[0].right_value) is not None:
            return Scan('Index Only Scan', condition_seq_filter, condition_seq_index, relationName,
                        indexName), condition_seq_index
        else:
            return Scan('Index Only Scan', condition_seq_filter, condition_seq_index, relationName, indexName), None
    if 'CTE Scan' == nodeType:
        if 'Filter' in node:
            condition_seq_filter = pre2seq(node['Filter'], alias2table, relationName, indexName)
        else:
            condition_seq_filter = []
        condition_seq_index = []
        return Scan('CTE Scan', condition_seq_filter, condition_seq_index, cteName, indexName), None
    if 'Subquery Scan' == nodeType:
        if 'Filter' in node:
            condition_seq_filter = pre2seq(node['Filter'], alias2table, relationName, indexName)
        else:
            condition_seq_filter = []
        condition_seq_index = []
        return Scan('Subquery Scan', condition_seq_filter, condition_seq_index, "", indexName), None
    raise Exception('Unsupported Node Type: ' + nodeType)
    return None, None


def getSumNodeinfo(node, alias2table):
    newNode = {'Node Type': node['Node Type']}
    if 'Relation Name' in node:
        newNode['Relation Name'] = node['Relation Name']
    if 'Index Name' in node:
        newNode['Index Name'] = node['Index Name']
    nodeType = node['Node Type']
    if 'Sort' == nodeType:
        keys = [changeAlias2Table(key, alias2table) for key in node['Sort Key']]
        newNode['Sort Key'] = keys
    if 'Hash Join' == nodeType:
        newNode["Hash Cond"] = node["Hash Cond"]
    if 'Merge Join' == nodeType:
        newNode["Merge Cond"] = node["Merge Cond"]
    if 'Nested Loop' == nodeType:
        if 'Join Filter' in node:
            newNode["Join Filter"] = node["Join Filter"]
    if 'Aggregate' == nodeType:
        if 'Group Key' in node:
            keys = [changeAlias2Table(key, alias2table) for key in node['Group Key']]
            newNode['Group Key'] = keys
    if 'Seq Scan' == nodeType:
        if 'Filter' in node:
            newNode['Filter'] = node["Filter"]
    if 'Bitmap Heap Scan' == nodeType:
        if 'Filter' in node:
            newNode['Filter'] = node["Filter"]
    if 'Index Scan' == nodeType:
        if 'Filter' in node:
            newNode['Filter'] = node["Filter"]
        if 'Index Cond' in node:
            newNode['Index Cond'] = node["Index Cond"]
    if 'Bitmap Index Scan' == nodeType:
        if 'Index Cond' in node:
            newNode['Index Cond'] = node["Index Cond"]
    if 'Index Only Scan' == nodeType:
        if 'Index Cond' in node:
            newNode['Index Cond'] = node["Index Cond"]
    return newNode


def pre2seq(predicates, alias2table, relation_name, index_name):
    if not predicates:
        return predicates
    pr = remove_invalid_tokens(predicates)
    pr = pr.replace("''", " ").replace("<>", "!=")
    p = pypred.Predicate(pr)
    try:
        # print("condition:{}".format(pr))
        predicates = predicates2seq(p.description().strip('\n').split('\n'), alias2table, relation_name, index_name)
    except Exception as e:
        # print("Cannot process this condition:{}".format(pr))
        pr = pr.replace("(", " ( ").replace(")", " )")
        predicates = mid2PreSeq(pr, alias2table, relation_name, index_name)
        # predicates = None
    return predicates


# def remove_invalid_tokens(predicate):
#     x = re.sub(r'\(\(([a-zA-Z_]+)\)::text ~~ \'(((?!::text).)*)\'::text\)', r"(\1 = '__LIKE__\2')", predicate)
#     x = re.sub(r'\(\(([a-zA-Z_]+)\)::text !~~ \'(((?!::text).)*)\'::text\)', r"(\1 = '__NOTLIKE__\2')", x)
#     x = re.sub(r'\(\(([a-zA-Z_]+)\)::text <> \'(((?!::text).)*)\'::text\)', r"(\1 = '__NOTEQUAL__\2')", x)
#     x = re.sub(r'\(([a-zA-Z_]+) ~~ \'(((?!::text).)*)\'::text\)', r"(\1 = '__LIKE__\2')", x)
#     x = re.sub(r'\(([a-zA-Z_]+) !~~ \'(((?!::text).)*)\'::text\)', r"(\1 = '__NOTLIKE__\2')", x)
#     x = re.sub(r'\(([a-zA-Z_]+) <> \'(((?!::text).)*)\'::text\)', r"(\1 = '__NOTEQUAL__\2')", x)
#     x = re.sub(r'(\'[^\']*\')::[a-z_]+', r'\1', x)
#     x = re.sub(r'\(([^\(]+)\)::[a-z_]+', r'\1', x)
#     # x = re.sub(r'\(([a-z_0-9A-Z\-]+) = ANY \(\'(\{.+?\})\'\[\]\)\)', r"(\1 = '__ANY__\2')", x)
#     x = re.sub(r'\(([a-z_0-9A-Z\-\(\),. ]+) = ANY \(\'(\{.+?\})\'\[\]\)\)', r"(\1 = '__ANY__\2')", x)
#     x = re.sub(r'\(SubPlan ([0-9]+)\)', r"(SubPlan_\1)", x)
#     x = re.sub(r'(\w*[:]*[ ]*)SubPlan ([0-9]+)', r"(SubPlan_\2 = 1)", x)
#     # print(predicate,x)
#     return x
#

def predicates2seq(preTree, alias2table, relationName, indexName):
    curLevel = -1
    curLine = 0
    sequence = []
    # print("preTree:{}".format(preTree))
    while curLine < len(preTree):
        opStr = preTree[curLine]
        level = len(re.findall(r'\t', opStr))
        opSeq = opStr.strip('\t').split()
        operator = opSeq[0]
        opType = opSeq[1]
        if level <= curLevel:
            for i in range(curLevel - level + 1):
                sequence.append(None)
        curLevel = level
        if 'operator' == opType:
            sequence.append(Operator(operator))
            curLine += 1
        elif 'comparison' == opType:
            operator = opSeq[0]
            curLine += 1
            opStr = preTree[curLine]
            opSeq = opStr.strip('\t').split()
            # left_type = opSeq[0]
            left_value = opSeq[1]
            curLine += 1
            opStr = preTree[curLine]
            opSeq = opStr.strip('\t').split()
            right_type = opSeq[0]
            # 数字
            if 'Number' == right_type:
                right_value = opSeq[1]
            # 字符
            elif 'Literal' == right_type:
                p = re.compile("Literal (.*) at line:")
                result = p.search(opStr)
                right_value = result.group(1)
            # 常数
            elif right_type == 'Constant':
                p = re.compile("Constant (.*) at line:")
                result = p.search(opStr)
                right_value = result.group(1)
            else:
                raise "Unsupport Value Type: " + right_type
            if re.match(r'^[a-z][a-z0-9_]*\.[a-z][a-z0-9_]*$', left_value) is not None:
                left_value = changeAlias2Table(left_value, alias2table)
            else:
                if relationName is None:
                    if indexName is not None:
                        relation = indexName.replace(left_value + '_', '')
                        left_value = relation + '.' + left_value
                else:
                    relation = relationName
                    left_value = relation + '.' + left_value
            if re.match(r'^[a-z][a-z0-9_]*\.[a-z][a-z0-9_]*$', right_value) is not None:
                right_value = changeAlias2Table(right_value, alias2table)
            sequence.append(Comparison(operator, left_value, right_value.strip('\'')))
            curLine += 1
    return sequence


# ===========================================================
# 如果是MV的json文件，则只需要返回本json文件表示的plan
# 如果是query的json文件，则需要返回所有的plan
# ===========================================================
def getSubPlans(root, alias2table):
    sequence = {}
    input_tables = []
    node, _ = getNodeinfo(root, alias2table)
    # subExpression = {}

    # if type(node).__name__ in ("Aggregate", "Join", "Scan"):
    sequence[type(node).__name__] = str(node)
    # else:
    #     sequence["subplan"] = [str(node)]

    if 'Plans' in root:
        for plan in root['Plans']:
            next_sequence, next_input_table = getSubPlans(plan, alias2table)
            if "subplan" in sequence:
                sequence["subplan"].append([next_sequence])
            else:
                sequence["subplan"] = [next_sequence]
            input_tables += next_input_table
    else:
        if 'Relation Name' in root:
            input_tables.append(root['Relation Name'])
    return sequence, input_tables


def getChildNode_PG(node, alias2table):
    relationName, indexName, cteName = None, None, None
    if 'Relation Name' in node:
        relationName = node['Relation Name']
    if 'Index Name' in node:
        indexName = node['Index Name']
    if 'CTE Name' in node:
        cteName = node['CTE Name']
    nodeType = node['Node Type']
    if 'SetOp' == nodeType:
        return Set(node['Command']), None
    if 'WindowAgg' == nodeType:
        return WindowAgg(), None
    if 'Unique' == nodeType:
        return Unique(), None
    if 'Append' == nodeType:
        return Append(), None
    if 'Limit' == nodeType:
        return Limit(), None
    if 'Materialize' == nodeType:
        return Materialize(), None
    if 'Hash' == nodeType:
        return Hash(), None
    if 'Sort' == nodeType:
        keys = [changeAlias2Table(key, alias2table) for key in node['Sort Key']]
        return Sort(keys), None
    if 'Group' == nodeType:
        keys = [changeAlias2Table(key, alias2table) for key in node['Group Key']]
        return Group(keys), None
    if 'BitmapAnd' == nodeType:
        return BitmapCombine('BitmapAnd'), None
    if 'BitmapOr' == nodeType:
        return BitmapCombine('BitmapOr'), None
    if 'Result' == nodeType:
        return Result(), None
    if 'Hash Join' == nodeType:
        return Join('Hash Join', pre2seq(node["Hash Cond"], alias2table, relationName, indexName)), None
    if 'Merge Join' == nodeType:
        return Join('Merge Join', pre2seq(node["Merge Cond"], alias2table, relationName, indexName)), None
    if 'Nested Loop' == nodeType:
        if 'Join Filter' in node:
            condition = pre2seq(node['Join Filter'], alias2table, relationName, indexName)
        else:
            condition = []
        return Join('Nested Loop', condition), None
    if 'Aggregate' == nodeType:
        if 'Group Key' in node:
            keys = [changeAlias2Table(key, alias2table) for key in node['Group Key']]
        else:
            keys = []
        return Aggregate(node['Strategy'], keys), None
    if 'Seq Scan' == nodeType:
        if 'Filter' in node:
            condition_seq_filter = pre2seq(node['Filter'], alias2table, relationName, indexName)
        else:
            condition_seq_filter = []
        condition_seq_index = []
        return Scan('Seq Scan', condition_seq_filter, condition_seq_index, relationName, indexName), None
    if 'Bitmap Heap Scan' == nodeType:
        if 'Filter' in node:
            condition_seq_filter = pre2seq(node['Filter'], alias2table, relationName, indexName)
        else:
            condition_seq_filter = []
        condition_seq_index = []
        return Scan('Bitmap Heap Scan', condition_seq_filter, condition_seq_index, relationName, indexName), None
    if 'Index Scan' == nodeType:
        if 'Filter' in node:
            condition_seq_filter = pre2seq(node['Filter'], alias2table, relationName, indexName)
        else:
            condition_seq_filter = []
        if 'Index Cond' in node:
            condition_seq_index = pre2seq(node['Index Cond'], alias2table, relationName, indexName)
            if condition_seq_index is None:
                condition_seq_index = []
        else:
            condition_seq_index = []
        if len(condition_seq_index) == 1 \
                and re.match(r'[a-zA-Z]+', condition_seq_index[0].right_value) is not None:
            return Scan('Index Scan', condition_seq_filter, condition_seq_index, relationName, indexName), \
                   condition_seq_index
        else:
            return Scan('Index Scan', condition_seq_filter, condition_seq_index, relationName, indexName), None
    if 'Bitmap Index Scan' == nodeType:
        if 'Index Cond' in node:
            condition_seq_index = pre2seq(node['Index Cond'], alias2table, relationName, indexName)
        else:
            condition_seq_index = []
        condition_seq_filter = [], None
        if len(condition_seq_index) == 1 \
                and re.match(r'[a-zA-Z]+', condition_seq_index[0].right_value) is not None:
            return Scan('Bitmap Index Scan', condition_seq_filter, condition_seq_index, relationName, indexName), \
                   condition_seq_index
        else:
            return Scan('Bitmap Index Scan', condition_seq_filter, condition_seq_index, relationName, indexName), None
    if 'Index Only Scan' == nodeType:
        if 'Index Cond' in node:
            condition_seq_index = pre2seq(node['Index Cond'], alias2table, relationName, indexName)
        else:
            condition_seq_index = []
        condition_seq_filter = []
        if len(condition_seq_index) == 1 \
                and re.match(r'[a-zA-Z]+', condition_seq_index[0].right_value) is not None:
            return Scan('Index Only Scan', condition_seq_filter, condition_seq_index, relationName,
                        indexName), condition_seq_index
        else:
            return Scan('Index Only Scan', condition_seq_filter, condition_seq_index, relationName, indexName), None
    if 'CTE Scan' == nodeType:
        if 'Filter' in node:
            condition_seq_filter = pre2seq(node['Filter'], alias2table, relationName, indexName)
        else:
            condition_seq_filter = []
        condition_seq_index = []
        return Scan('CTE Scan', condition_seq_filter, condition_seq_index, cteName, indexName), None
    if 'Subquery Scan' == nodeType:
        if 'Filter' in node:
            condition_seq_filter = pre2seq(node['Filter'], alias2table, relationName, indexName)
        else:
            condition_seq_filter = []
        condition_seq_index = []
        return Scan('Subquery Scan', condition_seq_filter, condition_seq_index, "", indexName), None
    raise Exception('Unsupported Node Type: ' + nodeType)
    return None, None


# 直接用执行计划的树，不用再新建树
def getTreeNode_PG(root, alias2table, clusters):
    input_tables = []
    curNode, _ = getNodeinfo(root, alias2table)
    # 如果有with子查询，该语句不做候选视图推荐
    if curNode.node_type in ('CTE Scan', 'Subquery Scan'):
        return None, None
    node = PlanTree(data=root)
    # sequence[type(node).__name__] = str(node)
    # else:

    if 'Plans' in root:
        for plan in root['Plans']:
            childNode, next_input_table = getTreeNode_PG(plan, alias2table, clusters)
            if childNode is None:
                # return None, input_tables
                continue
            getTreeConditions_PG(childNode, alias2table)
            input_tables += next_input_table
            childNode.ancestor = node

            if node.left is None:
                node.left = childNode
            else:
                node.right = childNode
    # else:
    if 'Relation Name' in root:
        input_tables.append(root['Relation Name'])

    # node["Tree"] = curNode
    for rel in list(set(input_tables)):
        node.relations[rel] = {}
    # node.relations = list(set(input_tables))
    if type(curNode).__name__ in ("Aggregate", "Join"):
    # if type(curNode).__name__ in ("Aggregate", "Join", "Scan"):
        clusters.append(node)
        getTreeConditions_PG(node, alias2table)
    return node, input_tables


def buildOnePlanTree_PG(sqlFile, jsonFile, analyze=True):
    global g_table
    f_open = open(jsonFile, "r", encoding='utf-8')
    plan = json.load(f_open)['Plan']
    f_open.close()

    alias2table = {}
    buildAlias2Table(plan, alias2table)

    clusters = []
    childNode, referredRels = getTreeNode_PG(plan, alias2table, clusters)
    if referredRels is not None:
        # 直接从SQL文件中读取每个表涉及的列名，放在build出来的tree结构中。在merge的时候，将这些key也merge一下，最终放在select后面
        referredKeys = getReferredKeys(alias2table, sqlFile, referredRels, g_table.data)
        for tree in clusters:
            for rel in tree.relations.keys():
                tree.referKeys[rel] = [value for value in referredKeys[rel]] # referredKeys[rel]
    return "", clusters


def canMerge_PG(clusterId, orgTree, newTree):
    # TODO: 如果orgTree是本次生成的候选视图，则他和newtree之间能合并就尽量合并
    # TODO：如果orgTree是历史的候选视图，orgTree包含newTree的情况认为可以合并，newtree的条件包含orgtree时，则认为不能合并
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
        # print("            relative info:{} ".format(orgTr ee.relations[rel]))
        if "variableNames" not in orgTree.relations[rel] and "variableNames" not in newTree.relations[rel]:
            continue
        if "variableNames" not in orgTree.relations[rel] and "variableNames" in newTree.relations[rel]:
            return False
        if "variableNames" in orgTree.relations[rel] and "variableNames" not in newTree.relations[rel]:
            return False

        if set(orgTree.relations[rel].keys()) != set(newTree.relations[rel].keys()):
            return False

        # cond1 = orgTree.relations[rel]["condition"]
        names1 = orgTree.relations[rel]["variableNames"]
        # print("            cond of cluster " + cond1)

        # cond2 = newTree.relations[rel]["condition"]
        names2 = newTree.relations[rel]["variableNames"]

        # if set(names1["Literal"] + names1["Number"]) != set(names2["Literal"] + names2["Number"]):
        #     # 二者涉及的属性不相等，则不需要比较具体条件，肯定不等价，直接返回False
        #     return False

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
            # exec('{}=tmp'.format(name))
            globals()[name] = tmp
        for name in list(set(names["Number"])):
            tmp = Real(name)
            # exec('{}=tmp'.format(name))
            globals()[name] = tmp

        for col, condition in orgTree.relations[rel].items():
            if col == "variableNames":
                continue
            cond1 = condition
            join_cond1 = "And("+",".join(cond1)+")"
            cond2 = newTree.relations[rel][col]
            join_cond2 = "And("+",".join(cond2)+")"
            # 先进行字符串比较，如果字符串比较不同的话，再进行条件逻辑比较
            if cond2 == cond1:
                continue
            if col + "Of" + rel in names["Literal"]:
                # z3对多个字符串条件的比较有问题,先用字符串处理函数比较
                if cond1 in cond2:
                    orgTree.relations[rel][col] = cond2
                    continue
                if cond2 in cond1:
                    newTree.relations[rel][col] = cond1
                    continue
                return False
            # 对同样的变量，有满足原始条件，不满足新条件的解，说明cond1包含了cond2
            try:
                # print("            before judge ")
                solver.reset()
                solver.add(eval(join_cond1))
                solver.add(eval(join_cond2))
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
                orgTree.relations[rel][col] = ["Or(" + join_cond1 + "," + join_cond2 + ")"]
                newTree.relations[rel][col] = ["Or(" + join_cond1 + "," + join_cond2 + ")"]
                continue
            else:
                lapFlag = False
                # 如果两个条件有交集,则检查谁包含谁,或者是否没有包含关系
                solver.reset()
                solver.add(eval(join_cond1))
                solver.add(Not(eval(join_cond2)))
                if str(solver.check()) == "sat":
                    # 对同样的变量，有满足原始条件，不满足新条件的解，说明cond1包含cond2或者两者相交
                    lapFlag = True
                solver.reset()
                solver.add(Not(eval(join_cond1)))
                solver.add(eval(join_cond2))
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
                    newTree.relations[rel][col] = cond1
                    continue
    return True