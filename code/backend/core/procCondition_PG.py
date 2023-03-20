
# from parsePlanJson_PG import mid2PreSeq
from procConditionBase import *
from procSQL import *

# from parsePlanJson_PG import remove_invalid_tokens


class TNode:
    def __init__(self, x):
        self.val = x
        self.left = None
        self.mid = None
        self.right = None


class Solution:
    # 判断是否是运算符
    def isOper(self, ch):
        if ch.upper() in ['(', ')', '*', '/', '%', '+', '-', '&', '|', '^', '=', '>', '<', '>=', '<=', '<>', '!=', '!>',
                          '!<',
                          'NOT', 'AND', 'OR', 'SUBSTR', 'ABS']:
            return True
        return False

    def isTripOper(self, ch):
        if ch.upper() in ['SUBSTR']:
            return True
        return False

    def isSingleOper(self, ch):
        if ch.upper() in ['ABS']:
            return True
        return False

    # 获取运算符所对应的优先级别
    def getOperOrder(self, ch):
        if ch == '(':
            return 1
        if ch in ['*', '/', '%']:
            return 2
        if ch in ['+', '-', '&', '|', '^']:
            return 3
        if ch in ['=', '>', '<', '>=', '<=', '<>', '!=', '!>', '!<']:
            return 4
        if self.isTripOper(ch) or ch.upper() == 'ABS':
            return 5
        if ch.upper() == 'NOT':
            return 6
        if ch.upper() == 'AND':
            return 7
        if ch.upper() == 'OR':
            return 8
        return 0

    # 中序遍历表达式二叉树
    def InorderTree(self, pNode, res):
        if not pNode:
            return
        if pNode.left:
            # 如果左子树是符号，且优先级低于父节点的优先级则需要加括号
            if self.isOper(pNode.left.val) and self.getOperOrder(pNode.left.val) < self.getOperOrder(pNode.val):
                res.append('(')
                self.InorderTree(pNode.left, res)
                res.append(')')
            else:
                self.InorderTree(pNode.left, res)
        res.append(pNode.val)
        if pNode.right:
            # 如果有子树是符号且优先级低于父节点的优先级，则需要加括号
            if self.isOper(pNode.right.val) and self.getOperOrder(pNode.right.val) <= self.getOperOrder(pNode.val):
                res.append('(')
                self.InorderTree(pNode.right, res)
                res.append(')')
            else:
                self.InorderTree(pNode.right, res)

    # 前序遍历表达式二叉树
    def preOrderTree(self, pNode, res):
        if not pNode:
            return
        res.append(pNode.val)
        if pNode.left:
            # 如果左子树是符号，且优先级低于父节点的优先级则需要加括号
            if self.isOper(pNode.left.val) and self.getOperOrder(pNode.left.val) < self.getOperOrder(pNode.val):
                res.append('(')
                self.preOrderTree(pNode.left, res)
                res.append(')')
            else:
                self.preOrderTree(pNode.left, res)
        if pNode.mid:
            if self.isOper(pNode.mid.val) and self.getOperOrder(pNode.mid.val) < self.getOperOrder(pNode.val):
                res.append('(')
                self.preOrderTree(pNode.mid, res)
                res.append(')')
            else:
                self.preOrderTree(pNode.mid, res)
        if pNode.right:
            # 如果右子树是符号且优先级低于父节点的优先级，则需要加括号
            if self.isOper(pNode.right.val) and self.getOperOrder(pNode.right.val) <= self.getOperOrder(pNode.val):
                res.append('(')
                self.preOrderTree(pNode.right, res)
                res.append(')')
            else:
                self.preOrderTree(pNode.right, res)

    # 创建二叉树
    def createTree(self, data):
        if not data:
            return
        ch = data.pop(0)
        if ch == '#':
            return None
        else:
            root = TNode(ch)
            root.left = self.createTree(data)
            root.right = self.createTree(data)
            return root

    # 创建二叉树
    def createInOrderTree(self, data):
        if not data:
            return
        ch = data.pop(0)
        if ch == '#':
            return None
        elif self.isOper(ch):
            root = TNode(ch)
            return root
        else:
            root = TNode(ch)
            root.left = self.createTree(data)
            root.right = self.createTree(data)
            return root

    # 后缀表达式生成二叉树
    def PostExpTree(self, data):
        if not data:
            return
        re = []
        while data:
            tmp = data.pop(0)
            if not self.isOper(tmp.upper()):
                re.append(TNode(tmp))
            else:
                p = TNode(tmp)
                if not self.isSingleOper(tmp):
                    p.right = re.pop()
                if self.isTripOper(tmp):
                    p.mid = re.pop()
                p.left = re.pop()
                re.append(p)
        return re.pop()

    # 前缀表达式生成二叉树
    def PreExpTree(self, data):
        re = []
        while data:
            tmp = data.pop()
            if not self.isOper(tmp):
                re.append(TNode(tmp))
            else:
                p = TNode(tmp)
                p.left = re.pop()
                p.right = re.pop()
                re.append(p)
        return re.pop()

    # 中缀表达式生成二叉树
    def InExpTree(self, data):
        re = []
        op = []
        while data:
            tmp = data.pop(0)
            if not self.isOper(tmp.upper()):
                # node = TNode(tmp)
                re.append(tmp)
            else:
                if tmp == '(':
                    op.append('(')
                elif tmp == ')':
                    while op[-1] != '(':
                        re.append(op.pop())
                    op.pop()
                else:
                    while op and op[-1] != '(' and self.getOperOrder(op[-1]) >= self.getOperOrder(tmp):
                        re.append(op.pop())
                    op.append(tmp)
        if op:
            re = re + op[::-1]
        # print(re)
        return self.PostExpTree(re)


# def unionCondition(*conditions):
#     _keys = set(sum([obj.keys() for obj in conditions], []))
#     _total = {}
#     for _key in _keys:
#         for obj in conditions:
#             if _key not in obj:
#                 continue
#             _total[_key] = "and " + obj[_key]
#         _total[_key] = _total[_key].strip("and ")
#     return _total
def remove_invalid_tokens(predicate):
    if not predicate:
        return predicate
    x = re.sub(r'\(\(([a-zA-Z_]+)\)::text ~~ \'(((?!::text).)*)\'::text\)', r"(\1 = '__LIKE__\2')", predicate)
    x = re.sub(r'\(\(([a-zA-Z_]+)\)::text !~~ \'(((?!::text).)*)\'::text\)', r"(\1 = '__NOTLIKE__\2')", x)
    x = re.sub(r'\(\(([a-zA-Z_]+)\)::text <> \'(((?!::text).)*)\'::text\)', r"(\1 = '__NOTEQUAL__\2')", x)
    x = re.sub(r'\(([a-zA-Z_]+) ~~ \'(((?!::text).)*)\'::text\)', r"(\1 = '__LIKE__\2')", x)
    x = re.sub(r'\(([a-zA-Z_]+) !~~ \'(((?!::text).)*)\'::text\)', r"(\1 = '__NOTLIKE__\2')", x)
    x = re.sub(r'\(([a-zA-Z_]+) <> \'(((?!::text).)*)\'::text\)', r"(\1 = '__NOTEQUAL__\2')", x)
    x = re.sub(r'(\'[^\']*\')::[a-z_]+', r'\1', x)
    x = re.sub(r'\(([^\(]+)\)::[a-z_]+', r'\1', x)
    # x = re.sub(r'\(([a-z_0-9A-Z\-]+) = ANY \(\'(\{.+?\})\'\[\]\)\)', r"(\1 = '__ANY__\2')", x)
    x = re.sub(r'\(([a-z_0-9A-Z\-\(\),. ]+) = ANY \(\'(\{.+?\})\'\[\]\)\)', r"(\1 = '__ANY__\2')", x)
    x = re.sub(r'\(SubPlan ([0-9]+)\)', r"(SubPlan_\1)", x)
    x = re.sub(r'(\w*[:]*[ ]*)SubPlan ([0-9]+)', r"(SubPlan_\2 = 1)", x)
    # print(predicate,x)
    return x


def testAND_OLD(conditions):
    if conditions.find("AND ") == -1:
        return conditions
    inner_pattern = re.compile('\((.*)\)')
    condition = inner_pattern.findall(conditions)
    if len(condition) == 0:
        return conditions
    newConds = []
    joinChars = " "
    for cond in getBracePatterns(condition[0]):
        # print("{}\n".format(cond))
        # outer_pattern = re.compile('OR\(.*\)')
        # condGrp = outer_pattern.findall(cond.strip())
        # if len(condGrp) != 0:
        #     return
        if cond.startswith("AND") and cond[4:].find("AND") == -1:
        # if cond.startswith("AND"):
        #     print("==================")
            conditions = "And(" + ",".join(condition[0].split("AND")) + ")"
            # print("after AND:{}\n".format(conditions))
            return conditions
            # return "OR"
        newCond = testAND_OLD(cond)
        if newCond != cond:
            outer_pattern = re.compile('([A-Za-z]*[ ]?)\(.*\)')
            # outer_pattern = re.compile('(,*?)(.*?)\(.*\)')
            condGrp = outer_pattern.findall(cond)
            if len(condGrp) != 0 and len(condGrp[0]) != 0:
                if condGrp[0] != 'AND ':
                    newCond = condGrp[0] + "(" + newCond + ")"
                else:
                    joinChars = ","
            # print("upper after AND:{}\n".format(newCond))
        newConds.append(newCond)
    if joinChars != " ":
        return "And(" + joinChars.join(newConds)+")"
    return joinChars.join(newConds)


def testOR_OLD(conditions):
    if conditions.find("OR ") == -1:
        return conditions
    inner_pattern = re.compile('\((.*)\)')
    condition = inner_pattern.findall(conditions)
    if len(condition) == 0:
        return conditions
    joinChars = " "
    newConds = []
    for cond in getBracePatterns(condition[0]):
        # print("{}\n".format(cond))
        # outer_pattern = re.compile('OR\(.*\)')
        # condGrp = outer_pattern.findall(cond.strip())
        # if len(condGrp) != 0:
        #     return
        if cond.startswith("OR ") and cond[2:].find("OR ") == -1:
            # print("==================")
            conditions = "Or(" + ",".join(condition[0].split("OR")) + ")"
            # print("after:{}\n".format(conditions))
            return conditions
            # return "OR"
        newCond = testOR_OLD(cond)
        if newCond != cond:
            outer_pattern = re.compile('([A-Za-z]*[ ]?)\(.*\)')
            # outer_pattern = re.compile('(,*?)(.*?)\(.*\)')
            condGrp = outer_pattern.findall(cond)
            if len(condGrp) != 0 and len(condGrp[0]) != 0:
                if condGrp[0] != 'OR ':
                    newCond = condGrp[0] + "(" + newCond + ")"
                else:
                    joinChars = ","
        newConds.append("(" + newCond + ")")
    if joinChars != " ":
        return "Or(" + joinChars.join(newConds)+")"
    return joinChars.join(newConds)


def adjustBoolOp(conditions):
    # 如果不需要调整，直接返回
    if conditions.find("OR ") == -1 and conditions.find("AND ") == -1:
        return conditions
    # 去除一重括号
    inner_pattern = re.compile('\((.*)\)')
    condition = inner_pattern.findall(conditions)
    # 如果没有更底层的括号，直接返回
    if len(condition) == 0:
        return conditions
    joinChars = " "
    newConds = []
    for cond in getBracePatterns(condition[0]):
        newCond = adjustBoolOp(cond)
        newConds.append("(" + newCond + ")")
        # 从最底层往上嵌套
        if cond.startswith("OR "):
            oper = "Or"
            joinChars = ","
        if cond.startswith("AND "):
            oper = "And"
            joinChars = ","

    if joinChars != " ":
        return oper + "(" + joinChars.join(newConds)+")"
    return " ".join(newConds)


def transCondPG(rel, conditions, alias2table):
    conditions = remove_invalid_tokens(conditions)
    names = {"Literal": [], "Number": []}
    joinCondition = {}
    namePattern = re.compile(r'[a-zA-Z][a-zA-Z0-9_]*\.*[a-zA-Z][a-zA-Z0-9_]*')
    # typePattern = re.compile(r'(\'[^\']*\')::[a-z_]+')
    columns = namePattern.findall(conditions)
    for column in set(columns):
        col = column.split(".")[-1]
        if column.find(".") != -1:
            curTable = changeAlias2Table(column, alias2table)
            curTable = curTable.split(".")[0]
            conditions = re.sub(r"\b" + column + r"\b", curTable + "." + col, conditions)
            # conditions = conditions.replace(column, curTable + "." + col)
        else:
            curTable = getTableFromItem(col, rel, g_table.data)
        if curTable is not None and curTable.upper() in g_table.data :
            typeName = g_table.data[curTable.upper()][col.upper()].upper()
            colPattern = re.compile(r"\b" + column + r"\b", re.IGNORECASE)
            colMatch = re.search(colPattern, conditions)
            shift = 0
            while colMatch is not None:
                cond_len = len(conditions)
                left = shift + colMatch.span()[0]
                right = shift + colMatch.span()[1]
                if conditions[left-1] != '.' and conditions[right] != '.':
                    conditions = conditions[:left] + curTable + "." + col + conditions[right-cond_len:]
                    shift = right - len(column) + len(curTable + "." + col)
                else:
                    shift = right
                colMatch = re.search(colPattern, conditions[shift:])
            # conditions = re.sub(r"\b" + column + r"\b", curTable + "." + col, conditions)
            # conditions = conditions.replace(column+" ", curTable + "." + col + " ")
            if typeName.find("INT") != -1 or typeName.find("DECIMAL") != -1:
                names["Number"].append(col + "Of" + curTable)
                names["Number"].append(col)
                if conditions.find(col + " = '__ANY__") != -1:
                    inPattern = re.compile(col + ' = \'__ANY__\{(.+?)\}')
                    grps = inPattern.findall(conditions)
                    for grp in grps:
                        inList = (" or " + curTable + "." + col + " = ").join([x.strip("\"") for x in grp.split(",")])
                        conditions = conditions.replace(" = '__ANY__{" + grp + "}'", " = " + inList)

            else:
                names["Literal"].append(col + "Of" + curTable)
                names["Literal"].append(col)
            # joinCondition[col] = conditions
            joinCondition[curTable + "." + col] = ""

    conditions = conditions.replace(" = ", " == ")
    conditions = adjustBoolOp(conditions)

    for key in joinCondition.keys():
        joinCondition[key] = conditions
    #
    #
    #
    # types = typePattern.findall(conditions)
    # if len(colNames) == 0:
    #     return conditions, names, joinCondition
    # if len(types) != 0 and len(colNames) == len(types):
    #     for idx in range(len(colNames)):
    #         if types[idx] == "numeric":
    #             names["Number"].append(colNames[idx].split(".")[-1])
    #         else:
    #             names["Literal"].append(colNames[idx].split(".")[-1])
    #         joinCondition[colNames[idx].split(".")[-1]] = conditions
    # # 获取最外层括号中的内容
    # inner_pattern = re.compile('\((.*)\)')
    # condition = inner_pattern.findall(conditions)
    #     for cond in getBracePatterns(condition[0]):

    return conditions, names, joinCondition


def transCondPG_OLD(rel, conditions):
    names = {"Literal": [], "Number": []}
    joinCondition = {}
    # 获取最外层的函数
    # var_pattern = re.compile('([A-Za-z]\w*[\.[A-Za-z]\w*]*)[,]')
    outer_pattern = re.compile('([A-Za-z]*?)\(.*\)')
    # outer_pattern = re.compile('(,*?)(.*?)\(.*\)')
    condGrp = outer_pattern.findall(conditions)
    if len(condGrp) != 0 and len(condGrp[0]) != 0:
        operator = condGrp[0]
        s = Solution()
        if not s.isOper(operator):
            print("{} is not an operator".format(operator))
            return "", names, joinCondition
    # 获取最外层括号中的内容
    inner_pattern = re.compile('\((.*)\)')
    condition = inner_pattern.findall(conditions)
    # 如果还有括号，继续获取
    transedCond = []
    if inner_pattern.findall(condition[0]):
        refKey = []
        # 获取逗号分隔的括号内容或者独立参数
        for cond in getBracePatterns(condition[0]):
            # print(condition[0])
            # print(cond)
            subCondition, subNames, subRefCondition = transCondPG(rel, cond)
            transedCond.append(subCondition)
            for interKey in list(set(joinCondition.keys()).intersection(set(subRefCondition.keys()))):
                subRefCondition[interKey] = joinCondition[interKey] + " and " + subRefCondition[interKey]
            joinCondition = dict(joinCondition, **subRefCondition)
            # joinCondition = unionCondition(joinCondition, subRefCondition)
            if len(subNames["Literal"]) != 0:
                if len(names["Literal"]) == 0:
                    names["Literal"] = subNames["Literal"]
                else:
                    names["Literal"] = list(set(names["Literal"]) | set(subNames["Literal"]))
            if len(subNames["Number"]) != 0:
                if len(names["Number"]) == 0:
                    names["Number"] = subNames["Number"]
                else:
                    names["Number"] = list(set(names["Number"]) | set(subNames["Number"]))
            for col in joinCondition.keys():
                if checkColName(rel, col):
                    refKey.append(col)
        # tmpCondition = combineOperand(operator, transedCond)
        # for refCol in refKey:
        #     joinCondition[refCol] = condition[0]
        # print(tmpCondition)
        # exit()
        return condition[0], names, joinCondition
    # 如果没有括号，处理普通参数
    # 引号引起来的部分作为一个参数
    # 获取字段名（变量名）
    for token in condition[0].split():
        namePattern = re.compile(r'^[a-zA-Z][a-zA-Z0-9_]*\.*[a-zA-Z][a-zA-Z0-9_]*$')
        colNames = namePattern.findall(token)
        if len(colNames) == 0:
            continue
        # 如果有单引号，就是字符
        # 如果没有单引号，就是数字
        if conditions.find("'") != -1:
            for col in colNames:
                names["Literal"].append(col.split(".")[-1])
        else:
            for col in colNames:
                names["Number"].append(col.split(".")[-1])
        joinCondition[token] = condition[0]
    return condition[0], names, joinCondition


def getTreeConditions_PG(planTree, alias2table):
    # 遍历树，将每个条件与相关的表关联
    # node = planTree
    def procNodeCondition(node, alias2table):
        global g_table
        # names = {"Literal": [], "Number": []}
        # cond = "(1==1)"
        refCondition = {}
        needProc = True
        for rel, cond in node.relations.items():
            # 只要有一个不为空，就说明处理过了，不需要再处理
            if node.relations[rel]:
                needProc = False
                break
        if node.data is not None and needProc is True:
            if 'Filter' in node.data:
                condTmp, namesTmp, refCondition = transCondPG(node.relations.keys(), node.data["Filter"], alias2table)
            if 'Index Cond' in node.data:
                condTmp, namesTmp, refCondition = transCondPG(node.relations.keys(), node.data["Index Cond"],
                                                              alias2table)
            if 'Hash Cond' in node.data:
                condTmp, namesTmp, refCondition = transCondPG(node.relations.keys(), node.data["Hash Cond"],
                                                              alias2table)
            if 'Merge Cond' in node.data:
                condTmp, namesTmp, refCondition = transCondPG(node.relations.keys(), node.data["Merge Cond"],
                                                              alias2table)
            if 'Join Filter' in node.data:
                condTmp, namesTmp, refCondition = transCondPG(node.relations.keys(), node.data["Join Filter"],
                                                              alias2table)
            # if node.left is None:
            #     return
            # print("relation:{}\ncondition:{}, \nnames:{}, \nrefCols:{}".format(rel, condTmp, namesTmp, refCondition))
            for refs, condition in refCondition.items():
                condition = remove_invalid_tokens(condition)
                if -1 == refs.find("."):
                    refRel = getTableFromItem(refs, node.relations.keys(), g_table.data)
                    refCol = refs
                else:
                    refRel = refs.split(".")[0]
                    refCol = refs.split(".")[1]
                    # condition = condition.replace(refs, refCol)
                condition = re.sub(r'([a-zA-Z][a-zA-Z0-9_]*)\.([a-zA-Z][a-zA-Z_0-9]*)', r'\2Of\1', condition)
                appendColCondition(node, refRel, refCol, condition)
                appendNames(node, refRel, namesTmp)
        # 根据节点的遍历路径，在处理本节点之前，子节点已经处理好了
        # 把子节点的条件合并到本节点上面来
        if node.left is not None:
            procNodeCondition(node.left, alias2table)
            # for interKey in list(set(node.relations.keys()).intersection(set(node.left.relations.keys()))):
            for interKey in node.left.relations.keys():
                # for innerKey in list(set(node.relations[interKey]).intersection(set(node.left.relations[interKey]))):
                    # if innerKey == 'variableNames':
                    #     appendNames(node, interKey, node.left.relations[interKey][innerKey])
                    # else:
                for innerKey, conds in node.left.relations[interKey].items():
                    if innerKey == 'variableNames':
                        appendNames(node, interKey, node.left.relations[interKey][innerKey])
                    else:
                        for cond in conds:
                            appendColCondition(node, interKey, innerKey, cond)
                    # node.relations[interKey][innerKey] = node.relations[interKey][innerKey] + " and " +\
                    #                                      node.left.relations[interKey][innerKey]
                # 不能直接合并。直接合并将导致使用相同地址
                # node.relations[interKey] = dict(node.left.relations[interKey], **node.relations[interKey])
            # print("====================================")
            # print(node.left.relations)
            # print(node.relations)
            # node.relations = dict(node.left.relations, **node.relations)
        if node.right is not None:
            procNodeCondition(node.right, alias2table)
            # for interKey in list(set(node.relations.keys()).intersection(set(node.right.relations.keys()))):
                # for innerKey in list(set(node.relations[interKey]).intersection(set(node.right.relations[interKey]))):
                #     if innerKey == 'variableNames':
                #         appendNames(node, interKey, node.right.relations[interKey][innerKey])
                #     else:
                #         for cond in node.right.relations[interKey][innerKey]:
                #             appendColCondition(node, interKey, innerKey, cond)
                #     # node.relations[interKey][innerKey] = node.relations[interKey][innerKey] + " and " + \
                #     #                                      node.right.relations[interKey][innerKey]
                # node.relations[interKey] = dict(node.right.relations[interKey], **node.relations[interKey])
            for interKey in node.right.relations.keys():
                for innerKey, conds in node.right.relations[interKey].items():
                    if innerKey == 'variableNames':
                        appendNames(node, interKey, node.right.relations[interKey][innerKey])
                    else:
                        for cond in conds:
                            appendColCondition(node, interKey, innerKey, cond)
            # node.relations = dict(node.right.relations, **node.relations)

    procNodeCondition(planTree, alias2table)
