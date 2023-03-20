import re
from procSQL import *

'''
结构体：
'''
class SPNode(object):
    def __init__(self, nodeId, nodeType, fieldList, tableList, joinFilterList, outerJoinFilterList, commonFilterList, curTime=0.0, filterNode=None):
        self.nodeId = nodeId
        self.nodeType = nodeType
        self.fieldList = fieldList
        self.tableList = tableList
        self.joinFilterList = joinFilterList
        self.outerJoinFilterList = outerJoinFilterList
        self.commonFilterList = commonFilterList
        self.curTime = curTime
        self.filterNode = filterNode
        self.totalTime = 0.0
        self.left = None
        self.right = None
        self.father = None
        self.isSubGraph = False # 是否同一个subgraph
        self.relations = {}
        self.isGroupBy = False
        self.is_hash_agg = False
        self.group_by_list = []
        self.agg_list = []
        self.isOrderBy = False
        self.isLimit = False

    def __str__(self):
        return str(self.nodeId) + "\n" + self.nodeType + "\n" + str(self.fieldList) + "\n" + str(self.tableList) + "\n" + str(self.totalTime) + "\n"
        # return str(self.nodeId) + "\n" + self.nodeType


class FilterNode_SP(object):
    def __init__(self, fieldList, tableList, joinFilterList, outerJoinFilterList, commonFilterList):
        self.fieldList = fieldList
        self.tableList = tableList
        self.joinFilterList = joinFilterList
        self.outerJoinFilterList = outerJoinFilterList
        self.commonFilterList = commonFilterList


    def __str__(self):
        return str(self.fieldList) + "\n" + str(self.tableList) + "\n" + str(self.joinFilterList) + "\n" + str(self.commonFilterList) + "\n"




'''
一些常用的方法
'''
#
def CheckAndAddNullTableName(num, field, relationDict, tableDict):
    return AddRelationDictFromExistingAsName(num, field, relationDict, tableDict)

# 如果找不到字段，那么从已经存在的信息里，根据相同的field来add到dict
# 并且修改asName
def AddRelationDictFromExistingAsName(num, field, relationDict, tableDict):
    for k, v in relationDict.items():
        if v.field == field:
            relationDict[num] = relationDict[k]
            return True
    return False


# 处理avg(ss_net_profit#47)和rank_col#11两种情况
def parseAggrOrfield(s, relationDict, tableDict):
    ansStr = ""
    err = ""
    # 首先匹配类似于：avg(ss_net_profit#47)
    pattern = r"[\w]+\([\w]+#[0-9L]+\)"
    # 使用match，匹配从开头开始的
    obj = re.match(pattern, s)
    if obj:
        matchStr = obj.group()
        pattern = r"([\w]+)\(([\w]+)#([0-9L]+)\)"
        key = re.findall(pattern, matchStr)
        if len(key) > 0:
            aggr = key[0][0]
            field = key[0][1].strip()
            num = key[0][2].strip()
            ok = CheckAndAddNullTableName(num, field, relationDict, tableDict)
            if not ok:
                err = "avgtype: can't get num:{}".format(num)
                return err, ansStr
            ansStr = aggr + "(" + relationDict[num].displayName + ")"
        else:
            err = "avgtype: matchStr can't get str:{}".format(matchStr)
            return err, ansStr
    else:
        ansStr = s

    return err, ansStr


# 解决SortMergeJoin的substr问题
def parseSortMergeJoinFilter(filterStr):
    i = 0
    ansList = []
    tmpStr = ""
    left = 0
    while i < len(filterStr):
        if filterStr[i] == "[":
            tmpStr = ""
            i = i + 1
            continue
        elif filterStr[i] == "(":
            left = left + 1
        elif filterStr[i] == ")":
            left = left - 1
        elif filterStr[i] == ",":
            if tmpStr != "":
                if left == 0:
                    ansList.append(tmpStr.strip())
                    tmpStr = ""
                else:
                    tmpStr = tmpStr + filterStr[i]
            i = i + 1
            continue
        elif filterStr[i] == "]":
            if tmpStr != "":
                if left == 0:
                    ansList.append(tmpStr.strip())
                    tmpStr = ""
                else:
                    tmpStr = tmpStr + filterStr[i]
            i = i + 1
            continue
        tmpStr = tmpStr + filterStr[i]
        i = i + 1

    if tmpStr.strip() != "":
        ansList.append(tmpStr.strip())
    return ansList


# 将多个信息通过括号来提取为数组
def parseBracketFilter(filterStr):
    ansList = []
    tmpStr = ""
    left = 0
    for s in filterStr:
        if s == "," and left == 0:
            ansList.append(tmpStr.strip())
            tmpStr = ""
            continue
        if s == "(":
            left = left + 1
        elif s == ")":
            left = left - 1
        tmpStr = tmpStr + s

    if tmpStr != "":
        ansList.append(tmpStr.strip())
    return ansList


# filter算子的特殊参数处理
def FilterFieldManage(data, relationDict, commonFilter, tableSet, subQueryDict, tableField):
    # 6.子查询的情况，暂不处理
    if data.find("Subquery") != -1:
        return {}
    # 1.Contains
    pattern = r"[Cc]ontain[s]*"
    ok = re.search(pattern, data)
    if ok:
        pattern = r"[Nn][Oo][Tt] [Cc]ontain[s]*"
        if re.search(pattern, data):
            prex = "not like"
        else:
            prex = "like"
        key1, key2 = data.split(",")
        filed, num = key1.strip().strip("(").strip(")").split("(")[1].split("#")
        value = getFilterValue(key2)
        num = num.strip()
        condition = {"left": relationDict[num].displayName, "op": prex, "right": "\'%" + value + "%\'", "table": relationDict[num].table, "field": relationDict[num].field}
        # condition = relationDict[num].displayName + prex + " \'%" + value + "%\'"
        # commonFilter.append(condition)
        # tableSet.add(relationDict[num].table)
        return condition
    # 2.StartsWith
    pattern = r"StartsWith"
    ok = re.search(pattern, data)
    if ok:
        pattern = r"[Nn][Oo][Tt] [Ss]tarts[Ww]ith+"
        if re.search(pattern, data):
            prex = "not like"
        else:
            prex = "like"
        key1, key2 = data.split(",")
        filed, num = key1.strip().strip("(").strip(")").split("(")[1].split("#")
        value = getFilterValue(key2)
        num = num.strip()
        condition = {"left": relationDict[num].displayName, "op": prex, "right": "\'%" + value + "\'", "table": relationDict[num].table, "field": relationDict[num].field}
        # condition = relationDict[num].displayName + prex + " \'%" + value + "\'"
        # commonFilter.append(condition)
        # tableSet.add(relationDict[num].table)
        return condition
    # 3.IN
    pattern = r"[ ]+[Ii][Nn][ ]+"
    ok = re.search(pattern, data)
    if ok:
        key1, key2 = re.split(pattern, data)
        key1 = key1.strip().strip("(")
        pattern = r"[\w]+#([0-9L]+)"
        keyList = re.findall(pattern, key1)
        num = keyList[0].strip()
        if not relationDict.get(num):
            print("[FilterFieldManage] in get num error! {}".format(data))
            return {}
        leftValue = re.sub(r"[\w]+#[0-9L]+", relationDict[num].displayName, key1)
        key2 = key2.strip().strip('(').strip(')')
        dataList = key2.split(',')
        # 判断左值是否为string
        table = relationDict[num].table
        if table.find(" AS ") != -1:
            table = table.split(" AS ")[0]
        isString = tableField[table][relationDict[num].field]["type"] == "string"
        isDate = tableField[table][relationDict[num].field]["type"] == "date"
        # 如果是in的情况，right值是个数组
        i = 0
        while i < len(dataList):
            dataList[i] = dataList[i].strip()
            if isString or isDate:
                dataList[i] = "\'" + dataList[i] + "\'"
            i += 1
        condition = {"left": leftValue, "op": "in", "right": dataList, "table": relationDict[num].table, "field": relationDict[num].field}
        # i = 0
        # value = '('
        # while i < len(dataList):
        #     dataList[i] = dataList[i].strip()
        #     if i != 0:
        #         value = value + ","
        #     if isString or isDate:
        #         value = value + "\'" + dataList[i] + "\'"
        #     else:
        #         value = value + dataList[i]
        #     i = i + 1
        # value = value + ")"
        # condition = leftValue + " IN " + value
        # commonFilter.append(condition)
        # tableSet.add(relationDict[num].table)
        return condition
    # 4.LIKE
    pattern = r"[ ]+LIKE[ ]+"
    ok = re.search(pattern, data)
    if ok:
        key1, key2 = re.split(pattern, data)
        filed, num = key1.strip().strip("(").split("#")
        key2 = key2.strip()
        condition = {"left": relationDict[num].displayName, "op": "like", "right": "\'" + key2 + "\'", "table": relationDict[num].table, "field": relationDict[num].field}
        # condition = relationDict[num].displayName + " LIKE " + "\'" + key2 + "\'"
        # commonFilter.append(condition)
        # tableSet.add(relationDict[num].table)
        return condition
    # 5.substr INSET
    pattern = r"(substr\([\w]+#[0-9L]+,[ ]*[0-9]+,[ ]*[0-9]+\))[ ]*INSET[ ]*(\([0-9,]*\))"
    key = re.findall(pattern, data)
    if len(key) > 0:
        leftValue = key[0][0]
        rightValue = key[0][1]
        pattern = r"[\w]+#([0-9L]+)"
        num = re.findall(pattern, leftValue)
        num = num[0].strip()
        if not relationDict.get(num):
            print("FilterFieldManage err!")
            return {}
        leftValue = re.sub(r"[\w]+#[0-9L]+", relationDict[num].displayName, leftValue)
        rightValue = re.sub(r"[(]", "('", rightValue)
        rightValue = re.sub(r"[)]", "')'", rightValue)
        rightValue = re.sub(r"[,]", "','", rightValue)
        condition = {"left": leftValue, "op": "INSET", "right": rightValue, "table": relationDict[num].table, "field": relationDict[num].field}
        # condition = leftValue + " INSET " + rightValue
        # commonFilter.append(condition)
        # tableSet.add(relationDict[num].table)
        return condition
    else:
        pattern = r"([\w\(\)]+#[0-9L]+)[^<>=]*([<>=]+)"
        key = re.findall(pattern, data)
        if len(key) == 0:
            return {}
        leftField = key[0][0]
        sign = key[0][1]
        rightField = getFilterValue(data.split(sign)[1])
        filed, num = leftField.strip().strip("(").split("#")
        filed = filed.strip()
        num = num.strip()
        if not relationDict.get(num):
            print("[FilterFieldManage] data: {} can't find num: {}".format(data, num))
            if not AddRelationDictFromExisting(num, filed, relationDict):
                print("[FilterFieldManage] data: {} can't find num: {} still can't find!".format(data, num))
                return {}
        if data.find("NOT") != -1:
            sign = "!="
        # 判断右值是否也是字段
        pattern = r"([\w]+)#([0-9L]+)"
        key = re.findall(pattern, rightField)
        if len(key) > 0:
            filed, num = key[0][0].strip(), key[0][1].strip()
            if not relationDict.get(num):
                print("[FilterFieldManage] data: {} can't find num: {}".format(data, num))
                if not AddRelationDictFromExisting(num, filed, relationDict):
                    print("[FilterFieldManage] data: {} can't find num: {} still can't find!".format(data, num))
                    return {}
            rightField = relationDict[num].displayName
        elif is_filter_date(num, relationDict, tableField):
            # 是日期格式，而且刚好是数字
            if is_number(rightField):
                # rightField = "cast('1970-01-01' as date) + interval " + rightField + " days"
                rightField = rightField.strip()
        elif not is_number(rightField):
            rightField = "\'" + rightField.strip() + "\'"

        condition = {"left": relationDict[num].displayName, "op": sign, "right": rightField, "table": relationDict[num].table, "field": relationDict[num].field}
        # condition = relationDict[num].displayName + " " + sign + " " + rightField
        # commonFilter.append(condition)
        # tableSet.add(relationDict[num].table)
        return condition


# 如果找不到字段，那么从已经存在的信息里，根据相同的field来add到dict
def AddRelationDictFromExisting(num, field, relationDict):
    for k, v in relationDict.items():
        if v.field == field:
            relationDict[num] = relationDict[k]
            return True
    return False


# 自定义函数：判断字符串是否是数字
def is_number(s):
    try:
        float(s)
        return True
    except ValueError:
        pass

    try:
        import unicodedata
        unicodedata.numeric(s)
        return True
    except (TypeError, ValueError):
        pass

    return False


# 栈
class Stack:
    def __init__(self):
        self.stack = []
        self.size = 0
    def push(self, item):
        self.stack.append(item) # 添加元素
        self.size += 1 # 栈元素数量加 1
    def pop(self):
        pop = self.stack.pop() # 删除栈顶元素
        self.size -= 1 # 栈元素数量减 1
        return pop
    def isEmpty(self):
        return self.stack == []
    def sizes(self):
        return self.size
    def peek(self):
        return self.stack[-1]


# 复杂的获取逻辑
# 通过栈来对括号进行匹配
# 用于解析plan
def deleteBracketValue(processedStr, relationDict, aliasDict):
    deleteList = {
        "CheckOverflow": 2,  # 还需要处理其余部门
        "promote_precision": 1,
        "cast": 1,
        "UnscaledValue": 1,
        "MakeDecimal": 1
    }
    signDict = {
        "+": 1,
        "-": 1,
        "*": 1,
        "/": 1
    }
    i = 0
    strLen = len(processedStr)
    curStr = ""
    ansStr = ""
    lastBracketFlag = False  # 连续两个括号
    stk = Stack()  # 栈，存储(前面的字符
    stk.push("")
    tableSet = set()
    while i < strLen:
        if processedStr[i] == "(":
            if curStr == "":
                if lastBracketFlag:
                    ansStr = ansStr + processedStr[i]  # (
                    stk.push(processedStr[i])
            elif deleteList.get(curStr):
                stk.push(curStr)
            else:
                ansStr = ansStr + curStr + "("  # 函数
                stk.push(curStr)
            curStr = ""  # 清空
            lastBracketFlag = True
        elif processedStr[i] == ")":
            if stk.isEmpty():
                print("[deleteBracketValue] str: {} error! stk empty!".format(processedStr))
                print(ansStr)
            else:
                peekStr = stk.peek()
                stk.pop()
            if peekStr == "(":
                ansStr = ansStr + curStr.strip() + ")"
            elif curStr.find(",") != -1:
                pass
            else:
                curStr = curStr.strip()
                # 处理字段和num
                if curStr != "":
                    if curStr.find("#") == -1:
                        print("[deleteBracketValue] {} curStr maybe not suitable!".format(processedStr))
                        break
                    # 批量处理d_day_name#31类型的数据
                    pattern = r"([\w]+#[0-9L]+)"
                    keyList = re.findall(pattern, curStr)
                    for key in keyList:
                        field, num = key.split("#")
                        field = field.strip()
                        num = num.strip()
                        table, field, displayName = replace_original_general(num, field, relationDict, aliasDict)
                        if table == "":
                            print(f"[deleteBracketValue] {num} get num error! key:{key}")
                            return False, "", ""
                        else:
                            curStr = curStr.replace(key, displayName)
                            tableSet.add(table)
                    ansStr = ansStr + curStr

                if not deleteList.get(peekStr) and peekStr != "":
                    ansStr = ansStr + ")"
            curStr = ""  # 清空
            lastBracketFlag = False
        elif signDict.get(processedStr[i]):  # 符号
            ansStr = ansStr + " " + processedStr[i] + " "
            curStr = ""  # 清空
            lastBracketFlag = False
        else:
            curStr = curStr + processedStr[i]
            lastBracketFlag = False

        i = i + 1

    return True, ansStr, list(tableSet)


# 通过堆栈方式将spark优化的函数删除
def deletePlanBracketValue(processedStr, relationDict, subQueryDict):
    processedStr = re.sub("[ ]*as[ ]*decimal\([0-9]+,[0-9]+\)", "", processedStr)
    processedStr = re.sub(",[ ]*DecimalType\([0-9]+,[0-9]+\),[ ]*true", "", processedStr)
    processedStr = re.sub(",[ ]*\[id=#[0-9]+\]", "", processedStr)
    processedStr = parseBracketConsistency(processedStr)
    deleteList = {
        "CheckOverflow": 2,  # 还需要处理其余部门
        "promote_precision": 1,
        "cast": 1,
        "UnscaledValue": 1,
    }
    signDict = {
        "+": 1,
        "-": 1,
        "*": 1,
        "/": 1,
        ">": 1,
        "<": 1
    }
    i = 0
    strLen = len(processedStr)
    curStr = ""
    ansStr = ""
    lastBracketFlag = False  # 连续两个括号
    stk = Stack()  # 栈，存储(前面的字符
    stk.push("")
    tableSet = set()
    while i < strLen:
        if processedStr[i] == "(":
            curStr = curStr.strip()
            if curStr == "":
                if lastBracketFlag:
                    ansStr = ansStr + processedStr[i]  # (
                    stk.push(processedStr[i])
            elif deleteList.get(curStr):
                stk.push(curStr)
            else:
                ansStr = ansStr + curStr + "("  # 函数
                stk.push(curStr)
            curStr = ""  # 清空
            lastBracketFlag = True
        elif processedStr[i] == ")":
            if stk.isEmpty():
                print("[deleteBracketValue] str: {} error! stk empty!".format(processedStr))
                print(ansStr)
            else:
                peekStr = stk.peek()
                stk.pop()
            if peekStr == "(":
                ansStr = ansStr + curStr.strip() + ")"
            elif curStr.find(",") != -1:
                pass
            else:
                curStr = curStr.strip()
                # 处理字段和num
                if curStr != "":
                    # 处理subquery
                    curStr = curStr.strip()
                    if curStr.find("Subquery") != -1:
                        pattern = r"Subquery[ ]*([\w-]+#[0-9]+)"
                        key = re.findall(pattern, curStr)
                        if len(key) > 0:
                            if subQueryDict and subQueryDict.get(key[0]):
                                ansStr += "(" + subQueryDict[key[0]] + ")"
                                curStr = ""  # 清空
                                lastBracketFlag = False
                                continue
                            else:
                                return "", []
                    if curStr.find("#") == -1:
                        print("[deleteBracketValue] {} curStr maybe not suitable!".format(processedStr))
                        break
                    # 批量处理d_day_name#31类型的数据
                    pattern = r"([\w]+#[0-9L]+)"
                    keyList = re.findall(pattern, curStr)
                    for key in keyList:
                        field, num = key.split("#")
                        field = field.strip()
                        num = num.strip()
                        if relationDict.get(num):
                            curStr = curStr.replace(key, relationDict[num].displayName)
                            tableSet.add(relationDict[num].table)
                    ansStr = ansStr + curStr

                if not deleteList.get(peekStr.strip()) and peekStr != "":
                    ansStr = ansStr + ")"
            curStr = ""  # 清空
            lastBracketFlag = False
        elif signDict.get(processedStr[i]):  # 符号
            if processedStr[i] == "-" and ansStr != "":
                curStr = curStr + processedStr[i]
                lastBracketFlag = False
                i = i + 1
                continue
            ansStr += curStr.strip()
            ansStr = ansStr + " " + processedStr[i] + " "
            curStr = ""  # 清空
            lastBracketFlag = False
        else:
            curStr = curStr + processedStr[i]
            lastBracketFlag = False

        i = i + 1

    return ansStr, list(tableSet)


# 处理括号一致性问题，删除多余的括号
def parseBracketConsistency(filterStr):
    cnt = 0
    i = 0
    while i < len(filterStr):
        ch = filterStr[i]
        if ch == "(":
            cnt += 1
        elif ch == ")":
            cnt -= 1
        if cnt < 0:
            break

        i += 1
    return filterStr[:i]


# 只得到一个带括号或不带括号的字段
def getFilterValue(info):
    value = ""
    info = info.strip()
    i = 0
    stopFlag = 0
    while i < len(info):
        if info[i] == '(':
            stopFlag = stopFlag + 1
        else:
            break
        i = i + 1
    for ch in info:
        if ch == ')':
            if stopFlag != 0:
                stopFlag = stopFlag - 1
            else:
                break
        value = value + ch
    return value


def findNodeHead(maxId, nodeMap):
    i = 0
    while i <= maxId:
        if nodeMap[str(i)].nodeType == "WholeStageCodegen":
            i = i + 1
            continue
        if nodeMap[str(i)].father == None:
            nodeHead = nodeMap[str(i)]
            break
        i = i + 1
    return nodeHead


# 用于候选视图生成部分
def traverseTreeNode(nodeHead, clusters, tableField):
    if not nodeHead: # 空结点
        return 0.0, set(), set(), set(), set(), set(), False
    cost1, fieldList1, tableList1, joinFilterList1, outerJoinFilterList1, commonFilterList1, lastSubFlag1 = traverseTreeNode(nodeHead.left, clusters, tableField)
    cost2, fieldList2, tableList2, joinFilterList2, outerJoinFilterList2, commonFilterList2, lastSubFlag2 = traverseTreeNode(nodeHead.right, clusters, tableField)
    # cost
    cost = cost1 + cost2
    fieldList = fieldList1 | fieldList2
    tableList = tableList1 | tableList2
    joinFilterList = joinFilterList1 | joinFilterList2
    commonFilterList = commonFilterList1 | commonFilterList2
    outerJoinFilterList = outerJoinFilterList1 | outerJoinFilterList2
    # isSubGraph是否为WholeStageCodegen
    if not nodeHead.isSubGraph:
        cost = cost + nodeHead.curTime
    else:
        if not lastSubFlag1 and not lastSubFlag2:
            cost = cost + nodeHead.curTime
    nodeHead.totalTime = cost
    # field/table/joinFilter/commonFilter
    for field in nodeHead.fieldList:
        fieldList.add(field)
    for table in nodeHead.tableList:
        tableList.add(table)
    for joinFilter in nodeHead.joinFilterList:
        joinFilterList.add(joinFilter)
    for outerJoinFilter in nodeHead.outerJoinFilterList:
        outerJoinFilterList.add(outerJoinFilter)
    for commonFilter in nodeHead.commonFilterList:
        commonFilterList.add(commonFilter)
    nodeHead.filterNode = FilterNode_SP(list(fieldList), list(tableList), list(joinFilterList), list(outerJoinFilterList), list(commonFilterList))

    # Join子树
    if nodeHead.nodeType.find("Join") != -1:
        # field/table不能为空
        if nodeHead.filterNode.fieldList != [] \
                and nodeHead.filterNode.tableList != []:
            if len(nodeHead.filterNode.tableList) > 1:
                # 取表和join条件的并集，删除其它条件
                remove_table_redundant(nodeHead)
                # # 如果某个表只出现一次，那么无需别名
                # remove_table_aliasName(nodeHead)
                # # 对于出现的相同列取别名
                check_equal_column_alias(nodeHead)
                # 检查列和filter的括号匹配情况
                check_bracket_match(nodeHead)
                clusters.append(nodeHead)
    # Agg子树
    if nodeHead.nodeType.find("Aggregate") != -1 and nodeHead.is_hash_agg:
        if len(nodeHead.filterNode.tableList) > 1:
            # 取表和join条件的并集，删除其它条件
            remove_table_redundant(nodeHead)
            # # 如果某个表只出现一次，那么无需别名
            # remove_table_aliasName(nodeHead)
            # # 对于出现的相同列取别名
            check_equal_column_alias(nodeHead)
            # 检查列和filter的括号匹配情况
            check_bracket_match(nodeHead)
            # 将filter涉及的列加入group_by_list中
            add_group_project(nodeHead)
            clusters.append(nodeHead)

    return cost, fieldList, tableList, joinFilterList, outerJoinFilterList, commonFilterList, nodeHead.isSubGraph


# 用于encode部分，和生成候选视图部分分离
def traverse_encode_tree_node(nodeHead, clusters, tableField):
    if not nodeHead: # 空结点
        return 0.0, set(), set(), set(), set(), False
    cost1, fieldList1, tableList1, joinFilterList1, commonFilterList1, lastSubFlag1 = traverseTreeNode(nodeHead.left, clusters, tableField)
    cost2, fieldList2, tableList2, joinFilterList2, commonFilterList2, lastSubFlag2 = traverseTreeNode(nodeHead.right, clusters, tableField)
    # cost
    cost = cost1 + cost2
    fieldList = fieldList1 | fieldList2
    tableList = tableList1 | tableList2
    joinFilterList = joinFilterList1 | joinFilterList2
    commonFilterList = commonFilterList1 | commonFilterList2
    # isSubGraph是否为WholeStageCodegen
    if not nodeHead.isSubGraph:
        cost = cost + nodeHead.curTime
    else:
        if not lastSubFlag1 and not lastSubFlag2:
            cost = cost + nodeHead.curTime
    nodeHead.totalTime = cost
    # field/table/joinFilter/commonFilter
    for field in nodeHead.fieldList:
        fieldList.add(field)
    for table in nodeHead.tableList:
        tableList.add(table)
    for joinFilter in nodeHead.joinFilterList:
        joinFilterList.add(joinFilter)
    for commonFilter in nodeHead.commonFilterList:
        commonFilterList.add(commonFilter)
    nodeHead.filterNode = FilterNode_SP(list(fieldList), list(tableList), list(joinFilterList), list(commonFilterList))
    # # 检查table是否已经被join，得到表的个数
    # CheckTableWithoutJoin(nodeHead)
    # # 其它过滤
    # CheckOtherFilter(nodeHead)
    # 子树
    if nodeHead.nodeType.find("Join") != -1 \
            or nodeHead.nodeType.find("Aggregate") != -1 \
                or nodeHead.nodeType.find("TakeOrderedAndProject") != -1:
        # field/table不能为空
        if nodeHead.filterNode.fieldList != [] \
                and nodeHead.filterNode.tableList != []:
            # # 多表才去处理
            # if len(nodeHead.filterNode.tableList) > 1:
            #     # 如果表只出现一次则无需别名
            #     CheckTableAliasName(nodeHead)
            #     # 处理field列的问题
            #     CheckTableField(nodeHead, tableField)
            #     # 将filter中出现的其它表的信息剔除
            #     CheckFilterOtherField(nodeHead)
            if len(nodeHead.filterNode.tableList) > 1:
                # 取表和join条件的并集，删除其它条件
                if nodeHead.nodeType != "BroadcastNestedLoopJoin":
                    remove_table_redundant(nodeHead)
                # 如果某个表只出现一次，那么无需别名
                remove_table_aliasName(nodeHead)
                clusters.append(nodeHead)

    return cost, fieldList, tableList, joinFilterList, commonFilterList, nodeHead.isSubGraph


# 用于encode部分，和生成候选视图部分分离
def traverse_qmv_encode_tree_node(nodeHead, clusters, tableField):
    if not nodeHead: # 空结点
        return 0.0, set(), set(), set(), set(), set(), False
    cost1, fieldList1, tableList1, joinFilterList1, outerJoinFilterList1, commonFilterList1, lastSubFlag1 = traverseTreeNode(nodeHead.left, clusters, tableField)
    cost2, fieldList2, tableList2, joinFilterList2, outerJoinFilterList2, commonFilterList2, lastSubFlag2 = traverseTreeNode(nodeHead.right, clusters, tableField)
    # cost
    cost = cost1 + cost2
    fieldList = fieldList1 | fieldList2
    tableList = tableList1 | tableList2
    joinFilterList = joinFilterList1 | joinFilterList2
    commonFilterList = commonFilterList1 | commonFilterList2
    outerJoinFilterList = outerJoinFilterList1 | outerJoinFilterList2
    # isSubGraph是否为WholeStageCodegen
    if not nodeHead.isSubGraph:
        cost = cost + nodeHead.curTime
    else:
        if not lastSubFlag1 and not lastSubFlag2:
            cost = cost + nodeHead.curTime
    nodeHead.totalTime = cost
    # field/table/joinFilter/commonFilter
    for field in nodeHead.fieldList:
        fieldList.add(field)
    for table in nodeHead.tableList:
        tableList.add(table)
    for joinFilter in nodeHead.joinFilterList:
        joinFilterList.add(joinFilter)
    for outerJoinFilter in nodeHead.outerJoinFilterList:
        outerJoinFilterList.add(outerJoinFilter)
    for commonFilter in nodeHead.commonFilterList:
        commonFilterList.add(commonFilter)
    nodeHead.filterNode = FilterNode_SP(list(fieldList), list(tableList), list(joinFilterList), list(outerJoinFilterList), list(commonFilterList))

    if nodeHead.nodeType.find("Join") != -1 \
            or nodeHead.nodeType.find("Aggregate") != -1 \
                or nodeHead.nodeType.find("TakeOrderedAndProject") != -1:
        clusters.append(nodeHead)

    return cost, fieldList, tableList, joinFilterList, outerJoinFilterList, commonFilterList, nodeHead.isSubGraph


# 取表和join条件的并集，删除其它条件
def remove_table_redundant(nodeHead):
    tableSet = set()
    joinTableSet = set()
    # 先取tableList的表
    for table in nodeHead.filterNode.tableList:
        if table.find(" AS ") != -1:
            table = table.split(" AS ")[1]
        tableSet.add(table)
    # 再取joinFilterList的表
    for joinFilter in nodeHead.filterNode.joinFilterList:
        pattern = r"[\w]+\.[\w]+"
        key = re.findall(pattern, joinFilter)
        if len(key) == 2:
            lValue, rValue = key[0].strip(), key[1].strip()
            ltable, rtable = lValue.split(".")[0].strip(), rValue.split(".")[0].strip()
            joinTableSet.add(ltable)
            joinTableSet.add(rtable)
    # 最后取outerJoinFilterList的表
    for joinFilter in nodeHead.filterNode.outerJoinFilterList:
        pattern = r"[\w]+\.[\w]+"
        key = re.findall(pattern, joinFilter[0])
        if len(key) == 2:
            lValue, rValue = key[0].strip(), key[1].strip()
            ltable, rtable = lValue.split(".")[0].strip(), rValue.split(".")[0].strip()
            joinTableSet.add(ltable)
            joinTableSet.add(rtable)

    # 删除tableList多余的table
    tableList = list(tableSet & joinTableSet)
    i = 0
    while i < len(nodeHead.filterNode.tableList):
        table = nodeHead.filterNode.tableList[i]
        if table.find(" AS ") != -1:
            table = table.split(" AS ")[1]
        if table not in tableList:
            del nodeHead.filterNode.tableList[i]
        else:
            i += 1

    # 删除多余的表
    deleteTable = list((tableSet - joinTableSet) | (joinTableSet - tableSet))
    # 删除fieldList
    i = 0
    while i < len(nodeHead.filterNode.fieldList):
        field = nodeHead.filterNode.fieldList[i]
        pattern = r"[\w]+\.[\w]+"
        keyList = re.findall(pattern, field)
        flag = True
        for key in keyList:
            table = key.split(".")[0].strip()
            if table not in tableList:
                del nodeHead.filterNode.fieldList[i]
                flag = False
                break
        if flag:
            i += 1
    # 删除joinFilterList
    i = 0
    while i < len(nodeHead.filterNode.joinFilterList):
        filter = nodeHead.filterNode.joinFilterList[i]
        pattern = r"[\w]+\.[\w]+"
        keyList = re.findall(pattern, filter)
        flag = True
        for key in keyList:
            table = key.split(".")[0].strip()
            if table not in tableList:
                del nodeHead.filterNode.joinFilterList[i]
                flag = False
                break
        if flag:
            i += 1
    # 删除commonFilterList
    i = 0
    while i < len(nodeHead.filterNode.commonFilterList):
        filter = nodeHead.filterNode.commonFilterList[i]
        pattern = r"[\w]+\.[\w]+"
        keyList = re.findall(pattern, filter)
        flag = True
        for key in keyList:
            table = key.split(".")[0].strip()
            if table not in tableList:
                del nodeHead.filterNode.commonFilterList[i]
                flag = False
                break
        if flag:
            i += 1
    # 删除outerJoinFilterList
    i = 0
    while i < len(nodeHead.filterNode.outerJoinFilterList):
        filter = nodeHead.filterNode.outerJoinFilterList[i][0]
        pattern = r"[\w]+\.[\w]+"
        keyList = re.findall(pattern, filter)
        flag = True
        for key in keyList:
            table = key.split(".")[0].strip()
            if table not in tableList:
                del nodeHead.filterNode.outerJoinFilterList[i]
                flag = False
                break
        if flag:
            i += 1


# 如果某个表只出现一次，那么无需别名
def remove_table_aliasName(nodeHead):
    tableSet = set()
    tableAlias = set()
    for table in nodeHead.filterNode.tableList:
        if table.find(" AS ") != -1:
            table = table.split(" AS ")[0]
        if table not in tableSet:
            tableSet.add(table)
        else:
            tableAlias.add(table)
    # 处理
    i = 0
    while i < len(nodeHead.filterNode.tableList):
        table = nodeHead.filterNode.tableList[i]
        if table.find(" AS ") != -1:
            originalTable, aliasTable = table.split(" AS ")[0:2]
            originalTable, aliasTable = originalTable.strip(), aliasTable.strip()
            if originalTable not in tableAlias:
                nodeHead.filterNode.tableList[i] = originalTable
                # 修改列
                j = 0
                while j < len(nodeHead.filterNode.fieldList):
                    nodeHead.filterNode.fieldList[j] = nodeHead.filterNode.fieldList[j].replace(aliasTable, originalTable)
                    j += 1
                # 修改joinFilterList
                j = 0
                while j < len(nodeHead.filterNode.joinFilterList):
                    nodeHead.filterNode.joinFilterList[j] = nodeHead.filterNode.joinFilterList[j].replace(aliasTable, originalTable)
                    j += 1
                # 修改commonFilterList
                j = 0
                while j < len(nodeHead.filterNode.commonFilterList):
                    nodeHead.filterNode.commonFilterList[j] = nodeHead.filterNode.commonFilterList[j].replace(
                        aliasTable, originalTable)
                    j += 1
        i += 1


# 对于出现的相同列取别名
def check_equal_column_alias(nodeHead):
    fieldDict = {}
    for i, field in enumerate(nodeHead.filterNode.fieldList):
        pattern = r"^[\w]+\.[\w]+"
        key = re.findall(pattern, field)
        if len(key) == 0:
            continue

        tableName, fieldName = key[0].split(".")[0:2]
        tableName, fieldName = tableName.strip(), fieldName.strip()
        if fieldName not in fieldDict:
            fieldDict[fieldName] = [(field + " AS " + tableName + "_" + fieldName, i)]
        else:
            fieldDict[fieldName].append((field + " AS " + tableName + "_" + fieldName, i))

    for key, value in fieldDict.items():
        if len(value) > 1:
            for tupleValue in value:
                nodeHead.filterNode.fieldList[tupleValue[1]] = tupleValue[0]


# 如果某个表没有join起来，将其field和commonFilter删除
def CheckTableWithoutJoin(nodeHead):
    filterNode = nodeHead.filterNode
    # 先记录join条件中出现的table
    tableSet = set()
    for i, joinFilter in enumerate(filterNode.joinFilterList):
        pattern = r"([\w]+)\.[\w]+"
        key = re.findall(pattern, joinFilter)
        if len(key) < 2:
            print("[CheckTableWithoutJoin] joinFilter error {}".format(joinFilter))
            continue
        # 自身和自身关联有问题
        if key[0] == key[1]:
            del filterNode.joinFilterList[i]
            continue
        tableSet.add(key[0])
        tableSet.add(key[1])

    # 遍历table，找到未被join的表
    checkTableSet = set()
    for i, table in enumerate(filterNode.tableList):
        if table.find(" AS ") != -1:
            table = table.split(" AS ")[1].strip()
        if table not in tableSet:
            checkTableSet.add(table)
            del filterNode.tableList[i]


    # 将不存在的table对应的字段删除
    i = 0
    while i < len(filterNode.fieldList):
        pattern = r"([\w]+)\.[\w]+"
        key = re.findall(pattern, filterNode.fieldList[i])
        flag = False
        for checkField in key:
            if checkField in checkTableSet or checkField not in tableSet:
                del filterNode.fieldList[i]
                flag = True
                break
        if not flag:
            i += 1

    # 将commonFilter不存在的参数删除
    i = 0
    while i < len(filterNode.commonFilterList):
        pattern = r"([\w]+)\.[\w]+"
        key = re.findall(pattern, filterNode.commonFilterList[i])
        flag = False
        for checkField in key:
            if checkField in checkTableSet:
                del filterNode.commonFilterList[i]
                flag = True
                break
        if not flag:
            i += 1

    return len(tableSet)


# 如果表只出现一次，那么无需别名
def CheckTableAliasName(nodeHead):
    filterNode = nodeHead.filterNode
    tableSet = set()
    tableDict = {}
    # 检查表
    for table in filterNode.tableList:
        if table.find(" AS ") == -1:
            continue
        tableName, aliasName = table.split(" AS ")[0:2]
        if tableName not in tableDict:
            tableDict[tableName] = 1
        else:
            tableDict[tableName] += 1
    # 第二次扫描表
    for i, table in enumerate(filterNode.tableList):
        if table.find(" AS ") == -1:
            continue
        tableName, aliasName = table.split(" AS ")[0:2]
        # 某表只出现一次但是却取别名
        if tableDict[tableName] == 1:
            filterNode.tableList[i] = tableName
            for j, table in enumerate(filterNode.fieldList):
                filterNode.fieldList[j] = table.replace(aliasName, tableName)
            for j, joinFilter in enumerate(filterNode.joinFilterList):
                filterNode.joinFilterList[j] = joinFilter.replace(aliasName, tableName)
            for j, commonFilter in enumerate(filterNode.commonFilterList):
                filterNode.commonFilterList[j] = commonFilter.replace(aliasName, tableName)


# 扩充列，将sql中出现的所有字段都加入
def CheckTableField(nodeHead, tableField):
    filterNode = nodeHead.filterNode
    fieldSet = set(filterNode.fieldList)
    for table in filterNode.tableList:
        if table.find(" AS ") != -1:
            tableName, aliasName = table.split(" AS ")[0:2]
        else:
            tableName = aliasName = table
        if tableName == "":
            continue
        for field in tableField[tableName].values():
            fieldSet.add(aliasName + "." + field["field"])

    filterNode.fieldList = list(fieldSet)


# 将filter中出现的其它表的信息剔除
def CheckFilterOtherField(nodeHead):
    tableList = []
    for table in nodeHead.filterNode.tableList:
        tableList.append(table)
    # 删除joinFilterList
    i = 0
    while i < len(nodeHead.filterNode.joinFilterList):
        filter = nodeHead.filterNode.joinFilterList[i]
        pattern = r"([\w]+)\.([\w]+)"
        keyList = re.findall(pattern, filter)
        flag = True
        for key in keyList:
            table, field = key[0], key[1]
            if is_number(table) or is_number(field):
                continue
            if table not in tableList:
                del nodeHead.filterNode.joinFilterList[i]
                flag = False
                break
        if flag:
            i += 1
    # 删除commonFilterList
    i = 0
    while i < len(nodeHead.filterNode.commonFilterList):
        filter = nodeHead.filterNode.commonFilterList[i]
        pattern = r"([\w]+)\.([\w]+)"
        keyList = re.findall(pattern, filter)
        flag = True
        for key in keyList:
            table, field = key[0], key[1]
            if is_number(table) or is_number(field):
                continue
            if table not in tableList:
                del nodeHead.filterNode.commonFilterList[i]
                flag = False
                break
        if flag:
            i += 1


# 其它过滤，将仍然带#号的条件直接删除
def CheckOtherFilter(nodeHead):
    i = 0
    while i < len(nodeHead.filterNode.commonFilterList):
        pattern = r"[\w]+#[0-9L]+"
        key = re.findall(pattern, nodeHead.filterNode.commonFilterList[i])
        if len(key) > 0:
            del nodeHead.filterNode.commonFilterList[i]
        else:
            i += 1


# 特殊处理: 如果不存在表但存在该表的参数，将这个参数剔除
def DeleteFieldWithEmptyTable(nodeHead, tableDict):
    filterNode = nodeHead.filterNode
    j = 0
    while j < len(filterNode.filterList):
    # for filterStr in filterNode.filterList:
        pattern = r"([\w]+).([\w]+)[ ]*=[ ]*([\w]+).([\w]+)"
        key = re.findall(pattern, filterNode.filterList[j])
        if len(key) > 0:
            table1, field1, table2, field2 = key[0][0], key[0][1], key[0][2], key[0][3]
            if table1 == table2 and field1 == field2:
                pattern = r"([a-zA-Z_]+)([0-9]+)"
                key = re.findall(pattern, table1)
                if len(key) > 0:
                    table = key[0][0]
                    curNum = int(key[0][1])
                    totalNum = tableDict[table]
                    i = 1
                    modifyNum = 1
                    while i <= totalNum:
                        if curNum != i:
                            modifyNum = i
                            break
                    filterNode.filterList[j] = table + str(modifyNum) + "." + field1 + " = " + table2 + "." + field2
        j = j + 1


def getStripNodeType(nodeType):
    if nodeType.find("(") != -1:
        key = nodeType.split("(")
        nodeType = key[0].strip()
    return nodeType


# 删除promote_precision
def remove_promote_precision(inputString, removeString="promote_precision("):
    index = inputString.find(removeString)
    while index != -1:
        inputString = inputString[0:index] + inputString[index + len(removeString):]
        i = index
        bracket = 0
        while i < len(inputString):
            ch = inputString[i]
            if ch == "(":
                bracket += 1
            elif ch == ")":
                if bracket == 0:
                    inputString = inputString[0:i] + inputString[i + 1:]
                    break
                else:
                    bracket -= 1
            i += 1

        index = inputString.find(removeString)

    return inputString


# 删除as decimal(19,2)等
def remove_unuse_parameter(inputString):
    inputString = re.sub("[ ]*as[ ]*decimal\([0-9]+,[0-9]+\)\)", "", inputString)
    inputString = re.sub("[ ]*AS[ ]*DECIMAL\([0-9]+,[0-9]+\)\)", "", inputString)
    inputString = re.sub(",[ ]*DecimalType\([0-9]+,[0-9]+\),[ ]*true\)", "", inputString)
    inputString = re.sub("CheckOverflow\(", "", inputString)
    inputString = re.sub("[ ]*as[ ]*double\)", "", inputString)
    inputString = re.sub("[ ]*as[ ]*bigint\)", "", inputString)
    inputString = re.sub("cast\(", "", inputString)
    inputString = re.sub("CAST\(", "", inputString)
    inputString = remove_promote_precision(inputString, "promote_precision(")
    inputString = remove_promote_precision(inputString, "UnscaledValue(")

    return inputString


# 将partial_sum等参数变为sum
def remove_unuse_agg(inputString):
    inputString = re.sub("partial_sum", "sum", inputString)
    inputString = re.sub("partial_count", "count", inputString)
    inputString = re.sub("partial_avg", "avg", inputString)
    inputString = re.sub("merge_sum", "sum", inputString)
    inputString = re.sub("merge_avg", "avg", inputString)
    inputString = re.sub("merge_count", "count", inputString)
    inputString = re.sub("partial_stddev_samp", "count", inputString)

    return inputString


# 使用relationDict将原名的*#*替换为.*.
def replace_with_relations(replaceString, relationDict, aliasDict):
    pattern = r"([\w]+)#([0-9L]+)"
    keyList = re.findall(pattern, replaceString)
    tableList = []
    if len(keyList) == 0:
        return "", []
    for key in keyList:
        field, num = key[0], key[1]
        if num not in relationDict:
            print("[replace_with_relations] can not find num:{0} replaceString:{1}".format(num, replaceString))
            return "", []
        replaceString = replaceString.replace(field + "#" + num, relationDict[num].displayName)
        tableList.append(relationDict[num].table)

    return replaceString, tableList


# 使用relationDict和aliasDict将原名的*#*替换为.*.
def replace_with_relations_alias(replaceString, relationDict, aliasDict):
    pattern = r"([\w]+)#([0-9L]+)"
    keyList = re.findall(pattern, replaceString)
    tableSet = set()
    for key in keyList:
        field, num = key[0], key[1]
        if num in relationDict:
            replaceString = replaceString.replace(field + "#" + num, relationDict[num].displayName)
            tableSet.add(relationDict[num].table)
        elif num in aliasDict:
            replaceString = replaceString.replace(field + "#" + num, aliasDict[num]["displayName"])

    return replaceString, list(tableSet)


# 处理filter信息，利用括号层级信息提取
def extract_bracket_filter(filterString):
    i = 0
    length = len(filterString)
    depth = 0
    curString = ""
    bracketList = []
    while i < length:
        ch = filterString[i]
        if ch == "(":
            depth += 1
        elif ch == ")":
            curString = curString.strip()
            bracketList.append([curString, depth])
            depth -= 1
            curString = ""
        else:
            curString += ch
        # 处理AND、OR
        if curString[-4:] == " OR ":
            curString = curString[:-4]
            if curString != "":
                bracketList.append([curString, depth])
            bracketList.append(["OR", depth, "OR"])
            curString = ""
        elif curString[-5:] == " AND ":
            curString = curString[:-5]
            if curString != "":
                bracketList.append([curString, depth])
            bracketList.append(["AND", depth, "AND"])
            curString = ""

        i += 1
    return bracketList


# 去掉project解析的错误
def remove_error_pattern_project(replaceString):
    replaceString = remove_unuse_parameter(replaceString)
    pattern = r"[\w()]+#[0-9L]+"
    pa = re.findall(pattern, replaceString)
    if len(pa) > 0:
        return ""
    if replaceString == "substr(":
        return ""
    if replaceString.find("shiftright") != 1:
        return ""

    return replaceString


# 检查括号的问题
def check_bracket_match(nodeHead):
    # 先检查列
    i = 0
    while i < len(nodeHead.filterNode.fieldList):
        flag, curString = check_one_string_bracket(nodeHead.filterNode.fieldList[i])
        if not flag:
            print("warn: [check_bracket_match] str bracket not match! {}".format(nodeHead.filterNode.fieldList[i]))
            del nodeHead.filterNode.fieldList[i]
        else:
            nodeHead.filterNode.fieldList[i] = curString
            i += 1

    # 再检查filter
    i = 0
    while i < len(nodeHead.filterNode.commonFilterList):
        flag, curString = check_one_string_bracket(nodeHead.filterNode.commonFilterList[i])
        if not flag:
            print("warn: [check_bracket_match] str bracket not match! {}".format(nodeHead.filterNode.commonFilterList[i]))
            del nodeHead.filterNode.commonFilterList[i]
        else:
            nodeHead.filterNode.commonFilterList[i] = curString
            i += 1


# 检查一个括号的匹配情况
def check_one_string_bracket(checkString):
    i = 0
    bracket = 0
    while i < len(checkString):
        ch = checkString[i]
        if ch == "(":
            bracket += 1
        elif ch == ")":
            bracket -= 1
            # 出现错误
            if bracket < 0:
                checkString = checkString[0:i] + checkString[i + 1:]
                bracket += 1
                continue
        i += 1

    return bracket == 0, checkString


# 判断属性是否是日期类型
def is_filter_date(num, relationDict, tableField):
    if not relationDict.get(num):
        return False
    table, field = relationDict[num].table, relationDict[num].field
    if table.find(" AS ") != -1:
        table = table.split(" AS ")[0]
    if not tableField.get(table):
        return False
    if not tableField[table].get(field):
        return False

    if tableField[table][field]["type"] == "date":
        return True
    return False


# z3处理时将日期格式改为数字，这里处理回来
def transform_date_format(condition):
    pattern = r"([\w]+)\.([\w]+)[ ]*[><=]+[ ]*([0-9]+)"
    keyList = re.findall(pattern, condition)
    if len(keyList) == 0:
        return condition
    for key in keyList:
        table, field, num = key[0], key[1], key[2]
        table, field, num = table.strip(), field.strip(), num.strip()
        table = get_table_without_alias(table)
        if table.upper() not in g_table.data:
            return condition
        if field.upper() not in g_table.data[table.upper()]:
            return condition
        if g_table.data[table.upper()][field.upper()].find("DATE") != -1:
            condition = condition.replace(num, "cast('1970-01-01' as date) + interval " + num + " days")

    return condition


# 将别名去掉
def get_table_without_alias(table):
    pattern = r"[^0-9]+"
    key = re.findall(pattern, table)
    if len(key) == 0:
        return table
    return key[0]


# 验证括号问题
def check_bracket_equal(condition):
    bracket = 0
    for ch in condition:
        if ch == "(":
            bracket += 1
        elif ch == ")":
            bracket -= 1
        if bracket < 0:
            return False
    return True


# 根据列名从g_table里查找
def search_project_from_table(key):
    for table, fieldDict in g_table.data.items():
        for field in fieldDict.keys():
            if key.upper() == field:
                return table.lower()
    return ""


# 根据num和field将#处理为.形式——通用
def replace_original_general(num, field, relationDict, aliasDict):
    # 1.先从relationDict中查找
    if relationDict.get(num):
        return relationDict[num].table, relationDict[num].field, relationDict[num].displayName
    # 2.从全表中查找
    table = search_project_from_table(field)
    if table != "":
        return table, field, table + "." + field
    # 3.从aliasDict中查找
    if aliasDict.get(num):
        return aliasDict[num]["table"], aliasDict[num]["field"], aliasDict[num]["displayName"]

    return "", "", ""


# 处理列
def add_group_project(nodeHead):
    filter_set = set()
    for filter in nodeHead.filterNode.commonFilterList:
        pattern = r"([\w]+\.[\w]+)"
        keys = re.findall(pattern, filter)
        if len(keys) > 0:
            for key in keys:
                filter_set.add(key)
    nodeHead.group_by_list = list(set(nodeHead.group_by_list) | filter_set)