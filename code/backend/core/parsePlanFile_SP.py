import re

from parseFilterSpecialCase_SP import *
from printTree_SP import *

'''
1.解析.txt后缀的文件，将表的每个字段和其表名对应起来
'''
class Table2FieldNode(object):
    def __init__(self, field, table, displayName):
        self.field = field
        self.table = table
        self.displayName = displayName

    def __str__(self):
        return self.field + " " + self.table + " " + self.displayName


class PlanFileTree(object):
    def __init__(self, nodeType, num, retractCnt):
        self.nodeType = nodeType
        self.num = num
        self.retractCnt = retractCnt
        self.children = []
        self.left = None
        self.right = None
        self.father = None
        self.dot = Digraph(comment="SparkNode")

    def __str__(self):
        return self.num + " name: " + str(self.nodeType)

    def printTree(self, save_path="./PlanTree.gv"):
        colors = ["skyblue", "tomato", "orange", "purple", "green", "yellow", "pink"]

        def printNode(node, nodeTag):
            color = sample(colors, 1)[0]
            for child in node.children:
                if len(child.children) == 0:
                    self.dot.node(child.num, str(child).replace("), ", "),\n"),
                                  style="filled", color=color, shape='rectangle', width='2')
                # self.dot.edge(node.left.tag, node.left.ancestor.tag, label="ancestor", style="dashed")
                else:
                    self.dot.node(child.num, str(child).replace("), ", "),\n"),
                                  style="filled", color=color, width='2')
                self.dot.edge(nodeTag, child.num)
                # self.dot.edge(node.left.tag, nodeTag, label="ancestor", style="dashed")
                # if node.left.visited is False:
                printNode(child, child.num)

        if self.num is not None:
            self.dot.node(self.num, str(self), style="filled", color=sample(colors, 1)[0], shape='octagon')
            printNode(self, self.num)

        self.dot.render(save_path)


# 将plan文件构建一棵树
def buildTreeFromPlanFile(planFile):
    nodeHead = None
    lastNode = None
    isMainTree = True
    isEveryNode = True
    nodeDict = {} # 记录所有结点 id->node
    reuseNodeDict = {} # ReusedExchange的id->reuseId
    subQueryDict = {}
    with open(planFile, encoding="utf-8") as f:
        while True:
            line = f.readline()
            if not line:
                break
            if line == "\n":
                isMainTree = False
            if line.find("Subqueries") != -1:
                isEveryNode = False
            line = line.strip("\n")
            #print(line)

            # 1.先构建主树
            if isMainTree:
                nodeHead, lastNode = buildPlanMainTree(line, lastNode, nodeHead, nodeDict)
                continue

            # 2.记录每个结点
            if isEveryNode:
                gatherEveryNode(line, reuseNodeDict)
                continue

            # 3.处理subquery
            subQueryDict = buildPlanSubTree(f, line)

            #print(subQueryDict)



    # 4.处理下ReusedExchange
    for id, reuseId in reuseNodeDict.items():
        father = nodeDict[id].father
        if father.left == nodeDict[id]:
            father.left = nodeDict[reuseId]
        else:
            father.right = nodeDict[reuseId]
        # 修改dot画图
        i = 0
        while i < len(father.children):
            if father.children[i] == nodeDict[id]:
                father.children[i] = nodeDict[reuseId]
                break
            i += 1

    # print(nodeHead)

    return nodeHead, subQueryDict


# 搭建主树
def buildPlanMainTree(line, lastNode, nodeHead, nodeDict):
    nodeType, num, retractCnt = separateBlankNode(line)
    if nodeType == "":
        return nodeHead, None
    # print(nodeType)
    # print(num)
    # print(retractCnt)
    # 树的节点
    curNode = PlanFileTree(nodeType, num, retractCnt)
    nodeDict[num] = curNode
    if nodeHead == None:
        nodeHead = curNode
    if lastNode == None:
        lastNode = curNode
        return nodeHead, curNode

    if curNode.retractCnt > lastNode.retractCnt:  # 是父子关系
        curNode.father = lastNode
        if lastNode.left:
            lastNode.right = curNode
        else:
            lastNode.left = curNode
        lastNode.children.append(curNode)
    else:
        # 找到父亲
        fatherNode = lastNode
        while fatherNode and curNode.retractCnt <= fatherNode.retractCnt:
            fatherNode = fatherNode.father
        if fatherNode:
            curNode.father = fatherNode
            if fatherNode.left:
                fatherNode.right = curNode
            else:
                fatherNode.left = curNode
            fatherNode.children.append(curNode)

    lastNode = curNode
    return nodeHead, lastNode


# 收集每个结点
def gatherEveryNode(line, reuseNodeDict):
    pattern = r"\(([0-9]+)\)[ ]*([\w .]+)"
    key = re.findall(pattern, line)
    if len(key) == 0:
        return
    num, nodeType = key[0][0].strip(), key[0][1].strip()

    if nodeType == "ReusedExchange":
        # 找到reuseId
        pattern = r"\[[\w ]+:[ ]*([0-9]+)\]"
        key = re.findall(pattern, line)
        if len(key) == 0:
            return
        reuseId = key[0]
        reuseNodeDict[num] = reuseId


# 处理subquery
def buildPlanSubTree(f, line):
    subQueryDict = {}
    while True:
        line = f.readline()
        if not line:
            break
        pattern = r"Subquery:([0-9]+)[ ]*[\w ]+=[ ]*([0-9]+)[ ]*[\w ]+=[ ]*([\w\-# ]+)"
        key = re.findall(pattern, line)
        if len(key) == 0:
            continue

        subQueryId, HostingId, expression = key[0][0], key[0][1], key[0][2].strip()
        nodeHead = None
        lastNode = None
        nodeDict = {}  # 记录所有结点 id->node

        tmp = line.split(" Subquery ")
        if len(tmp) < 2:
            return subQueryDict

        key = line.split(" Subquery ")[1]
        subqueryData = key.split(",")[0].strip()

        # 1.提取树的关系
        while True:
            line = f.readline()
            if not line:
                break
            if line == "\n":
                break

            nodeHead, lastNode = buildPlanMainTree(line, lastNode, nodeHead, nodeDict)

        subSelect = ""
        # 2.提取信息
        if nodeHead and nodeHead.nodeType != "ReusedExchange":
            projectList = []
            groupByList = []
            tableList = []
            filterList = []
            while True:
                line = f.readline()
                if not line:
                    break

                # 参数



                pattern = r"\(([0-9]+)\)[ ]*([\w .]+)"
                key = re.findall(pattern, line)
                if len(key) > 0:

                    id, nodeType = key[0][0].strip(), key[0][1].strip()
                    if nodeType == "Project":
                        line = f.readline()
                        pattern = r"([\w]+)#([0-9L]+)"
                        keyList = re.findall(pattern, line)
                        for key in keyList:
                            field, num = key[0].strip(), key[1].strip()
                            projectList.append(field)
                    elif nodeType == "HashAggregate":
                        projectList = []
                        line = f.readline()
                        line = f.readline()
                        pattern = r"([\w]+)#([0-9L]+)"
                        keyList = re.findall(pattern, line)
                        for key in keyList:
                            field, num = key[0].strip(), key[1].strip()
                            groupByList.append(field)
                        line = f.readline()
                        line = line.split(":")[1]
                        pattern = r"([\w()#]+)"
                        keyList = re.findall(pattern, line)
                        for key in keyList:
                            projectList.append(deletePlanBracketValue(key))
                    elif nodeType == "Filter":
                        line = f.readline()
                        line = f.readline()
                        line = line.split(":")[1].strip()
                        keyList = line.split("AND")
                        for key in keyList:
                            key = key.strip()
                            if key.find("isnotnull") != -1:
                                continue
                            elif key.find("isnull") != -1:
                                pattern = r"([\w]+)#[0-9L]+"
                                reList = re.findall(pattern, key)
                                for reStr in reList:
                                    filterList.append(reStr + " is null")
                            else:
                                key = key.strip("(").strip(")")
                                key = re.sub(r"#[0-9L]+", "", key)
                                filterList.append(key)

                    elif nodeType.find("Scan orc") != -1:
                        table = nodeType.split(".")[1]
                        tableList.append(table)


            # 将subquery信息整理
            if projectList == [] or tableList == [] or filterList == []:
                return
            subSelect += "select "
            for field in projectList:
                subSelect += field + ", "
            subSelect = subSelect[:-2]
            subSelect += " from "
            for table in tableList:
                subSelect += table + ", "
            subSelect = subSelect[:-2]
            subSelect += " where "
            for filter in filterList:
                subSelect += filter + " AND "
            subSelect = subSelect[:-5]
            if len(groupByList) > 0:
                subSelect += " group by "
                for groupBy in list(set(groupByList)):
                    subSelect += groupBy + ", "
                subSelect = subSelect[:-2]
        if subSelect != "":
            subQueryDict[subqueryData] = subSelect

    return subQueryDict


def separateBlankNode(line):
    i = 0
    isAlpha = False # 是否遇到字母
    isBracket = False # 是否遇到(
    retractCnt = 0
    nodeType = ""
    tmpStr = ""
    num = ""
    while i < len(line):
        ch = line[i]
        # 1.先计算缩进的数量
        if not isAlpha:
            if ch.isalpha():
                isAlpha = True
            else:
                retractCnt += 1
                i = i + 1
            continue
        # 2.记录算子名称，遇到(停止，如果没有遇到(说明有误
        if not isBracket:
            if ch == "(":
                nodeType = delete_invalid_token(tmpStr)
                tmpStr = ""
                isBracket = True
            else:
                tmpStr += ch
            i += 1
            continue
        # 3.计算
        if ch == ")":
            num = tmpStr
        else:
            tmpStr += ch

        i += 1

    if not isAlpha and not isBracket:
        return "", "", 0

    return nodeType, num, retractCnt


def delete_invalid_token(tmpStr):
    tmpStr = tmpStr.strip()
    ansStr = ""
    for ch in tmpStr:
        if ch.isalpha():
            ansStr += ch
        else:
            break
    return ansStr


def deletePlanBracketValue(processedStr):
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
                        curStr = curStr.replace(key, field)
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

    return ansStr


# 在plan计划中处理HashAggregate算子
def parse_plan_HashAggregate(f, relationDict, tableDict, aliasDict):
    keyTableList = []
    while True:
        line = f.readline()
        if not line:
            break
        if line == "\n":
            break
        # 匹配Input那行，获取各个字段
        if line.find("Input") != -1:
            data = line.split("[")
            if len(data) < 3:
                continue
            data = data[2].strip().strip("]")
            # 找到所有的类似于： sum#250
            pattern = r"([\w]+)#([0-9L]+)"
            key = re.findall(pattern, data)
            for info in key:
                addNum = info[1].strip()
                keyTableList.append(addNum)
            continue

        # 匹配Functions那一行
        if line.find("Functions") != -1:
            pattern = r"([\w]+)#([0-9L]+)"
            key = re.findall(pattern, line)
            for info in key:
                field = info[0].strip()
                num = info[1].strip()
                if relationDict.get(num):
                    continue
                for tableNum in keyTableList:
                    if relationDict.get(tableNum):
                        relationDict[num] = Table2FieldNode(
                            field=field,
                            table=relationDict[tableNum].table,
                            displayName=relationDict[tableNum].table + "." + field)
                        break

            continue

        pattern = r"Results[ ]+\[[0-9]+\]:[ ]+\[(.*)\]"
        data = re.findall(pattern, line)
        if len(data) == 0:
            continue
        type = data[0]
        # 这时候已经定位到Results那一行了
        ansList = parseBracketFilter(type)
        for typeStr in ansList:
            # 预处理
            typeStr = re.sub(r"[ ]*as[ ]*decimal\([0-9L ]+,[ 0-9L]\)", "",
                             typeStr)  # 去掉 as decimal(8,2)
            typeStr = re.sub(r"DecimalType\([0-9L ]+,[0-9L ]+\)", "DecimalType",
                             typeStr)  # 去掉DecimalType(8,2)
            typeStr = re.sub(r"[ ]*[Aa][Ss][ ]*DECIMAL\([0-9L ]+,[0-9L ]+\)", "",
                             typeStr)  # 去掉 AS DECIMAL(21,6)
            typeStr = typeStr.replace("as double", "")
            typeStr = typeStr.replace("as bigint", "")
            typeStr = typeStr.replace("AS DOUBLE", "")
            if typeStr.find("AS") == -1:
                continue

            # 正式解析
            # 1.处理普通的，比如：c_customer_id#41 AS customer_id#10
            pattern = r"([\w_\(\)]+)#([0-9L]+)[ ]*AS[ ]*([\w_\(\)]+)#([0-9L]+)"
            key = re.findall(pattern, typeStr)
            if len(key) > 0:
                for info in key:
                    field = info[0]
                    num = info[1]
                    if not relationDict.get(num):
                        continue
                    aliasField = info[2]
                    aliasNum = info[3]
                    relationDict[aliasNum] = relationDict[num]
                continue

            # 2.处理特殊的
            pattern = r"[\w]+\(([\w]+)\([\w]+\(([\w]+)#([0-9L]+)\)\).*AS[ ]*([\w_]+)#([0-9L]+)"
            key = re.findall(pattern, typeStr)
            if len(key) > 0:
                for info in key:
                    aggr = info[0]
                    field = info[1]
                    num = info[2]
                    if not relationDict.get(num):
                        ok = AddRelationDictFromExisting(num, field, relationDict)
                        if not ok:
                            print("[getOneRelationTable] get num error! num:{}".format(num))
                            continue
                    aliasField = info[3]
                    aliasNum = info[4]
                    relationDict[aliasNum] = Table2FieldNode(
                        aggr + "(" + relationDict[num].table + "." + field + ")",
                        relationDict[num].table, relationDict[num].displayName)
                continue

            # 3.处理更加复杂的逻辑——avg(xxx) as cnt
            bracketStr, otherStr = re.split(r"[ ]+AS[ ]+", typeStr)
            ok, ansStr, ansList = deleteBracketValue(bracketStr, relationDict, aliasDict)
            if not ok:
                continue
            if otherStr.find("#") != -1:
                # print(otherStr)
                # 处理别名
                pattern = r"([\w\(\)#]+)#([0-9L]+)$"
                key = re.findall(pattern, otherStr)
                if len(key) == 0:
                    continue
                field = key[0][0].strip()
                num = key[0][1].strip()
                err, displayName = parseAggrOrfield(field, relationDict, tableDict)
                if err != "":
                    print("[getOneRelationTable] error:{}".format(err))
                    continue
                displayfield = ansStr + " AS " + displayName
                if len(ansList) == 0:
                    relationDict[num] = Table2FieldNode(displayfield, "", ansStr)
                else:
                    relationDict[num] = Table2FieldNode(displayfield, ansList[0], ansStr)



# 处理plan的Project算子，需要修改
def parse_plan_Project(f, relationDict, aliasDict):
    line = f.readline()
    if not line:
        return
    if len(line.split("[")) < 3:
        return

    key = line.split("[")[2].strip().strip("[").strip("]")
    keyList = parseBracketFilter(key)
    for key in keyList:
        if key.find(" AS ") == -1:
            continue
        # 先把特殊格式删除，如： as decimal(19,2)
        key = remove_unuse_parameter(key)
        # 将两者分割
        originalString, aliasString = key.split(" AS ")[0:2]

        # 将原名的*#*替换为*.*
        originalString, tableList = replace_with_relations(originalString, relationDict, aliasDict)
        if originalString == "":
            continue

        # 先将原始的#分割
        fieldString, num = aliasString.split("#")
        fieldString, num = fieldString.strip(), num.strip()
        aliasDict[num] = {"num": num, "table": tableList[0],"field": fieldString, "displayName": originalString}


# 处理plan的Window算子
def parse_plan_Window(f, relationDict):
    line = f.readline()
    if not line:
        return
    line = f.readline()
    if not line:
        return
    key = line.split("Arguments: ")[1]
    # 处理rank over形式
    displayName = ""
    asNum = ""
    tableSet = set()  # 记录出现的表
    if key.find("rank") != -1 and key.find("windowspecdefinition") != -1:
        pattern = r"(\[[\w, #$()]+\])"
        ansList = re.findall(pattern, key)
        if len(ansList) == 2:
            asNumList = ansList[0].strip("]").split("#")
            asNum = asNumList[len(asNumList) - 1]
            pattern = r"([\w]+)#([0-9L]+)"
            orderList = re.findall(pattern, ansList[1])
            orderbyStr = ""
            for orderStr in orderList:
                field = orderStr[0]
                num = orderStr[1]
                if not relationDict.get(num):
                    ok = AddRelationDictFromExisting(num, field, relationDict)
                    if not ok:
                        continue
                orderbyStr = orderbyStr + relationDict[num].displayName + ","
                tableSet.add(relationDict[num].table)
            # 处理结束
            orderbyStr = orderbyStr[:-1]  # 去掉最后的,
            displayName = "rank() over (order by " + orderbyStr + ")"

        elif len(ansList) == 3:
            asNumList = ansList[0].strip("]").split("#")
            asNum = asNumList[len(asNumList) - 1]
            # partition by
            pattern = r"([\w]+)#([0-9L]+)"
            orderList = re.findall(pattern, ansList[1])
            partitionbyStr = ""
            for orderStr in orderList:
                field = orderStr[0]
                num = orderStr[1]
                if not relationDict.get(num):
                    ok = AddRelationDictFromExisting(num, field, relationDict)
                    if not ok:
                        continue
                partitionbyStr = partitionbyStr + relationDict[num].displayName + ","
                tableSet.add(relationDict[num].table)
            partitionbyStr = partitionbyStr[:-1]  # 去掉最后的,
            # order by
            pattern = r"([\w]+)#([0-9L]+)"
            orderList = re.findall(pattern, ansList[2])
            orderbyStr = ""
            for orderStr in orderList:
                field = orderStr[0]
                num = orderStr[1]
                if not relationDict.get(num):
                    ok = AddRelationDictFromExisting(num, field, relationDict)
                    if not ok:
                        continue
                orderbyStr = orderbyStr + relationDict[num].displayName + ","
                tableSet.add(relationDict[num].table)
            orderbyStr = orderbyStr[:-1]  # 去掉最后的,
            displayName = "rank() over (partition by " + partitionbyStr + " order by " + orderbyStr + ")"
        else:
            print("[getOneRelationTable] Window parse error! {}".format(line))
            return
        tableList = list(tableSet)
        if len(tableList) == 0:
            print("[getOneRelationTable] Window tableSet is empty! {}".format(line))
            return
        relationDict[asNum] = Table2FieldNode(
            field=displayName,
            table=list(tableSet)[0],
            displayName=displayName
        )


# 处理plan的ReusedExchange算子
def parse_plan_ReusedExchange(f, num, reuseNodeDict):
    line = f.readline()
    if not line:
        return
    data = line.split("[")[2].strip().strip("]")
    # 找到所有的类似于： sum#250
    pattern = r"([\w]+)#([0-9L]+)"
    key = re.findall(pattern, data)
    infoList = []
    for info in key:
        addNum = info[1].strip()
        infoList.append(addNum)
    reuseNodeDict[num] = infoList


# 处理plan的Scan orc算子，需要提前处理
def parse_plan_Scanorc(f, line, relationDict, tableDict, aliasTableSet, tableField):
    pattern = r'\([0-9]+\).*Scan orc'
    ok = re.match(pattern, line)
    if not ok:
        return

    # 得到表名
    tableName = line.split(".")[1].strip()
    # 读取下一行
    line = f.readline()
    if not line:
        return
    # 不考虑重复的表
    # 得到字段
    try:
        fieldTmp = line.split("[")[2].split("]")[0]
    except:
        fieldTmp = ""
    if fieldTmp != "":
        for field in fieldTmp.split(","):
            field = field.strip()
            fieldName, num = field.split("#")[0:2]
            if not relationDict.get(num):
                relationDict[num] = Table2FieldNode(fieldName, tableName, tableName + "." + fieldName)
            # 将该条sql对应表用到的字段加入
            if tableName in tableField:
                if fieldName in tableField:
                    continue
                tableField[tableName][fieldName] = {"field": fieldName, "type": "bigint"}
            else:
                tableField[tableName] = {}
                if fieldName in tableField:
                    continue
                tableField[tableName][fieldName] = {"field": fieldName, "type": "bigint"}

    # 读取ReadSchema得到字段的数据类型
    while True:
        line = f.readline()
        if not line:
            break
        if line == "\n":
            break
        if line.find("ReadSchema: struct") == -1:
            continue
        line = line.split("ReadSchema: struct")[1]
        pattern = r"([\w]+)\:([\w]+)"
        keyList = re.findall(pattern, line)
        for key in keyList:
            field, type = key[0], key[1]
            tableField[tableName][field]["type"] = type


# 处理Exchange——两个表的字段可能共用，NodeMap
def manage_share_Exchange(f, line, NodeMap):
    pattern = r"^\(([0-9]+)\)[\w ]*Exchange"
    key = re.findall(pattern, line)
    if len(key) > 0 and line.find("ReusedExchange") == -1:
        num = key[0].strip()
        while True:
            line = f.readline()
            if not line:
                break
            if line == "\n":
                break
            pattern = r"^Input[ ]*\[[0-9]\]:[ ]*\[([\w0-9#, ]*)\]"
            key = re.findall(pattern, line)
            if len(key) == 0:
                break
            dataList = key[0].split(",")
            nodeList = []
            for data in dataList:
                data = data.strip()
                key1, key2 = data.split("#")
                key1 = key1.strip()
                key2 = key2.strip()
                nodeList.append(key2)  # 将数值记录
            NodeMap[num] = nodeList


# 处理ReusedExchange的情况
def manage_share_ReusedExchange(f, line, NodeMap, relationDict, tableDict):
    pattern = r"\([0-9]+\).*ReusedExchange[ ]*\[Reuses operator id: ([0-9]+)\]"
    key = re.findall(pattern, line)
    if len(key) == 0:
        return
    num = key[0].strip()
    if not NodeMap.get(num):
        print("[getOneRelationTable] can't find Reuses operator id!" + num)
        return
    line = f.readline()
    if not line:
        return

    pattern = r"Output[ \[\]0-9:]*([\w#, ]*)\]"
    key = re.findall(pattern, line)
    if len(key) == 0:
        return
    dataList = key[0].split(",")
    if len(dataList) != len(NodeMap[num]):
        print("[getOneRelationTable] Reuses operator len not equals dataList! " + str(
            len(dataList)) + "---" + str(len(NodeMap[num])))
        return
    i = 0
    while i < len(dataList):
        field, number = dataList[i].strip().split("#")
        field = field.strip()
        number = number.strip()
        compareNum = NodeMap[num][i]
        # 重复表计算
        if not relationDict.get(compareNum):
            i = i + 1
            continue
        if not tableDict.get(relationDict[compareNum].table):
            i = i + 1
            continue

        relationDict[number] = relationDict[compareNum]
        i = i + 1


# 该函数主要用于获取标号->表信息的对应关系
def getOneRelationTable(planFile):
    relationDict = {} # 列的标号->Table2FieldNode类
    aliasDict = {} # 列的标号->原名
    tableField = {}   # 表->字段
    tableDict = {}
    aliasTableSet = set()
    NodeMap = {} # 每个结点对应的map
    # 1.先把Scan orc和ReusedExchange的情况的情况处理
    with open(planFile, encoding="utf-8") as f:
        while True:
            line = f.readline()
            if not line:
                break

            # (1)处理Scan orc——字段和表的关联关系，主要在这处理
            parse_plan_Scanorc(f, line, relationDict, tableDict, aliasTableSet, tableField)

            # (2)处理Exchange——两个表的字段可能共用
            manage_share_Exchange(f, line, NodeMap)

            # (3)处理ReusedExchange的情况
            manage_share_ReusedExchange(f, line, NodeMap, relationDict, tableDict)


    # 2.再处理各个结点的信息
    reuseNodeDict = {}
    with open(planFile, encoding="utf-8") as f:
        while True:
            line = f.readline()
            if not line:
                break
            pattern = r"\(([0-9]+)\)[ ]+([a-zA-Z]+)"
            key = re.findall(pattern, line)
            if len(key) == 0:
                continue
            num, nodeType = key[0][0], key[0][1]
            # 下面主要处理各算子的AS情况
            if nodeType == "HashAggregate":
                parse_plan_HashAggregate(f, relationDict, tableDict, aliasDict)
            elif nodeType == "Project":
                parse_plan_Project(f, relationDict, aliasDict)
            elif nodeType == "Window":
                pass
            elif nodeType == "ReusedExchange":
                parse_plan_ReusedExchange(f, num, reuseNodeDict)

    # 4.可以暂时不用
    node, subQueryDict = buildTreeFromPlanFile(planFile)

    return relationDict, tableDict, subQueryDict, tableField, aliasDict



if __name__ == "__main__":

    node, subQueryDict = buildTreeFromPlanFile("./resource/queries/job/q/plan-log/plan/application_1656680820985_0048.lz4")
    node.printTree(save_path="resource/queries/job/q/mv/gv/test0048_1.gv")