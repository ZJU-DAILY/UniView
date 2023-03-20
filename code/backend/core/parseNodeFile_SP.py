'''
3.解析node文件
'''

from parseFilterSpecialCase_SP import *
from parse_operator_SP import *
from z3 import *


# 处理SortAggregate算子
def parseSortAggregate(line, tableSet, relationDict):
    _, tmp = line.split("functions=[")
    pattern = r"[(\w]+#[)\w]+"
    dataList = re.findall(pattern, tmp)
    # 处理表名和别名
    for data in dataList:
        # 包含聚合函数
        if data.find("(") != -1:
            aggr, key = data.split("(")
            key = key.split(")")[0]
            info, fieldId = key.split("#")
            fieldId = fieldId.strip()
            # 处理partial_min
            if aggr.find("_") != -1:
                aggr = aggr.split("_")[1].strip()
            if not relationDict.get(fieldId):
                continue
            # fieldList.append(aggr + "(" + relationDict[fieldId].displayName + ")")
            # tableSet.add(relationDict[fieldId].table)
        else:
            info, fieldId = data.split("#")
            if not relationDict.get(fieldId.strip()):
                continue
            # fieldList.append(relationDict[fieldId.strip()].displayName)
            # tableSet.add(relationDict[fieldId].table)


# 处理HashAggregate算子
def parseHashAggregate(line, fieldList, tableSet, commonFilterList, relationDict):
    # 先找keys=
    pattern = r"keys=\[([^=]+)\]"
    key = re.findall(pattern, line)
    if len(key) > 0:
        # 分开
        keyList = parseSortMergeJoinFilter(key[0])
        # 处理group by
        groupByStr = "group by"
        for info in keyList:
            info = info.strip()
            # 先处理substr的情况：
            pattern = r"(substr\([\w]+#[0-9L]+,[ ]*[0-9]+,[ ]*[0-9]+\))"
            dataList = re.findall(pattern, info)
            if len(dataList) > 0:
                data = dataList[0]
                pattern = r"[\w]+#([0-9L]+)"
                keyList = re.findall(pattern, data)
                num = keyList[0].strip()
                if not relationDict.get(num):
                    print("[parseOneNodeFile] HashAggregate get num: {} error!".format(num))
                    continue
                data = re.sub(r"[\w]+#[0-9L]+", relationDict[num].displayName, data)
                # fieldList.append(data)
                tableSet.add(relationDict[num].table)
                # group by
                groupByStr = groupByStr + " " + data + ","
            else:
                try:
                    field, num = info.split("#")
                except:
                    print("[parseHashAggregate] {}".format(info))
                num = num.strip()
                if not relationDict.get(num):
                    print("HashAggregate find not num! {}".format(info))
                    continue
                # fieldList.append(relationDict[num].displayName)
                tableSet.add(relationDict[num].table)
                # group by
                groupByStr = groupByStr + " " + relationDict[num].displayName + ","
        groupByStr = groupByStr[:-1]
        commonFilterList.append(groupByStr)
    # 再找functions=
    _, functionsStr = line.split("functions=")
    pattern = r"([\w]+)[^,]*\(([\w]+)#([0-9L]+)"
    key = re.findall(pattern, functionsStr)
    for info in key:
        aggr = info[0]
        field = info[1]
        num = info[2].strip()
        if aggr.find("_") != -1:
            aggr = aggr.split("_")[1].strip()
        if not relationDict.get(num):
            print("HashAggregate find not num!")
            continue
        # fieldList.append(aggr + "(" + relationDict[num].displayName + ")")
        tableSet.add(relationDict[num].table)


# 处理Project算子
def parseProject(line, fieldList, tableSet, relationDict, subQueryDict, aliasDict):
    key2 = line.split("Project")[1].strip().strip("[").strip("]")
    # 遇到subquery中：, [id=#312]的情况，会通过,分开，需要提前处理
    if key2.find("Subquery") != -1:
        key2 = re.sub(r",[ ]*\[id=#[0-9L]+\]", "", key2)
    keyList = parseBracketFilter(key2)
    for key in keyList:
        # 先把特殊格式删除，如： as decimal(19,2)
        key = remove_unuse_parameter(key)

        # 先不处理subquery的情况
        if key.find("Subquery") != -1:
            continue
        # 有AS的情况
        if key.find(" AS ") != -1:
            replaceString, aliasString = key.split(" AS ")[0:2]
            replaceString, tableList = replace_with_relations_alias(replaceString, relationDict, aliasDict)
            if replaceString == "":
                continue
            # 将错误格式去掉
            replaceString = remove_error_pattern_project(replaceString)
            if replaceString == "":
                continue

            if len(tableList) == 1:
                tableSet.add(tableList[0])
            fieldList.append(replaceString)
        else:
            replaceString, tableList = replace_with_relations_alias(key, relationDict, aliasDict)
            if replaceString == "":
                continue
            # 将错误格式去掉
            replaceString = remove_error_pattern_project(replaceString)
            if replaceString == "":
                continue

            if len(tableList) == 1:
                tableSet.add(tableList[0])
            fieldList.append(replaceString)


# 处理SortMergeJoin算子
def parseSortMergeJoin(line, tableSet, joinFilterList, relationDict):
    # 特殊处理substr
    key2 = line.split("Inner")[0]
    key2 = key2.split("LeftSemi")[0]
    key2 = key2.split(", ExistenceJoin")[0]
    key2 = key2.split(", LeftAnti")[0]
    if key2.find("substr") != -1:
        dataList = parseSortMergeJoinFilter(key2)
        if len(dataList) & 1 != 0:
            print("[SortMergeJoin] dataList parse err, len not equal! {}".format(key2))
            return
        i = 0
        length = len(dataList)
        while i < length / 2:
            if re.search(r"is[not]*null", dataList[i]):
                i = i + 1
                continue
            if re.search(r"is[not]*null", dataList[i + (int)(length / 2)]):
                i = i + 1
                continue
            # 先解析1是否substr
            pattern = r"(substr\([\w]+#[0-9L]+,[ ]*[0-9]+,[ ]*[0-9]+\))"
            data1 = re.findall(pattern, dataList[i])
            pattern = r"([\w]+#[0-9L]+)"
            innerData1 = re.findall(pattern, dataList[i])
            info1, fieldId1 = innerData1[0].split("#")
            fieldId1 = fieldId1.strip()
            if not relationDict.get(fieldId1):
                continue
            if len(data1) > 0:
                field1 = re.sub(pattern, relationDict[fieldId1].displayName, data1[0])
            else:
                field1 = relationDict[fieldId1].displayName
            # fieldList.append(relationDict[fieldId1].displayName)
            # tableSet.add(relationDict[fieldId1].table)
            # 再解析2是否substr
            pattern = r"(substr\([\w]+#[0-9L]+,[ ]*[0-9]+,[ ]*[0-9]+\))"
            data2 = re.findall(pattern, dataList[i + (int)(length / 2)])
            pattern = r"([\w]+#[0-9L]+)"
            innerData2 = re.findall(pattern, dataList[i + (int)(length / 2)])
            info2, fieldId2 = innerData2[0].split("#")
            fieldId2 = fieldId2.strip()
            if not relationDict.get(fieldId2):
                continue
            if len(data2) > 0:
                field2 = re.sub(pattern, relationDict[fieldId2].displayName, data2[0])
            else:
                field2 = relationDict[fieldId2].displayName
            # fieldList.append(relationDict[fieldId2].displayName)
            # tableSet.add(relationDict[fieldId2].table)
            # Filter
            joinFilterList.append(field1 + " = " + field2)
            i = i + 1
        # tableList = list(tableSet)
        # fieldList = list(set(fieldList))
    elif key2.find("coalesce") != -1:
        dataList = parseSortMergeJoinFilter(key2)
        if len(dataList) & 1 != 0:
            print("[SortMergeJoin] dataList coalesce parse err, len not equal! {}".format(key2))
            return
        i = 0
        length = len(dataList)
        while i < length / 2:
            if re.search(r"is[not]*null", dataList[i]):
                i = i + 1
                continue
            if re.search(r"is[not]*null", dataList[i + (int)(length / 2)]):
                i = i + 1
                continue
            # 先解析1是否为coalesce
            pattern = r"coalesce\([\w]+#([0-9L]+),[ ]*[0-9]\)"
            data1 = re.findall(pattern, dataList[i])
            pattern = r"([\w]+#[0-9L]+)"
            innerData1 = re.findall(pattern, dataList[i])
            info1, fieldId1 = innerData1[0].split("#")
            fieldId1 = fieldId1.strip()
            if not relationDict.get(fieldId1):
                i = i + 1
                continue
            if len(data1) > 0:
                field1 = re.sub(pattern, relationDict[fieldId1].displayName, data1[0])
            else:
                field1 = relationDict[fieldId1].displayName
            # fieldList.append(relationDict[fieldId1].displayName)
            tableSet.add(relationDict[fieldId1].table)
            # 再解析2是否为coalesce
            pattern = r"coalesce\([\w]+#([0-9L]+),[ ]*[0-9]\)"
            data2 = re.findall(pattern, dataList[i + (int)(length / 2)])
            pattern = r"([\w]+#[0-9L]+)"
            innerData2 = re.findall(pattern, dataList[i + (int)(length / 2)])
            info2, fieldId2 = innerData2[0].split("#")
            fieldId2 = fieldId2.strip()
            if not relationDict.get(fieldId2):
                i = i + 1
                continue
            if len(data2) > 0:
                field2 = re.sub(pattern, relationDict[fieldId2].displayName, data2[0])
            else:
                field2 = relationDict[fieldId2].displayName
            # fieldList.append(relationDict[fieldId2].displayName)
            tableSet.add(relationDict[fieldId2].table)
            # Filter
            joinFilterList.append(field1 + " = " + field2)
            i = i + 1
        # tableList = list(tableSet)
        # fieldList = list(set(fieldList))
    else:
        pattern = r"[(\w]+#[)\w]+"
        key2 = key2.split("Inner")[0]
        key2 = key2.split("LeftSemi")[0]
        key2 = key2.split(", ExistenceJoin")[0]
        dataList = re.findall(pattern, key2)
        if len(dataList) & 1 != 0:
            print("[SortMergeJoin] not substr dataList parse err, len not equal!")
            return
        i = 0
        length = len(dataList)
        while i < length / 2:
            info1, fieldId1 = dataList[i].split("#")
            info2, fieldId2 = dataList[i + (int)(length / 2)].split("#")
            fieldId1 = fieldId1.strip()
            fieldId2 = fieldId2.strip()
            if not relationDict.get(fieldId1):
                ok = AddRelationDictFromExisting(fieldId1, info1, relationDict)
                if not ok:
                    i = i + 1
                    continue
            if not relationDict.get(fieldId2):
                ok = AddRelationDictFromExisting(fieldId2, info2, relationDict)
                if not ok:
                    i = i + 1
                    continue
            # Filter
            joinFilterList.append(relationDict[fieldId1].displayName + " = " + relationDict[fieldId2].displayName)
            # table
            tableSet.add(relationDict[fieldId1].table)
            tableSet.add(relationDict[fieldId2].table)
            i = i + 1
        # tableList = list(tableSet)
        # fieldList = list(set(fieldList))
        # print(tableList)
        # print(joinFilter)
        # print(fieldList)


# 处理BroadcastHashJoin算子
def parseBroadcastHashJoin(line, tableSet, joinFilterList, relationDict):
    pattern = r"[(\w]+#[)\w]+"
    key2 = line.split("Inner")[0]
    key2 = key2.split("LeftSemi")[0]
    key2 = key2.split(", ExistenceJoin")[0]

    dataList = re.findall(pattern, key2)
    # 不是偶数，无法一一对应，错误
    if len(dataList) & 1 != 0:
        print("dataList parse err, len not equal!")
        return
    i = 0
    length = len(dataList)
    while i < length / 2:
        info1, fieldId1 = dataList[i].split("#")
        info2, fieldId2 = dataList[i + (int)(length / 2)].split("#")
        fieldId1 = fieldId1.strip()
        fieldId2 = fieldId2.strip()
        if not relationDict.get(fieldId1):
            continue
        if not relationDict.get(fieldId2):
            continue
        # 字段
        # fieldList.append(relationDict[fieldId1].displayName)
        # fieldList.append(relationDict[fieldId2].displayName)
        # Filter
        joinFilterList.append(relationDict[fieldId1].displayName + " = " + relationDict[fieldId2].displayName)
        # table
        tableSet.add(relationDict[fieldId1].table)
        tableSet.add(relationDict[fieldId2].table)
        i = i + 1
    # tableList = list(tableSet)


# 处理BroadcastNestedLoopJoin算子
def parseBroadcastNestedLoopJoin(line, tableSet, joinFilterList, relationDict):
    try:
        key = line.split("Inner,")[1].strip()
        keyList = key.split("AND")
        # print(keyList)
        for data in keyList:
            pattern = r"is[not]*null"
            ok = re.search(pattern, data)
            if ok:
                # 处理isnull
                if data.find("isnull") == -1:
                    continue
                filed, num = data.strip().split("isnull(")[1].strip(")").split("#")
                num = num.strip()
                # fieldList.append(relationDict[num].displayName)
                joinFilterList.append(relationDict[num].displayName + " IS NULL")
                tableSet.add(relationDict[num].table)
                continue
            if data.find("OR") != -1:
                subList = data.split("OR")
                tmpFilterList = []
                tmpStr = ""
                for subData in subList:
                    FilterFieldManage(subData, relationDict, tmpFilterList, tableSet)
                i = 0
                length = len(tmpFilterList)
                while i < length:
                    tmpStr = tmpStr + tmpFilterList[i]
                    if i != length - 1:
                        tmpStr = tmpStr + " OR "
                    i = i + 1
                joinFilterList.append(tmpStr)
            else:
                FilterFieldManage(data, relationDict, joinFilterList, tableSet)
        tableList = list(tableSet)
    except:
        print("err: {}".format(line))


# 处理Filter算子
def parseFilter(line, relations, tableSet, commonFilterList, relationDict, subQueryDict, tableField):
    key = line.split("Filter")[1].strip()
    with open("filter_lst.txt", "a+") as f:
        f.write(key + "\n")
    # 处理key
    # key = remove_unuse_parameter(key)
    keyList = key.split("AND")

    conditionList = []
    for data in keyList:
        pattern = r"is[not]*null"
        ok = re.search(pattern, data)
        if ok:
            # 处理isnull,不处理
            # if data.find("isnull") == -1:
            #     continue
            # filed, num = data.strip().split("isnull(")[1].strip(")").split("#")
            # num = num.strip()
            # commonFilterList.append(relationDict[num].displayName + " IS NULL")
            # tableSet.add(relationDict[num].table)
            continue
        if data.find("OR") != -1:
            subList = data.split("OR")
            # tmpFilterList = []
            for subData in subList:
                condition = FilterFieldManage(subData, relationDict, commonFilterList, tableSet, subQueryDict, tableField)
                if condition != {}:
                    tableSet.add(condition["table"])
                    conditionList.append(condition)
            # tmpStr = ""
            # i = 0
            # length = len(tmpFilterList)
            # while i < length:
            #     tmpStr = tmpStr + tmpFilterList[i]
            #     if i != length - 1:
            #         tmpStr = tmpStr + " OR "
            #     i = i + 1
            # if length > 0:
            #     commonFilterList.append("(" + tmpStr + ")")
        else:
            condition = FilterFieldManage(data, relationDict, commonFilterList, tableSet, subQueryDict, tableField)
            if condition != {}:
                tableSet.add(condition["table"])
                conditionList.append(condition)

    # 处理condition中可以合并的情况，直接加入relations
    relations = {}
    solver = Solver()
    # 处理两个参数，但是是and的情况且是>和<

    if len(conditionList) == 2:
        opList = [">", "<", ">=", "<="]
        if conditionList[0]["op"] in opList and conditionList[1]["op"] in opList:
            leftValue = conditionList[0]["left"] + " " + conditionList[0]["op"] + " " + conditionList[0]["right"]
            rightValue = conditionList[1]["left"] + " " + conditionList[1]["op"] + " " + conditionList[1]["right"]
            filter = "(" + leftValue + " AND " + rightValue + ")"
            commonFilterList.append(filter)
            return

    for condition in conditionList:
        table = condition["table"]
        field = condition["field"]
        left = condition["left"]
        op = condition["op"]
        right = condition["right"]
        if op.find("like") != -1 or op == "INSET":
            continue
        left = change_dot_to_Of(left)
        # 处理=的情况
        if op == "=":
            op = "=="
        # 将普通的等号=转换为==
        if op == "in":
            cond2 = "Or("
            for i, value in enumerate(right):
                cond2 += left + " == " + value + ","
            cond2 = cond2[:-1] + ")"
        else:
            cond2 = left + " " + op + " " + right
        # 处理
        if left not in relations:
            relations[left] = [cond2]
        else:
            type = tableField[table][field]["type"]
            if type == "string":
                tmp = String(left)
                exec('{}=tmp'.format(left))
            else:
                tmp = Real(left)
                exec('{}=tmp'.format(left))
            # 执行sovler
            i = 0
            flag = False

            while i < len(relations[left]):
                try:
                    solver.reset()
                    cond1 = relations[left][i]
                    solver.add(eval(cond1))
                    solver.add(eval(cond2))
                except Exception as r:
                    print("Exception raised!!!")
                    print(r)
                    print(cond1)
                    print(cond2)
                    i += 1
                    continue
                if str(solver.check()) == "unsat":
                    # 如果没有同时满足两个条件的解，说明两个条件完全无关，可以将两个条件合并
                    if cond1.find("Or") != -1 and cond2.find("Or") != -1:
                        relations[left][i] = cond1[:-1] + "," + cond2[3:-1] + ")"
                    elif cond1.find("Or") != -1:
                        relations[left][i] = cond1[:-1] + "," + cond2 + ")"
                    elif cond2.find("Or") != -1:
                        relations[left][i] = cond2[:-1] + "," + cond1 + ")"
                    else:
                        relations[left][i] = "Or(" + cond1 + "," + cond2 + ")"
                    # relations[left][i] = cond1[:-1] + "," + cond2 + ")"
                    flag = True
                    break
                else:
                    lapFlag = False
                    # 如果两个条件有交集,则检查谁包含谁,或者是否没有包含关系
                    solver.reset()
                    solver.add(eval(cond1))
                    solver.add(Not(eval(cond2)))
                    if str(solver.check()) == "sat":
                        # 对同样的变量，有满足原始条件，不满足新条件的解，说明cond1包含cond2或者两者相交
                        lapFlag = True
                    solver.reset()
                    solver.add(Not(eval(cond1)))
                    solver.add(eval(cond2))
                    # 对同样的变量，有不满足原始条件，但是满足新条件的解，说明cond2包含cond1
                    if str(solver.check()) == "sat":
                        if lapFlag is True:
                            i += 1
                            continue
                        else:
                            # 说明cond2包含cond1
                            relations[left][i] = cond2
                            flag = True
                            break
                    else:
                        # 两个条件相等或者cond1包含cond2
                        flag = True
                        break
            # 处理
            if not flag:
                relations[left].append(cond2)

    # 加入
    for k, v in relations.items():
        v = list(set(v))
        for value in v:
            value = change_Of_to_dot(value)
            value = reset_condition_z3(value)
            commonFilterList.append(value)
            if value.find("date_dim") != -1:
                continue
            if not check_bracket_equal(value):
                continue
            # with open("0914.txt", "a+") as f:
            #     f.write(value + "\n")


# 将condition从z3适应的结构切换回来
def reset_condition_z3(condition):
    condition = condition.replace("\'", "\"")

    # 将等号==转换为普通=
    if condition.find("==") != -1:
        condition = condition.replace("==", "=")

    # 处理OR的情况，OR后置
    if condition.find("Or") != -1:
        condition = condition[2:]
        condition = condition.replace(",", " OR ")

    # 处理like/not like/!=
    condition = condition.replace("= \"__LIKE__", "like \"")
    condition = condition.replace("= \"__NOTLIKE__", "not like \"")
    condition = condition.replace("= \"__NOTEQUAL__", "!= \"")

    condition = condition.replace("\"", "\'")

    return condition

# 将.处理为Of
def change_dot_to_Of(value):
    pattern = r"([\w]+)\.([\w]+)"
    keyList = re.findall(pattern, value)
    if len(keyList) == 0:
        return value
    # 先处理filter
    for key in keyList:
        table, field = key[0], key[1]
        # if is_number(table) or is_number(field):
        #     continue
        # if table not in tableList:
        #     print("[generate_relations_z3] current table not exist! {}".format(table))
        #     continue
        # # 先处理filter
        value = value.replace(table + "." + field, field + "Of" + table)
    return value


# 将Of处理为.
def change_Of_to_dot(value):
    pattern = r"([\w]+)Of([\w]+)"
    keyList = re.findall(pattern, value)
    if len(keyList) == 0:
        return value
    # 先处理filter
    for key in keyList:
        field, table = key[0], key[1]
        # if is_number(table) or is_number(field):
        #     continue
        # if table not in tableList:
        #     print("[generate_relations_z3] current table not exist! {}".format(table))
        #     continue
        # # 先处理filter
        value = value.replace(field + "Of" + table, table + "." + field)
    return value


# 处理TakeOrderedAndProject算子
def parseTakeOrderedAndProject(line, commonFilterList, relationDict):
    data = line.split("TakeOrderedAndProject(")[1].strip().strip(")")
    # 将limit和orderby取出
    # 1.先将limit取出
    pattern = r"limit=([0-9]+)"
    dataList = re.findall(pattern, data)
    if len(dataList) > 0:
        limitNum = dataList[0].strip()
        commonFilterList.append("limit " + limitNum)
    # 2.再将order by取出
    pattern = r"orderBy=\[([\w#, ]+)\]"
    dataList = re.findall(pattern, data)
    if len(dataList) > 0:
        orderByStr = dataList[0].strip()
        orderByStrList = orderByStr.split(",")
        ansStr = "order by"
        for tmpStr in orderByStrList:
            tmpStr = tmpStr.strip()
            pattern = r"([\w]+)#([0-9L]+)[ ]*([\w]+)"
            key = re.findall(pattern, tmpStr)
            if len(key) > 0:
                orderField = key[0][0]
                orderNum = key[0][1]
                orderMode = key[0][2]
                if not relationDict.get(orderNum):
                    ok = AddRelationDictFromExisting(orderNum, orderField, relationDict)
                    if not ok:
                        print("[parseOneNodeFile] get orderNum error! {}".format(orderNum))
                        continue
                ansStr = ansStr + " " + relationDict[orderNum].displayName + " " + orderMode + ","
        ansStr = ansStr[:-1]
        commonFilterList.append(ansStr)


# 处理join类型
def parse_operator_join(line, tableSet, joinFilterList, outerJoinFilterList, relationDict, aliasDict):
    # old: [\w]+[ ]*\[([\w#(), ]+)\],[ ]*\[([\w#(), ]+)\]
    pattern = r"[\w]+[ ]*\[([\w#(), ]+)\],[ ]*\[([\w#(), ]+)\],[ ]*(.*)"
    key = re.findall(pattern, line)
    if len(key) > 0:
        leftKey, rightKey, joinType = key[0][0], key[0][1], key[0][2]
        # 先获取join的类型
        joinType = joinType.split(",")[0].strip()
        if joinType == "Inner":
            joinType = "Inner"
        elif joinType == "LeftOuter":
            joinType = "LEFT OUTER JOIN"
        elif joinType == "LeftSemi":
            joinType = "SEMI JOIN"
        elif joinType == "LeftAnti":
            joinType = "ANTI JOIN"
        elif joinType == "FullOuter":
            joinType = "FULL JOIN"
        elif joinType == "RightOuter":
            joinType = "RIGHT JOIN"
        elif joinType == "RightSemi":
            joinType = "SEMI JOIN"
        elif joinType == "RightAnti":
            joinType = "ANTI JOIN"
        else:
            joinType = "Inner"

        # #12L#257L的情况
        pattern = r"(#[0-9L]+)#[0-9L]+"
        leftKey = re.sub(pattern, r"\1", leftKey)
        rightKey = re.sub(pattern, r"\1", rightKey)
        leftKeyList = parseBracketFilter(leftKey)
        rightKeyList = parseBracketFilter(rightKey)
        # 检查两者长度是否一致
        if len(leftKeyList) != len(rightKeyList):
            print("[parse_operator_join] leftKeyList and rightKeyList len not equal!{}".format(line))
            return

        i = 0
        length = len(leftKeyList)
        while i < length:
            leftValue, rightValue = leftKeyList[i], rightKeyList[i]
            if leftValue.find("isnull") != -1 or rightValue.find("isnull") != -1 or \
                leftValue.find("isnotnull") != -1 or rightValue.find("isnotnull") != -1:
                i += 1
                continue
            # 找到x#x并做替换
            pattern = r"[\w]+#[0-9L]+"
            key = re.findall(pattern, leftValue)
            replaceField1 = key[0]
            field1, num1 = replaceField1.split("#")
            if not relationDict.get(num1):
                table1 = search_project_from_table(field1)
                if table1 == "":
                    if aliasDict.get(num1):
                        table1 = aliasDict[num1]["table"]
                        displayName1 = aliasDict[num1]["displayName"]
                    else:
                        print("[parse_operator_join] can't find num:{}".format(leftValue))
                        i += 1
                        continue
                else:
                    displayName1 = table1 + "." + field1
            else:
                table1 = relationDict[num1].table
                displayName1 = relationDict[num1].displayName

            key = re.findall(pattern, rightValue)
            replaceField2 = key[0]
            field2, num2 = replaceField2.split("#")
            if not relationDict.get(num2):
                table2 = search_project_from_table(field2)
                if table2 == "":
                    if aliasDict.get(num2):
                        table2 = aliasDict[num2]["table"]
                        displayName2 = aliasDict[num2]["displayName"]
                    else:
                        print("[parse_operator_join] can't find num:{}".format(rightValue))
                        i += 1
                        continue
                else:
                    displayName2 = table2 + "." + field2
            else:
                table2 = relationDict[num2].table
                displayName2 = relationDict[num2].displayName

            # 替换后加入table和filter
            leftFilter = leftValue.replace(replaceField1, displayName1)
            rightFilter = rightValue.replace(replaceField2, displayName2)
            if not check_bracket_equal(leftFilter):
                print(leftFilter)
                i += 1
                continue
            if not check_bracket_equal(rightFilter):
                print(rightFilter)
                i += 1
                continue
            if leftFilter == rightFilter:
                i += 1
                continue
            tableSet.add(table1)
            tableSet.add(table2)
            # joinFilterList.append({"displayName": leftFilter + " = " + rightFilter, "l_op" : leftFilter, "r_op": rightFilter, "type": joinType, "l_table": table1, "r_table": table2})
            if joinType != "Inner":
                outerJoinFilterList.append((leftFilter + " = " + rightFilter, leftFilter, rightFilter, joinType, table1, table2))
            else:
                joinFilterList.append(leftFilter + " = " + rightFilter)


            i += 1


# 处理Scan类型，将涉及到的列加入fieldList
def parse_operator_Scan(line, relationDict, fieldList, g_table_common):
    line = line.split("[")[1].split("]")[0]
    # keyList: d_date_sk#49L,d_month_seq#52
    keyList = line.split(",")
    for key in keyList:
        # key: d_date_sk#49L
        field, num = key.split("#")
        field, num = field.strip(), num.strip()
        if num in relationDict:
            fieldList.append(relationDict[num].displayName)
        else:
            continue
        # common
        if not g_table_common.get(relationDict[num].table):
            g_table_common[relationDict[num].table] = [relationDict[num].displayName]
        else:
            if not relationDict[num].displayName in g_table_common[relationDict[num].table]:
                g_table_common[relationDict[num].table].append(relationDict[num].displayName)



# 解析各种类型的算子
def getChildNode_SP(line, relations, relationDict, subQueryDict, aliasDict, tableField, g_table_common={}):
    # 1.获取id/nodeType/key2
    if line.find("id:") == -1:
        return "", "", ""
    lineId = line.split("id:")
    keyList = lineId[1].split("name:")
    key1, key2 = keyList[0], keyList[1]
    nodeId = key1.strip()

    keyList = key2.split("desc:")
    key1, key2 = keyList[0], keyList[1]
    nodeType = getStripNodeType(key1).strip()

    # 2.通过类型来判断该如何处理，后面写成一个函数
    fieldList = []
    tableSet = set()
    joinFilterList = []
    commonFilterList = []
    outerJoinFilterList = []
    is_group_by = False
    is_hash_agg = False
    group_by_list = []
    agg_list = []
    if nodeType == "SortAggregate":
        parse_SortAggregate(key2)
    elif nodeType == "HashAggregate":
        is_hash_agg, group_by_list, agg_list = parse_HashAggregate(key2, relationDict, aliasDict, tableField)
    elif nodeType == "Project":
        parseProject(key2, fieldList, tableSet, relationDict, subQueryDict, aliasDict)
    elif nodeType == "SortMergeJoin":
        parse_operator_join(key2, tableSet, joinFilterList, outerJoinFilterList, relationDict, aliasDict)
    elif nodeType == "BroadcastHashJoin":
        parse_operator_join(key2, tableSet, joinFilterList, outerJoinFilterList, relationDict, aliasDict)
    elif nodeType == "BroadcastNestedLoopJoin":
        parse_operator_join(key2, tableSet, joinFilterList, outerJoinFilterList, relationDict, aliasDict)
    elif nodeType == "Filter":
        parseFilter(key2, relations, tableSet, commonFilterList, relationDict, subQueryDict, tableField)
    elif nodeType == "TakeOrderedAndProject":  # 有limit时候order by会跟limit合成一个TakeOrderedAndProject
        pass
    elif nodeType == "ColumnarToRow":
        pass
    elif nodeType == "BroadcastExchange":
        pass
    elif nodeType == "LocalTableScan":
        pass
    elif nodeType == "Union":
        pass
    elif nodeType == "Window":
        pass
    elif nodeType == "Expand":
        pass
    elif nodeType == "Subquery":
        pass
    elif nodeType == "CollectLimit":
        pass
    elif nodeType == "WholeStageCodegen":
        pass
    elif nodeType == "Sort":
        pass
    elif nodeType == "SubqueryBroadcast":
        pass
    elif nodeType == "ReusedExchange":
        pass
    elif nodeType == "Exchange":
        pass
    elif nodeType.find("Scan orc") != -1:
        parse_operator_Scan(key2, relationDict, fieldList, g_table_common)
    else:
        print(nodeType + " not found!")

    # 表名
    tableList = list(tableSet)
    fieldList = list(set(fieldList))
    joinFilterList = list(set(joinFilterList))
    commonFilterList = list(set(commonFilterList))
    outerJoinFilterList = list(set(outerJoinFilterList))
    node = SPNode(int(nodeId), nodeType, fieldList, tableList, joinFilterList, outerJoinFilterList, commonFilterList)
    node.isGroupBy = len(group_by_list) > 0
    node.group_by_list = group_by_list
    node.is_hash_agg = is_hash_agg
    node.agg_list = agg_list

    return nodeId, nodeType, node


# 用于q-mv解析算子，只获取不深入解析
def get_child_node_qmv_SP(line, relationDict, subQueryDict, aliasDict, tableField):
    # 1.获取id/nodeType/key2
    if line.find("id:") == -1:
        return "", "", ""
    lineId = line.split("id:")
    keyList = lineId[1].split("name:")
    key1, key2 = keyList[0], keyList[1]
    nodeId = key1.strip()

    keyList = key2.split("desc:")
    key1, key2 = keyList[0], keyList[1]
    nodeType = getStripNodeType(key1).strip()

    # 2.通过类型来判断该如何处理，后面写成一个函数
    fieldList = []
    tableSet = set()
    joinFilterList = []
    commonFilterList = []
    outerJoinFilterList = []
    isGroupBy = False
    # 表名
    tableList = list(tableSet)
    fieldList = list(set(fieldList))
    joinFilterList = list(set(joinFilterList))
    commonFilterList = list(set(commonFilterList))
    outerJoinFilterList = list(set(outerJoinFilterList))
    node = SPNode(int(nodeId), nodeType, fieldList, tableList, joinFilterList, outerJoinFilterList, commonFilterList)
    node.isGroupBy = isGroupBy

    return nodeId, nodeType, node


# 解析node文件，用于生成候选视图
def parseOneNodeFile(nodeFile, relationDict, subIdMap, tableDict, subQueryDict, tableField, aliasDict, g_table_common):
    global cur_node_step, cur_node_cnt
    # 返回的结构
    nodeMap = {} # node标号->SPNode
    maxId = 0
    relations = {}
    cur_node_step += 1
    cur_node_cnt = 0
    with open(nodeFile, encoding="utf-8") as f:
        while True:
            line = f.readline()
            if not line:
                break

            # 1.生成树的结构
            # union-sql18,left和right不够
            if line.find("->") != -1:
                children, father = line.strip().strip(";").split("->")
                nodeMap[children].father = nodeMap[father]
                if not nodeMap[father].left:
                    nodeMap[father].left = nodeMap[children]
                else:
                    nodeMap[father].right = nodeMap[children]
                continue

            # 2.解析各个算子
            nodeId, nodeType, node = getChildNode_SP(line, relations, relationDict, subQueryDict, aliasDict, tableField, g_table_common)
            if not node:
                continue
            maxId = max(maxId, int(nodeId))

            # 3.处理时间
            curTime = 0.0
            while True:
                line = f.readline()
                if line.find("SQLPlanMetric") == -1:
                    break
                if line.find("timing") == -1 and line.find("nsTiming") == -1:
                    continue
                lineList = line.split(",")
                length = len(lineList)
                execTime = lineList[length - 2].split("ns")[0].strip()
                curTime = curTime + float(execTime)
            # 处理该结点的时间
            node.curTime = curTime
            nodeMap[nodeId] = node
            # 处理subgraph的情况
            if nodeType == "WholeStageCodegen":
                for tmpNode in subIdMap[nodeId]:
                    nodeMap[tmpNode].curTime = curTime
                    nodeMap[tmpNode].isSubGraph = True
                nodeMap[nodeId].isSubGraph = True

    # 获取根节点
    nodeHead = findNodeHead(maxId, nodeMap)
    # 遍历这棵树，将fieldList/tableList/joinFilter和cost累积起来
    clusters = []
    traverseTreeNode(nodeHead, clusters, tableField)
    return clusters


# 解析node文件，用于encode
def parse_encode_one_node_file(nodeFile, relationDict, subIdMap, tableDict, subQueryDict, tableField, aliasDict):
    # 返回的结构
    nodeMap = {} # node标号->SPNode
    maxId = 0
    with open(nodeFile, encoding="utf-8") as f:
        while True:
            line = f.readline()
            if not line:
                break

            # 1.生成树的结构
            # union-sql18,left和right不够
            if line.find("->") != -1:
                children, father = line.strip().strip(";").split("->")
                nodeMap[children].father = nodeMap[father]
                if not nodeMap[father].left:
                    nodeMap[father].left = nodeMap[children]
                else:
                    nodeMap[father].right = nodeMap[children]
                continue

            # 2.解析各个算子
            nodeId, nodeType, node = getChildNode_SP(line, relationDict, subQueryDict, aliasDict, tableField, tableField)
            if not node:
                continue
            maxId = max(maxId, int(nodeId))

            # 3.处理时间
            curTime = 0.0
            while True:
                line = f.readline()
                if line.find("SQLPlanMetric") == -1:
                    break
                if line.find("timing") == -1 and line.find("nsTiming") == -1:
                    continue
                lineList = line.split(",")
                length = len(lineList)
                execTime = lineList[length - 2].split("ns")[0].strip()
                curTime = curTime + float(execTime)
            # 处理该结点的时间
            node.curTime = curTime
            nodeMap[nodeId] = node
            # 处理subgraph的情况
            if nodeType == "WholeStageCodegen":
                for tmpNode in subIdMap[nodeId]:
                    nodeMap[tmpNode].curTime = curTime
                    nodeMap[tmpNode].isSubGraph = True
                nodeMap[nodeId].isSubGraph = True

    # 获取根节点
    nodeHead = findNodeHead(maxId, nodeMap)
    # 遍历这棵树，将fieldList/tableList/joinFilter和cost累积起来
    clusters = []
    traverse_encode_tree_node(nodeHead, clusters, tableField)
    return clusters


# 解析node文件，用于encode中的q-mv部分
def parse_encode_qmv_one_node_file(nodeFile, relationDict, subIdMap, tableDict, subQueryDict, tableField, aliasDict):
    # 返回的结构
    nodeMap = {} # node标号->SPNode
    maxId = 0
    with open(nodeFile, encoding="utf-8") as f:
        while True:
            line = f.readline()
            if not line:
                break

            # 1.生成树的结构
            # union-sql18,left和right不够
            if line.find("->") != -1:
                children, father = line.strip().strip(";").split("->")
                nodeMap[children].father = nodeMap[father]
                if not nodeMap[father].left:
                    nodeMap[father].left = nodeMap[children]
                else:
                    nodeMap[father].right = nodeMap[children]
                continue

            # 2.解析各个算子
            nodeId, nodeType, node = get_child_node_qmv_SP(line, relationDict, subQueryDict, aliasDict, tableField)
            if not node:
                continue
            maxId = max(maxId, int(nodeId))

            # 3.处理时间
            curTime = 0.0
            while True:
                line = f.readline()
                if line.find("SQLPlanMetric") == -1:
                    break
                if line.find("timing") == -1 and line.find("nsTiming") == -1:
                    continue
                lineList = line.split(",")
                length = len(lineList)
                execTime = lineList[length - 2].split("ns")[0].strip()
                curTime = curTime + float(execTime)
            # 处理该结点的时间
            node.curTime = curTime
            nodeMap[nodeId] = node
            # 处理subgraph的情况
            if nodeType == "WholeStageCodegen":
                for tmpNode in subIdMap[nodeId]:
                    nodeMap[tmpNode].curTime = curTime
                    nodeMap[tmpNode].isSubGraph = True
                nodeMap[nodeId].isSubGraph = True

    # 获取根节点
    nodeHead = findNodeHead(maxId, nodeMap)
    # 遍历这棵树，将fieldList/tableList/joinFilter和cost累积起来
    clusters = []
    traverse_qmv_encode_tree_node(nodeHead, clusters, tableField)
    return clusters