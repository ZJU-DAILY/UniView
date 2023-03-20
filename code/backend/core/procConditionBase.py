import re
from procSQL import *
from procConf import *
__PG__ = 0
__CH__ = 1
if __CH__ == 1:
    stringType = ["STRING", "TEXT", "CHARACTER"]

if __PG__ == 1:
    stringType = ["character", "text"]


def transRegStr(baseString, regString):
    strArr = regString.strip("'").split("%")
    i = 0
    curPos = 0
    tmpCond = ""
    for pattenStr in strArr:
        operator = "Contains"
        if len(pattenStr) != 0:
            if i == 0 and curPos == 0:
                operator = "PrefixOf"
            elif i == len(strArr) - 1 and (curPos + len(pattenStr)) == len(regString.strip("'")):
                operator = "SuffixOf"
            # 如果在中间有长度为0的，则不需要处理
            if operator != "Contains":
                if tmpCond == "":
                    tmpCond = operator + "(\"" + pattenStr + "\"," + baseString + ")"
                else:
                    tmpCond = "And(" + tmpCond + "," + operator + "(\"" + pattenStr + "\"," + baseString + "))"
            else:
                myCondition = operator + "(SubString(" + baseString + ", " + str(
                    curPos) + ", Length(" + baseString + ")-" + str(curPos) + "),\"" + pattenStr + "\")"
                if tmpCond != "":
                    tmpCond = "And(" + tmpCond + "," + "(" + myCondition + "))"
                else:
                    tmpCond = "(" + myCondition + ")"
        curPos += len(pattenStr) + 1
        i += 1
    return tmpCond


def checkColName(rel, value):
    if re.match(r'^[a-z][a-z0-9_]*\.[a-z][a-z0-9_]*$', value) is not None:
        return True
    if rel is not None:
        for r in rel:
            if value.upper() in g_table.data[r.upper()]:
                return True
    else:
        if re.match(r'^[a-zA-Z][a-zA-Z0-9_]*$', value) is not None:
            return True
    return False


def appendCondition(planTree, relName, conditionDict):
    # print("appendCondition.relName:{},conditionDict:{}".format(relName, conditionDict))
    if relName not in planTree.relations:
        planTree.relations[relName] = conditionDict
    elif "condition" not in planTree.relations[relName]:
        planTree.relations[relName] = dict(planTree.relations[relName], **conditionDict)
    else:
        newCondition = conditionDict["condition"]
        orgCondition = planTree.relations[relName]["condition"]
        if orgCondition.find(newCondition) == -1:
            planTree.relations[relName]["condition"] = "And (" + orgCondition + ", " + conditionDict["condition"] + ")"


def appendColCondition(planTree, relName, colName, condition):
    # print("appendCondition.relName:{},conditionDict:{}".format(relName, conditionDict))
    if relName not in planTree.relations:
        planTree.relations[relName] = {colName: [condition]}
    elif colName not in planTree.relations[relName]:
        planTree.relations[relName][colName] = [condition]
    else:
        orgCondition = planTree.relations[relName][colName]
        if condition not in orgCondition:
            planTree.relations[relName][colName].append(condition)


def appendNames(planTree, relName, nameDict):
    if "variableNames" not in planTree.relations[relName]:
        planTree.relations[relName]["variableNames"] = nameDict
    else:
        planTree.relations[relName]["variableNames"]["Literal"] = list(
            set(planTree.relations[relName]["variableNames"]["Literal"]
                + nameDict["Literal"]))
        planTree.relations[relName]["variableNames"]["Number"] = list(
            set(planTree.relations[relName]["variableNames"]["Number"]
                + nameDict["Number"]))


def getFirstPattern(string):
    start = string.find("(")
    if -1 == start:
        return string, len(string)
    # 括号之前有逗号
    if string.count(",", 0, start) != 0:
        end = string[:start].rfind(",")
        return string[:end], end + 1

    # 如果括号是被引号引起来的
    if string.count("'", 0, start) != 0:
        quotaStart = string[:start].find("'")
        quotaEnd = string.find("'", quotaStart + 1)
        return string[:quotaEnd + 2], quotaEnd + 2
    # 如果括号是被引号引起来的
    if string.count("\"", 0, start) != 0:
        quotaStart = string[:start].find("\"")
        quotaEnd = string.find("'", quotaStart + 1)
        return string[:quotaEnd + 2], quotaEnd + 2

    right_count = 1
    # 第一个右括号的位置
    end = string.find(")")
    if -1 == end:
        return string
    left_count = string.count("(", 0, end)

    inner_left, inner_right = getQuotaEnclCnt(string[start:end])
    left_count -= inner_left
    right_count -= inner_right

    while left_count > right_count or string.count("'", start, end+1) % 2 != 0 or string.count("\"", start, end+1) % 2 != 0:
        end = end + 1 + string[end + 1:].find(")")
        left_count = string.count("(", 0, end+1)
        right_count = string.count(")", 0, end+1)
        inner_left, inner_right = getQuotaEnclCnt(string[start:end+1])
        left_count -= inner_left
        right_count -= inner_right
    return string[:end + 1], end + 1


def getQuotaEnclCnt(string):
    left_count = 0
    right_count = 0
    start = 0
    while string.count("'", start) != 0 and string.count("'", start) % 2 == 0:
        quotaStart = string.find("'", start)
        quotaEnd = string.find("'",  quotaStart + 1)
        left_count += string.count("(", quotaStart, quotaEnd)
        right_count += string.count(")", quotaStart, quotaEnd)
        start = quotaEnd + 1
    start = 0
    while string.count("\"", start) != 0 and string.count("\"", start) % 2 == 0:
        quotaStart = string.find("\"", start)
        quotaEnd = string.find("\"",  quotaStart + 1)
        left_count += string.count("(", quotaStart, quotaEnd)
        right_count += string.count(")", quotaStart, quotaEnd)
        start = quotaEnd + 1
    return left_count, right_count


def getBracePatterns(string):
    outString = []
    length = len(string)
    end = -1
    while end + 1 < length:
        procString = string[end + 1:]
        tmpString, tmpEnd = getFirstPattern(procString)
        outString.append(tmpString)
        # print("string:{},\nend:{},\ntmpEnd:{}\noutString:{}".format(string, end, tmpEnd, outString))
        end = end + 1 + tmpEnd
    return outString


if __name__ == '__main__':
    left, right = getQuotaEnclCnt(
        'and(and(equals(company_type.id, movie_companies.company_type_id), like(movie_companies.note, \'%(France)%\')), like(movie_companies.note, \'%(theatrical)%\')), equals(company_type.kind, \'production companies\')')
    print(left)
    print(right)
