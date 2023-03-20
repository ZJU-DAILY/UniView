from procConditionBase import *
from parsePlanJson import *
from basePlan import *
from procSQL import *
import re

'''
'And((infoOfinfo_type == \'votes\'),(infoOfinfo_type == \'genres\'))'
' And(PrefixOf("Lionsgate",nameOfcompany_name))'
'Or((noteOfcast_info == \'(writer)\'),(noteOfcast_info ==  \'(head writer)\'),(noteOfcast_info ==  \'(written by)\'),(noteOfcast_info ==  \'(story)\'),(noteOfcast_info ==  \'(story editor)\'))'
'And(And((Contains(SubString(title.title, 1, Length(title.title)-1),"Freddy")),(Contains(SubString(title.title, 1, Length(title.title)-1),"Jason"))), And(PrefixOf("Saw",title.title)))'
'''


def resetCondition2Sql(condition, pyp=False):
    condition = condition.replace("\'", "\"")
    condition = resetPrefixCondition(condition, pyp)
    condition = resetContainsCondition(condition, pyp)
    condition = resetSuffixCondition(condition, pyp)
    condition = resetLogicCondition(condition)
    condition = resetInCondition(condition)
    condition = resetLikeCondition(condition)
    condition = condition.replace("\"", "\'")
    return condition


def resetPrefixCondition(condition, pyp):
    pattern = re.compile(r'\bPrefixOf\b')
    grps = pattern.findall(condition)
    if len(grps) == 0:
        return condition
    while -1 != condition.find("PrefixOf"):
        idx = condition.find("PrefixOf") + len("PrefixOf")
        orgCond, _ = getFirstPattern(condition[idx:])
        string, col = orgCond.strip("(").strip(")").split(",")
        if pyp is True:
            replacedString = "(" + col + " = \'__LIKE__ " + string[1:-1] + "%\')"
        else:
            replacedString = "(" + col + " like \'" + string[1:-1] + "%\')"
        condition = condition.replace('PrefixOf' + orgCond, replacedString)
    return condition


def resetSuffixCondition(condition, pyp):
    pattern = re.compile(r'\bSuffixOf\b')
    grps = pattern.findall(condition)
    if len(grps) == 0:
        return condition
    while -1 != condition.find("SuffixOf"):
        idx = condition.find("SuffixOf") + len("SuffixOf")
        orgCond, _ = getFirstPattern(condition[idx:])
        string, col = orgCond.strip("(").strip(")").split(",")
        if pyp is True:
            replacedString = "(" + col + " = \'__LIKE__ " + string[1:-1] + "\')"
        else:
            replacedString = "(" + col + " like \'%" + string[1:-1] + "\')"
        condition = condition.replace('SuffixOf' + orgCond, replacedString)
    return condition


def resetContainsCondition(condition, pyp):
    pattern = re.compile(r'\bContains\(SubString\b')
    grps = pattern.findall(condition)
    if len(grps) == 0:
        return condition
    while -1 != condition.find("Contains"):
        idx = condition.find("Contains") + len("Contains\(SubString")
        replacedCondition, _ = getFirstPattern(condition[condition.find("Contains") + len("Contains"):])
        orgCond, vLen = getFirstPattern(condition[idx - 1:])
        beginIdx = idx + vLen
        endIdx = condition[beginIdx:].find(",")
        if endIdx == -1:
            string = condition[beginIdx:].strip(",").strip(")")
        else:
            string = condition[beginIdx:beginIdx + endIdx].strip("(").strip(")").split(",")[-1]
        col = orgCond.strip("(").strip(")").split(",")[0]
        if pyp is True:
            replacedString = "(" + col + " = \'__LIKE__ %" + string[1:-1] + "%\')"
        else:
            replacedString = "(" + col + " like \'%" + string[1:-1] + "%\')"
        condition = condition.replace('Contains' + replacedCondition, replacedString)
    return condition


def resetLogicCondition(condition):
    outString = ""
    condition = condition.strip()
    if condition.upper().startswith("AND"):
        for cond in getBracePatterns(condition[4:].strip()[:-1]):
            if cond.strip().startswith("("):
                cond = cond.strip()[1:-1]
            outString += resetLogicCondition(cond) + " And "
            # outString += "(" + resetLogicCondition(cond) + ") And "
        outString = outString[:-5]
        # outString = "(" + outString[:-5] + ")"
    elif condition.upper().startswith("OR"):
        for cond in getBracePatterns(condition[3:]):
            # outString += "(" + resetLogicCondition(cond) + ") Or "
            if cond.strip().startswith("("):
                cond = cond.strip()[1:-1]
            outString += "(" + resetLogicCondition(cond) + ") Or "
        outString = "(" + outString[:-4] + ")"
        # print(outString)
    else:
        outString = condition
    return outString


def resetInCondition(condition):
    condition = condition.strip()
    if condition.upper().find(" == ANY (") == -1 and condition.upper().find(" == \"__ANY__{") == -1:
        return condition
    inPattern = re.compile(r'ANY \(\'\{(.+?)\}\'\[\]\)\)')
    grps = inPattern.findall(condition)
    outString = condition
    for grp in grps:
        inList = "', '".join(grp.split(","))
        outString = outString.replace(" == ANY ('{" + grp + "}'[])", " in ('" + inList + "')")
    if len(grps) == 0:
        inPattern = re.compile(r'ANY__\{(.+?)\}')
        grps = inPattern.findall(condition)
        for grp in grps:
            inList = "', '".join([x.strip("\"") for x in grp.split(",")])
            outString = outString.replace(" == \"__ANY__{" + grp + "}\"", " in ('" + inList + "')")
    return outString


def resetLikeCondition(condition):
    condition = condition.strip()
    if condition.upper().find("LIKE__%") == -1:
        return condition
    condition = condition.replace("== \"__LIKE__", "like \"")
    condition = condition.replace("== \"__NOTLIKE__", "not like \"")
    return condition


def combineOperand(operator, operands):
    if len(operands) == 1:
        if not operands[0]:
            return
        if operator == "isNotNull":
            tmpCondition = "(" + operands[0] + " != \'\')"
        elif operator == "isNull":
            tmpCondition = "(" + operands[0] + " == \'\')"
        else:
            print("operator:{} with one operand".format(operator))
            return
        return tmpCondition

    if len(operands) > 1:
        if not operands[0]:
            return
        if not operands[1]:
            return
        if operator == "and":
            tmpCondition = "And(" + ",".join(operands) + ")"
        elif operator == "or":
            tmpCondition = "Or((" + "), (".join(operands) + "))"
        elif operator == "in":
            tmpCondition = "Or("
            for candidate in operands[1].split(","):
                tmpCondition += "(" + operands[0] + " == " + candidate.strip("(").strip(")") + "),"
            #     去掉最后一个逗号，加上最外层的括号
            tmpCondition = tmpCondition.strip(",") + ")"
        elif len(operands) == 2:
            left_value = operands[0]
            right_value = operands[1]
            if operator == "equals":
                tmpCondition = "(" + left_value + " == " + right_value + ")"
            elif operator == "notEquals":
                tmpCondition = "(" + left_value + " != " + right_value + ")"
            elif operator == "greater":
                tmpCondition = "(" + left_value + " > " + right_value + ")"
            elif operator == "greaterOrEquals":
                tmpCondition = "(" + left_value + " >= " + right_value + ")"
            elif operator == "less":
                tmpCondition = "(" + left_value + " < " + right_value + ")"
            elif operator == "lessOrEquals":
                tmpCondition = "(" + left_value + " <= " + right_value + ")"
            elif operator == "plus":
                tmpCondition = "(" + left_value + " + " + right_value + ")"
            elif operator == "minus":
                tmpCondition = "(" + left_value + " - " + right_value + ")"
            elif operator == "like":
                tmpCondition = transRegStr(left_value, right_value)
            elif operator == "notLike":
                tmpCondition = "Not (" + transRegStr(left_value, right_value) + ")"
            else:
                print("operator:{} with 2 operands".format(operator))
                return
            return tmpCondition
        else:
            print("operator:{} with more than 2 operands{}".format(operator, operands))
            return
        return tmpCondition


def transCol2Var(rel, col, names, analyze=True):
    if re.match(r'^[a-z][a-z0-9_]*\.[a-z][a-z0-9_]*$', col) is not None:
        joinRelationL, joinColumnL = col.split(".")
        if joinRelationL == rel:
            tmp_value = joinColumnL
        else:
            tmp_value = joinColumnL + "Of" + joinRelationL
        # print("rel:{},joinRelationL:{},joinColumnL:{}".format(rel,joinRelationL,joinColumnL ))
        if analyze is True:
            if joinRelationL.upper() in g_table.data and joinColumnL.upper() in g_table.data[joinRelationL.upper()]:
                if g_table.data[joinRelationL.upper()][joinColumnL.upper()].strip(",") in stringType:
                    names["Literal"].append(tmp_value)
                else:
                    names["Number"].append(tmp_value)
        return tmp_value
    return col


# def transSingleCond(rel, singleCondition, analyze=True):
#     condition = singleCondition
#     names = {"Literal": [], "Number": []}
#     joinCondition = {}
#     # 获取最外层的函数
#     # var_pattern = re.compile('([A-Za-z]\w*[\.[A-Za-z]\w*]*)[,]')
#     outer_pattern = re.compile('([A-Za-z]*?)\(.*\)')
#     # outer_pattern = re.compile('(,*?)(.*?)\(.*\)')
#     condGrp = outer_pattern.findall(condition)
#     if len(condGrp) == 0 or len(condGrp[0]) == 0:
#         # 只有一个参数，没有函数
#         # 如equals(keyword.id, plus(3, 2))只传入keyword.id时
#         colName = transCol2Var(rel, condition.strip(), names, analyze)
#         if colName.isdigit():
#             return colName, names, {}
#         if -1 == colName.find("'"):
#             names["Literal"].append(colName)
#         return colName, names, {}
#     operator = condGrp[0]
#     # 获取最外层括号中的内容
#     inner_pattern = re.compile('\((.*)\)')
#     condition = inner_pattern.findall(condition)
#     # 如果还有括号，继续获取
#     transedCond = []
#     if inner_pattern.findall(condition[0]):
#         refKey = []
#         # 获取逗号分隔的括号内容或者独立参数
#         for cond in getBracePatterns(condition[0]):
#             # print(condition[0])
#             # print(cond)
#             subCondition, subNames, subRefCondition = transSingleCond(rel, cond, analyze)
#             transedCond.append(subCondition)
#             for interKey in list(set(joinCondition.keys()).intersection(set(subRefCondition.keys()))):
#                 subRefCondition[interKey] = operator[0].upper() + operator[1:] + "(" + joinCondition[interKey] + ", " \
#                                             + subRefCondition[interKey] + ")"
#             joinCondition = dict(joinCondition, **subRefCondition)
#             if len(subNames["Literal"]) != 0:
#                 if len(names["Literal"]) == 0:
#                     names["Literal"] = subNames["Literal"]
#                 else:
#                     names["Literal"] = list(set(names["Literal"]) | set(subNames["Literal"]))
#             if len(subNames["Number"]) != 0:
#                 if len(names["Number"]) == 0:
#                     names["Number"] = subNames["Number"]
#                 else:
#                     names["Number"] = list(set(names["Number"]) | set(subNames["Number"]))
#             if checkColName(rel, cond):
#                 refKey.append(cond)
#         tmpCondition = combineOperand(operator, transedCond)
#         for refCol in refKey:
#             joinCondition[refCol] = tmpCondition
#         # print(tmpCondition)
#         # exit()
#         return tmpCondition, names, joinCondition
#
#     # 如果没有括号，处理普通参数
#     # 引号引起来的部分作为一个参数
#     inner_pattern = re.compile('[\'](.*?)[\']')
#     paras = inner_pattern.findall(condition[0])
#     if len(paras) == 0:
#         name_list = []
#         tmp = condition[0].split(",")
#         for tmp_value in tmp:
#             tmp_value = transCol2Var(rel, tmp_value.strip(), names, analyze)
#             name_list.append(tmp_value)
#         tmpCondition = combineOperand(operator, name_list)
#         for refRel in tmp:
#             if checkColName(rel, refRel.strip()):
#                 joinCondition[refRel.strip()] = tmpCondition
#                 if re.match(r'^[a-z][a-z0-9_]*$', refRel.strip()) is not None:
#                     if len(names["Literal"]) != 0:
#                         names["Literal"].append(refRel.strip())
#                     else:
#                         names["Number"].append(refRel.strip())
#         return tmpCondition, names, joinCondition
#     elif len(paras) == 1:
#         refRelation = []
#         tmp = condition[0].split(",")
#         if tmp[0].find("'") == -1:
#             left_value = tmp[0]
#             right_value = "'" + paras[0] + "'"
#             if re.match(r'^[a-z][a-z0-9_]*\.[a-z][a-z0-9_]*$', left_value) is not None:
#                 joinRelationL, joinColumnL = left_value.split(".")
#                 if joinRelationL == rel:
#                     left_value = joinColumnL
#                 else:
#                     left_value = joinColumnL + "Of" + joinRelationL
#                     refRelation.append(joinRelationL + "." + joinColumnL)
#             names["Literal"].append(left_value)
#         else:
#             left_value = "'" + paras[0] + "'"
#             right_value = tmp[-1]
#             if re.match(r'^[a-z][a-z0-9_]*\.[a-z][a-z0-9_]*$', right_value) is not None:
#                 joinRelationL, joinColumnL = right_value.split(".")
#                 if joinRelationL == rel:
#                     right_value = joinColumnL
#                 else:
#                     right_value = joinColumnL + "Of" + joinRelationL
#                     refRelation.append(joinRelationL + "." + joinColumnL)
#             names["Literal"].append(right_value)
#         tmpCondition = combineOperand(operator, [left_value, right_value])
#         for refRel in refRelation:
#             joinCondition[refRel] = tmpCondition
#         return tmpCondition, names, joinCondition


def transSingleCond(rel, singleCondition, analyze=True):
    condition = singleCondition
    names = {"Literal": [], "Number": []}
    joinCondition = {}
    # 获取最外层的函数
    # var_pattern = re.compile('([A-Za-z]\w*[\.[A-Za-z]\w*]*)[,]')
    outer_pattern = re.compile('([A-Za-z]*?)\(.*\)')
    # outer_pattern = re.compile('(,*?)(.*?)\(.*\)')
    condGrp = outer_pattern.findall(condition)
    if len(condGrp) == 0 or len(condGrp[0]) == 0:
        # 只有一个参数，没有函数
        # 如equals(keyword.id, plus(3, 2))只传入keyword.id时
        colName = transCol2Var(rel, condition.strip(), names, analyze)
        if colName.isdigit():
            return colName, names, {}
        if -1 == colName.find("'"):
            names["Literal"].append(colName)
        return colName, names, {}
    operator = condGrp[0]
    # 获取最外层括号中的内容
    inner_pattern = re.compile('\((.*)\)')
    condition = inner_pattern.findall(condition)
    # 如果还有括号，继续获取
    transedCond = []
    if inner_pattern.findall(condition[0]):
        refKey = []
        # 获取逗号分隔的括号内容或者独立参数
        for cond in getBracePatterns(condition[0]):
            # print(condition[0])
            # print(cond)
            subCondition, subNames, subRefCondition = transSingleCond(rel, cond, analyze)
            if subCondition:
                transedCond.append(subCondition)
            for interKey in list(set(joinCondition.keys()).intersection(set(subRefCondition.keys()))):
                if not joinCondition[interKey]:
                    continue
                subRefCondition[interKey] = operator[0].upper() + operator[1:] + "(" + joinCondition[interKey] + ", " \
                                            + subRefCondition[interKey] + ")"
            joinCondition = dict(joinCondition, **subRefCondition)
            if len(subNames["Literal"]) != 0:
                for subCol in subNames["Literal"]:
                    if subCol not in joinCondition.keys():
                        refKey.append(subCol)
                if len(names["Literal"]) == 0:
                    names["Literal"] = subNames["Literal"]
                else:
                    names["Literal"] = list(set(names["Literal"]) | set(subNames["Literal"]))
            if len(subNames["Number"]) != 0:
                for subCol in subNames["Number"]:
                    if subCol not in joinCondition.keys():
                        refKey.append(subCol)
                if len(names["Number"]) == 0:
                    names["Number"] = subNames["Number"]
                else:
                    names["Number"] = list(set(names["Number"]) | set(subNames["Number"]))
            if checkColName(rel, cond):
                refKey.append(cond)
        tmpCondition = combineOperand(operator, transedCond)
        if tmpCondition:
            for refCol in refKey:
                joinCondition[refCol] = tmpCondition
        # print(tmpCondition)
        # exit()
        return tmpCondition, names, joinCondition

    # 如果没有括号，处理普通参数
    # 引号引起来的部分作为一个参数
    inner_pattern = re.compile('[\'](.*?)[\']')
    paras = inner_pattern.findall(condition[0])
    if len(paras) == 0:
        name_list = []
        tmp = condition[0].split(",")
        for tmp_value in tmp:
            tmp_value = transCol2Var(rel, tmp_value.strip(), names, analyze)
            name_list.append(tmp_value)
        tmpCondition = combineOperand(operator, name_list)
        for refRel in tmp:
            if checkColName(rel, refRel.strip()):
                if tmpCondition is not None:
                    joinCondition[refRel.strip()] = tmpCondition
                if re.match(r'^[a-zA-Z][a-zA-Z0-9_]*$', refRel.strip()) is not None:
                    if len(names["Literal"]) != 0:
                        names["Literal"].append(refRel.strip())
                    else:
                        names["Number"].append(refRel.strip())
        if tmpCondition is None:
            tmpCondition = singleCondition
        return tmpCondition, names, joinCondition
    elif len(paras) == 1:
        refRelation = []
        tmp = condition[0].split(",")
        if tmp[0].find("'") == -1:
            left_value = tmp[0]
            right_value = "'" + paras[0] + "'"
            if re.match(r'^[a-z][a-z0-9_]*\.[a-z][a-z0-9_]*$', left_value) is not None:
                joinRelationL, joinColumnL = left_value.split(".")
                if joinRelationL == rel:
                    left_value = joinColumnL
                else:
                    left_value = joinColumnL + "Of" + joinRelationL
                    refRelation.append(joinRelationL + "." + joinColumnL)
            names["Literal"].append(left_value)
        else:
            left_value = "'" + paras[0] + "'"
            right_value = tmp[-1]
            if re.match(r'^[a-z][a-z0-9_]*\.[a-z][a-z0-9_]*$', right_value) is not None:
                joinRelationL, joinColumnL = right_value.split(".")
                if joinRelationL == rel:
                    right_value = joinColumnL
                else:
                    right_value = joinColumnL + "Of" + joinRelationL
                    refRelation.append(joinRelationL + "." + joinColumnL)
            names["Literal"].append(right_value)
        tmpCondition = combineOperand(operator, [left_value, right_value])
        for refRel in refRelation:
            joinCondition[refRel] = tmpCondition
        return tmpCondition, names, joinCondition


# CH的条件已经是Z3支持的格式，现在要解析变量名和列名
def transCondCH(rel, planCondition):
    condition, names, refCondition = transSingleCond(rel, planCondition)
    # print("relation:{}\ncondition:{}, \nnames:{}, \nrefCols:{}".format(rel, condition, names, refCondition))
    return condition, names, refCondition


# def getTreeConditions_CH(planTree):
#     # 遍历树，将每个条件与相关的表关联
#     # node = planTree
#     def procNodeCondition(node):
#         # names = {"Literal": [], "Number": []}
#         # cond = "(1==1)"
#         if node.data is not None and hasattr(node.data, 'condition') and node.data.condition is not None:
#             if node.left is None:
#                 return
#             if node.left.data.type == NodeType.RELATION:
#                 rel = [node.left.data.relation]
#             else:
#                 rel = None
#             condTmp, namesTmp, refCondition = transCondCH(rel, node.data.condition)
#             # print("relation:{}\ncondition:{}, \nnames:{}, \nrefCols:{}".format(rel, condTmp, namesTmp, refCondition))
#             for refs, condition in refCondition.items():
#                 if -1 == refs.find("."):
#                     continue
#                 refRel = refs.split(".")[0]
#                 refCol = refs.split(".")[1]
#                 appendColCondition(planTree, refRel, refCol, condition)
#                 appendNames(planTree, refRel, namesTmp)
#         if node.left is not None:
#             procNodeCondition(node.left)
#         if node.right is not None:
#             procNodeCondition(node.right)
#
#     procNodeCondition(planTree)


def getTreeConditions_CH(planTree):
    # 遍历树，将每个条件与相关的表关联
    # node = planTree
    def procNodeCondition(node):
        # names = {"Literal": [], "Number": []}
        # cond = "(1==1)"
        needProc = True
        for rel, cond in node.relations.items():
            # 只要有一个不为空，就说明处理过了，不需要再处理
            if node.relations[rel]:
                needProc = False
                break
        if needProc and node.data is not None and hasattr(node.data, 'condition') and node.data.condition is not None:
            if node.left is None:
                return
            # if node.left.data.type == NodeType.RELATION:
            #     rel = [node.left.data.relation]
            # else:
            if node.leaveNodes is not None:
                rel = list(node.leaveNodes.keys())
            else:
                rel = None
            condTmp, namesTmp, refCondition = transCondCH(rel, node.data.condition)
            # print("relation:{}\ncondition:{}, \nnames:{}, \nrefCols:{}".format(rel, condTmp, namesTmp, refCondition))
            for refs, condition in refCondition.items():
                if -1 == refs.find("."):
                    continue
                refRel = refs.split(".")[0]
                refCol = refs.split(".")[1]
                appendColCondition(planTree, refRel, refCol, condition)
                appendNames(planTree, refRel, namesTmp)
        # 根据节点的遍历路径，在处理本节点之前，子节点已经处理好了
        # 把子节点的条件合并到本节点上面来
        if node.left is not None:
            procNodeCondition(node.left)
            for interKey in node.left.relations.keys():
                for innerKey, conds in node.left.relations[interKey].items():
                    if innerKey == 'variableNames':
                        appendNames(node, interKey, node.left.relations[interKey][innerKey])
                    else:
                        for cond in conds:
                            appendColCondition(node, interKey, innerKey, cond)
        if node.right is not None:
            procNodeCondition(node.right)
            for interKey in node.right.relations.keys():
                for innerKey, conds in node.right.relations[interKey].items():
                    if innerKey == 'variableNames':
                        appendNames(node, interKey, node.right.relations[interKey][innerKey])
                    else:
                        for cond in conds:
                            appendColCondition(node, interKey, innerKey, cond)
    procNodeCondition(planTree)



if __name__ == '__main__':
    test = resetLogicCondition("And((title!='x'),Or((title = '%pro%'),(title = '%compnay%')))")
    print(test)
