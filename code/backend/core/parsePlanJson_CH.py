# from graphviz import Digraph
# from random import sample
# from basePlan import *
from procSQL import *
from parsePipelineTime_CH import *
from procCondition_CH import *
import json
import sys
import copy

# from z3 import *
# from mvpp import *

pipeline2Plan = {
    "MergeTree": "MERGE",
    "MergeTreeThread": "MERGE",
    "ExpressionTransform": "EXPRESSION",
    "JoiningTransform": "JOIN",
    "FilterTransform": "FILTER",
    "AggregatingTransform": "PROJECT"
}


# def getChildNode_CH(node, alias2table, relations, index, itemOfTable):
#     nodeType = node['Node Type']
#     if 'Aggregating' == nodeType:
#         attrs = []
#         if 'Keys' in node:
#             for key in node['Keys']:
#                 attrs.append(changeAlias2Table(key, alias2table))
#                 # attrs.append(key)
#
#         for aggNode in node['Aggregates']:
#             aggName = aggNode['Name']
#             # 匹配括号中的内容（列名）
#             pattern = re.compile(r"[(](.*?)[)]")
#             attr = pattern.findall(aggName)[0].strip("-")
#             attrs.append(changeAlias2Table(attr, alias2table))
#             # attrs.append(attr)
#
#         treeNode = PlanTree(data=ProjectNode(attrs))
#         return treeNode
#
#     if 'Filter' == nodeType:
#         condition = node["Filter Column"]
#         if condition == "0":
#             # treeNode = PlanTree(data=FilterNode())
#             # return treeNode
#             # 有过滤条件是0的查询，整个语句不再做处理
#             return None
#
#         # 匹配--开始的字段（判断条件）
#         pattern = re.compile('(\(.*?,.*?\))')
#         conds = pattern.findall(condition)
#         # 匹配单目运算符（isnull和isnotnull）
#         sigPattern = re.compile('(is[Not]*Null\(.*?\))')
#         sigConds = sigPattern.findall(condition)
#         conds += sigConds
#         for outer_cond in conds:
#             if outer_cond == "":
#                 continue
#             replacedCond = outer_cond
#             inner_pattern = re.compile('([-A-Za-z_0-9.]*)')
#             inner_conds = inner_pattern.findall(outer_cond)
#             # pure_cols = []
#             for cond in list(set(inner_conds)):
#                 if cond == "":
#                     continue
#                 for col in cond.strip("(").strip(")").split(","):
#                     col = col.strip()
#                     if col.find(".") != -1:
#                         replacedCol = changeAlias2Table(col.strip("-"), alias2table)
#                         # replacedCol = col.strip("-")
#                         replacedCond = replacedCond.replace(col, replacedCol)
#                     # else:
#                     # pure_cols.append(col)
#                     elif col in itemOfTable:
#                         if len(itemOfTable[col]) == 1:
#                             replacedCol = itemOfTable[col][0] + "." + col
#                             replacePat = re.compile(r'\.(\b' + col + r'\b)')
#                             replacedCond = replacePat.sub("replacedCol", replacedCond)
#                             replacePat = re.compile(r'\b' + col + r'\b')
#                             replacedCond = replacePat.sub(replacedCol, replacedCond)
#                             replacedCond = replacedCond.replace("replacedCol", "." + col)
#                             # replacedCond = replacedCond.replace(col, replacedCol)
#                         else:
#                             tables = []
#                             col_pattern = re.compile('[-A-Za-z_0-9]*\.' + col)
#                             tablesUsed = col_pattern.findall(node["Filter Column"])
#                             for table in tablesUsed:
#                                 tables.append(changeAlias2Table(table.strip("-"), alias2table).split(".")[0])
#                                 # tables.append(table.strip("-").split(".")[0])
#
#                             leftTable = list(set(itemOfTable[col]) - set(tables))
#                             if len(leftTable) == 0:
#                                 for table in relations:
#                                     # for table in alias2table.keys():
#                                     if relations.count(table) > 1:
#                                         lastTable = table
#                                         break
#                             elif len(leftTable) == 1:
#                                 lastTable = leftTable[0]
#                             else:
#                                 # 第一个包含该字段的表
#                                 for table in relations:
#                                     # for table in alias2table.keys():
#                                     if table in itemOfTable[col] and \
#                                             relations.count(table) > 1 and tables.count(table) <= 1:
#                                         lastTable = table
#                                         break
#                                     if table in leftTable:
#                                         lastTable = table
#                                         break
#                             replacedCol = lastTable + "." + col
#                             replacePat = re.compile(r'\.(\b' + col + r'\b)')
#                             replacedCond = replacePat.sub("replacedCol", replacedCond)
#                             replacePat = re.compile(r'\b' + col + r'\b')
#                             replacedCond = replacePat.sub(replacedCol, replacedCond)
#                             replacedCond = replacedCond.replace("replacedCol", "." + col)
#                             # replacedCond = replacedCond.replace(col, replacedCol)
#             condition = condition.replace(outer_cond, replacedCond)
#
#         treeNode = PlanTree(data=FilterNode(condition))
#         return treeNode
#
#     if 'Join' == nodeType:
#         treeNode = PlanTree(data=JoinNode())
#         return treeNode
#
#     if 'Expression' == nodeType:
#         headers = node['Header']
#         attr = []
#         conds = []
#         for header in headers:
#             name = header["Name"].strip("-")
#             # if name.find("(") != -1:
#             #     conds.append(name)
#             # else:
#             attr.append(changeAlias2Table(name, alias2table))
#
#         condition = None
#         if len(conds) > 1:
#             condition = "And ".join(conds)
#         elif len(conds) == 1:
#             condition = conds[0]
#             # attr += header["Name"].strip("-") + ", "
#         treeNode = PlanTree(data=ExpressNode(attr, condition=condition))
#         return treeNode
#     if 'ReadFromMergeTree' == nodeType or 'ReadFromPreparedSource' == nodeType:
#         headers = node['Header']
#         attrs = []
#         conds = []
#         for header in headers:
#             name = header["Name"]
#             # if name.find("(") != -1:
#             #     conds.append(name)
#             # else:
#             attrs.append(name)
#         condition = None
#         if len(conds) > 1:
#             condition = "And ".join(conds)
#         elif len(conds) == 1:
#             condition = conds[0]
#
#         relNode = PlanTree(data=RelationNode(relations[index]))
#         treeNode = PlanTree(data=MergeNode(attrs, relation=relations[index], condition=condition), left=relNode)
#         relNode.ancestor = treeNode
#         return treeNode
#     rootNode = PlanTree()
#     return rootNode


def getChildNode_CH(node, alias2table, relations, index, itemOfTable):
    nodeType = node['Node Type']
    if 'Aggregating' == nodeType:
        attrs = []
        if 'Keys' in node:
            for key in node['Keys']:
                attrs.append(changeAlias2Table(key, alias2table))
                # attrs.append(key)

        for aggNode in node['Aggregates']:
            aggName = aggNode['Name']
            # 匹配括号中的内容（列名）
            pattern = re.compile(r"[(](.*?)[)]")
            attr = pattern.findall(aggName)[0].strip("-")
            attrs.append(changeAlias2Table(attr, alias2table))
            # attrs.append(attr)

        treeNode = PlanTree(data=ProjectNode(attrs))
        return treeNode

    if 'Filter' == nodeType:
        condition = node["Filter Column"]
        if condition == "0":
            # treeNode = PlanTree(data=FilterNode())
            # return treeNode
            # 有过滤条件是0的查询，整个语句不再做处理
            return None

        # 匹配--开始的字段（判断条件）
        pattern = re.compile('(\(.*?,.*?\))')
        conds = pattern.findall(condition)
        # 匹配单目运算符（isnull和isnotnull）
        sigPattern = re.compile('(is[Not]*Null\(.*?\))')
        sigConds = sigPattern.findall(condition)
        conds += sigConds
        for outer_cond in conds:
            if outer_cond == "":
                continue
            replacedCond = outer_cond
            inner_pattern = re.compile('([-A-Za-z_0-9.]*)')
            inner_conds = inner_pattern.findall(outer_cond)
            # pure_cols = []
            for cond in list(set(inner_conds)):
                if cond == "":
                    continue
                for col in cond.strip("(").strip(")").split(","):
                    col = col.strip()
                    if col.find(".") != -1:
                        replacedCol = changeAlias2Table(col.strip("-"), alias2table)
                        replacedCond = replacedCond.replace(col, replacedCol)
                    elif col in itemOfTable[0]:
                        tmp_item_table = itemOfTable[0]
                        if len(tmp_item_table[col]) == 1:
                            replacedCol = tmp_item_table[col][0] + "." + col
                            replacePat = re.compile(r'\.(\b' + col + r'\b)')
                            replacedCond = replacePat.sub("replacedCol", replacedCond)
                            replacePat = re.compile(r'\b' + col + r'\b')
                            replacedCond = replacePat.sub(replacedCol, replacedCond)
                            replacedCond = replacedCond.replace("replacedCol", "." + col)
                            # replacedCond = replacedCond.replace(col, replacedCol)
                        else:
                            tables = []
                            col_pattern = re.compile('[-A-Za-z_0-9]*\.' + col)
                            tablesUsed = col_pattern.findall(node["Filter Column"])
                            for table in tablesUsed:
                                tables.append(changeAlias2Table(table.strip("-"), alias2table).split(".")[0])
                                # tables.append(table.strip("-").split(".")[0])

                            leftTable = list(set(tmp_item_table[col]) - set(tables))
                            if len(leftTable) == 0:
                                for table in relations:
                                    # for table in alias2table.keys():
                                    if relations.count(table) > 1:
                                        lastTable = table
                                        break
                            elif len(leftTable) == 1:
                                lastTable = leftTable[0]
                            else:
                                # 第一个包含该字段的表
                                for table in relations:
                                    # for table in alias2table.keys():
                                    if table in tmp_item_table[col] and \
                                            relations.count(table) > 1 and tables.count(table) <= 1:
                                        lastTable = table
                                        break
                                    if table in leftTable:
                                        lastTable = table
                                        break
                            replacedCol = lastTable + "." + col
                            replacePat = re.compile(r'\.(\b' + col + r'\b)')
                            replacedCond = replacePat.sub("replacedCol", replacedCond)
                            replacePat = re.compile(r'\b' + col + r'\b')
                            replacedCond = replacePat.sub(replacedCol, replacedCond)
                            replacedCond = replacedCond.replace("replacedCol", "." + col)
                            # replacedCond = replacedCond.replace(col, replacedCol)
            condition = condition.replace(outer_cond, replacedCond)

        treeNode = PlanTree(data=FilterNode(condition))
        return treeNode

    if 'Join' == nodeType:
        treeNode = PlanTree(data=JoinNode())
        return treeNode

    if 'Expression' == nodeType:
        headers = node['Header']
        attr = []
        conds = []
        for header in headers:
            name = header["Name"].strip("-")
            if name.find("'") != -1:
                conds.append(name)
            else:
                attr.append(changeAlias2Table(name, alias2table))

        condition = None
        if len(conds) > 1:
            condition = "And ".join(conds)
        elif len(conds) == 1:
            condition = conds[0]
            # attr += header["Name"].strip("-") + ", "
        treeNode = PlanTree(data=ExpressNode(attr, condition=condition))
        return treeNode
    if 'ReadFromMergeTree' == nodeType or 'ReadFromPreparedSource' == nodeType:
        headers = node['Header']
        attrs = []
        conds = []
        for header in headers:
            name = header["Name"]
            if name.find("'") != -1:
                conds.append(name)
            else:
                attrs.append(name)
        condition = None
        if len(conds) > 1:
            condition = "And ".join(conds)
        elif len(conds) == 1:
            condition = conds[0]

        rel_ = None
        for attr in attrs:
            if attr not in itemOfTable[0]:
                continue
            if rel_ is None:
                rel_ = set(itemOfTable[0][attr])
            else:
                rel_ = rel_.intersection(itemOfTable[0][attr])
            if len(rel_) == 1:
                break
        if len(rel_) > 1:
            tmp_rel_ = rel_.copy()
            for rel__ in tmp_rel_:
                if set(attrs) != itemOfTable[1][rel__]:
                    rel_.remove(rel__)
        relNode = PlanTree(data=RelationNode(list(rel_)[0]))
        treeNode = PlanTree(data=MergeNode(attrs, relation=list(rel_)[0], condition=condition), left=relNode)
        relNode.ancestor = treeNode
        return treeNode
    rootNode = PlanTree()
    return rootNode


def get_child_node_projection_CH(node, alias2table, relations, index, itemOfTable):
    nodeType = node['Node Type']
    if 'Aggregating' == nodeType:
        attrs = []
        if 'Keys' in node:
            for key in node['Keys']:
                attrs.append(changeAlias2Table(key, alias2table))
                # attrs.append(key)

        # for aggNode in node['Aggregates']:
        #     aggName = aggNode['Name']
        #     # 匹配括号中的内容（列名）
        #     pattern = re.compile(r"[(](.*?)[)]")
        #     attr = pattern.findall(aggName)[0].strip("-")
        #     attrs.append(changeAlias2Table(attr, alias2table))
        #     # attrs.append(attr)

        treeNode = PlanTree(data=ProjectNode(attrs))
        return treeNode

    if 'Filter' == nodeType:
        condition = node["Filter Column"]
        if condition == "0":
            # treeNode = PlanTree(data=FilterNode())
            # return treeNode
            # 有过滤条件是0的查询，整个语句不再做处理
            return None

        # 匹配--开始的字段（判断条件）
        pattern = re.compile('(\(.*?,.*?\))')
        conds = pattern.findall(condition)
        # 匹配单目运算符（isnull和isnotnull）
        sigPattern = re.compile('(is[Not]*Null\(.*?\))')
        sigConds = sigPattern.findall(condition)
        conds += sigConds
        for outer_cond in conds:
            if outer_cond == "":
                continue
            replacedCond = outer_cond
            inner_pattern = re.compile('([-A-Za-z_0-9.]*)')
            inner_conds = inner_pattern.findall(outer_cond)
            # pure_cols = []
            for cond in list(set(inner_conds)):
                if cond == "":
                    continue
                for col in cond.strip("(").strip(")").split(","):
                    col = col.strip()
                    if col.find(".") != -1:
                        replacedCol = changeAlias2Table(col.strip("-"), alias2table)
                        replacedCond = replacedCond.replace(col, replacedCol)
                    elif col in itemOfTable[0]:
                        tmp_item_table = itemOfTable[0]
                        if len(tmp_item_table[col]) == 1:
                            replacedCol = tmp_item_table[col][0] + "." + col
                            replacePat = re.compile(r'\.(\b' + col + r'\b)')
                            replacedCond = replacePat.sub("replacedCol", replacedCond)
                            replacePat = re.compile(r'\b' + col + r'\b')
                            replacedCond = replacePat.sub(replacedCol, replacedCond)
                            replacedCond = replacedCond.replace("replacedCol", "." + col)
                            # replacedCond = replacedCond.replace(col, replacedCol)
                        else:
                            tables = []
                            col_pattern = re.compile('[-A-Za-z_0-9]*\.' + col)
                            tablesUsed = col_pattern.findall(node["Filter Column"])
                            for table in tablesUsed:
                                tables.append(changeAlias2Table(table.strip("-"), alias2table).split(".")[0])
                                # tables.append(table.strip("-").split(".")[0])

                            leftTable = list(set(tmp_item_table[col]) - set(tables))
                            if len(leftTable) == 0:
                                for table in relations:
                                    # for table in alias2table.keys():
                                    if relations.count(table) > 1:
                                        lastTable = table
                                        break
                            elif len(leftTable) == 1:
                                lastTable = leftTable[0]
                            else:
                                # 第一个包含该字段的表
                                for table in relations:
                                    # for table in alias2table.keys():
                                    if table in tmp_item_table[col] and \
                                            relations.count(table) > 1 and tables.count(table) <= 1:
                                        lastTable = table
                                        break
                                    if table in leftTable:
                                        lastTable = table
                                        break
                            replacedCol = lastTable + "." + col
                            replacePat = re.compile(r'\.(\b' + col + r'\b)')
                            replacedCond = replacePat.sub("replacedCol", replacedCond)
                            replacePat = re.compile(r'\b' + col + r'\b')
                            replacedCond = replacePat.sub(replacedCol, replacedCond)
                            replacedCond = replacedCond.replace("replacedCol", "." + col)
                            # replacedCond = replacedCond.replace(col, replacedCol)
            condition = condition.replace(outer_cond, replacedCond)

        treeNode = PlanTree(data=FilterNode(condition))
        return treeNode

    if 'Join' == nodeType:
        treeNode = PlanTree(data=JoinNode())
        return treeNode

    if 'Expression' == nodeType:
        headers = node['Header']
        attr = []
        conds = []
        for header in headers:
            name = header["Name"].strip("-")
            if name.find("'") != -1:
                conds.append(name)
            else:
                attr.append(changeAlias2Table(name, alias2table))

        condition = None
        if len(conds) > 1:
            condition = "And ".join(conds)
        elif len(conds) == 1:
            condition = conds[0]
            # attr += header["Name"].strip("-") + ", "
        treeNode = PlanTree(data=ExpressNode(attr, condition=condition))
        return treeNode
    if 'ReadFromMergeTree' == nodeType or 'ReadFromPreparedSource' == nodeType:
        headers = node['Header']
        attrs = []
        conds = []
        for header in headers:
            name = header["Name"]
            # if name.find("(") != -1:
            #     conds.append(name)
            # else:
            attrs.append(name)
        condition = None
        if len(conds) > 1:
            condition = "And ".join(conds)
        elif len(conds) == 1:
            condition = conds[0]

        relNode = PlanTree(data=RelationNode(relations[index]))
        treeNode = PlanTree(data=MergeNode(attrs, relation=relations[index], condition=condition), left=relNode)
        relNode.ancestor = treeNode
        return treeNode
    rootNode = PlanTree()
    return rootNode


def get_projection_child_node_CH(node, projection_keys, group_keys, alias_table):
    nodeType = node['Node Type']
    if 'Aggregating' == nodeType:
        if 'Keys' in node:
            for key in node['Keys']:
                if key not in group_keys:
                    group_keys.append(key)
    if 'Expression' == nodeType:
        if "Description" in node and node["Description"] == "Projection":
            if "Header" not in node:
                return
            if "Expression" not in node:
                return
            # 处理Expression
            input_list = []
            output_list = []
            for head in node['Expression']['Inputs']:
                input_list.append(head['Name'])
            for head in node['Expression']['Outputs']:
                output_list.append(head['Name'])
            # 处理别名问题
            for index in range(len(input_list)):
                if input_list[index] != output_list[index]:
                    projection_keys.append(input_list[index] + " AS " + output_list[index])
                    alias_table[input_list[index]] = output_list[index]
                else:
                    projection_keys.append(input_list[index])
    if 'Filter' == nodeType:
        condition = node["Filter Column"]
        if condition == "0":
            return None
        with open("ch_condition.txt", "a+") as f:
            f.write(condition + "\n")


def getTreeNode_CH(queryid, root, alias2table, clusters, itemOfTable, relations, analyze=True, index=0):
    mvPrePath = getRawPath(DataType.MV)
    if mvPrePath is None:
        print("Please check the config file", sys._getframe().f_code.co_name)
        exit(1)
        return
    input_tables = []
    relationNodes = {}
    curNode = get_child_node_projection_CH(root, alias2table, relations, index, itemOfTable)
    if curNode is None:
        return None, None, None, index
    if curNode.left is not None and curNode.left.data.type == NodeType.RELATION:
        input_tables.append(curNode.left.data.relation)
        index += 1
        relationNodes[curNode.left.data.relation] = [curNode.left]

    if 'Plans' in root:
        for plan in root['Plans']:
            childNode, childTables, childRelNods, index = getTreeNode_CH(queryid, plan, alias2table, clusters,
                                                                         itemOfTable,
                                                                         relations, analyze, index)
            if childNode is None:
                return None, None, None, index
            # CLICKHOUSE的express和连续两个filter节点时，可以看做一个完整的子树
            if childNode.data is not None and childNode.data.type is not None \
                    and (childNode.data.type == NodeType.EXPRESSION
                         or (childNode.data.type == NodeType.FILTER and childNode.left.data.type == NodeType.FILTER)):
                childNode.leaveNodes = childRelNods
                childNode.plan = plan
                clusters.append(childNode)
                if set(childNode.relations.keys()) != set(childTables):
                    for rel in set(set(childTables) - set(childNode.relations.keys())):
                        childNode.relations[rel] = {}
                    for rel in set(set(childNode.relations.keys()) - set(childTables)):
                        del childNode.relations[rel]

            if analyze is True:
                getTreeConditions_CH(childNode)

            if childNode.data is not None and childNode.data.type is not None:
                childNode.ancestor = curNode
            if childNode.data is None and childNode.left is not None:
                if curNode.left is None:
                    curNode.left = childNode.left
                else:
                    curNode.right = childNode.left
                childNode.left.ancestor = curNode
            else:
                if curNode.left is None:
                    curNode.left = childNode
                else:
                    curNode.right = childNode
            input_tables += childTables
            for k, v in childRelNods.items():
                if k in relationNodes:
                    relationNodes[k] += copy.deepcopy(v)
                else:
                    relationNodes[k] = copy.deepcopy(v)
    return curNode, input_tables, relationNodes, index


def get_tree_node_projection_CH(queryid, root, alias2table, clusters, itemOfTable, relations, analyze=True, index=0):
    mvPrePath = getRawPath(DataType.MV)
    if mvPrePath is None:
        print("Please check the config file", sys._getframe().f_code.co_name)
        exit(1)
    input_tables = []
    relationNodes = {}
    curNode = get_child_node_projection_CH(root, alias2table, relations, index, itemOfTable)
    if curNode is None:
        return None, None, None, index
    # if curNode.left is not None and curNode.left.data.type == NodeType.RELATION:
    #     input_tables.append(curNode.left.data.relation)
    #     index += 1
    #     relationNodes[curNode.left.data.relation] = [curNode.left]

    if 'Plans' in root:
        for plan in root['Plans']:
            childNode, childTables, childRelNods, index = get_tree_node_projection_CH(queryid, plan, alias2table, clusters,
                                                                         itemOfTable,
                                                                         relations, analyze, index)
            if childNode is None:
                return None, None, None, index
            # CLICKHOUSE的express和连续两个filter节点时，可以看做一个完整的子树
            # if childNode.data is not None and childNode.data.type is not None \
            #         and (childNode.data.type == NodeType.EXPRESSION
            #              or (childNode.data.type == NodeType.FILTER and childNode.left.data.type == NodeType.FILTER)):
            #     childNode.leaveNodes = childRelNods
            #     childNode.plan = plan
            #     clusters.append(childNode)
            #
            #     if set(childNode.relations.keys()) != set(childTables):
            #         for rel in set(set(childTables) - set(childNode.relations.keys())):
            #             childNode.relations[rel] = {}
            #         for rel in set(set(childNode.relations.keys()) - set(childTables)):
            #             del childNode.relations[rel]
            clusters.append(childNode)
            if analyze is True:
                getTreeConditions_CH(childNode)

            if childNode.data is not None and childNode.data.type is not None:
                childNode.ancestor = curNode
            if childNode.data is None and childNode.left is not None:
                if curNode.left is None:
                    curNode.left = childNode.left
                else:
                    curNode.right = childNode.left
                childNode.left.ancestor = curNode
            else:
                if curNode.left is None:
                    curNode.left = childNode
                else:
                    curNode.right = childNode
            input_tables += childTables
            for k, v in childRelNods.items():
                # 不能直接赋值用相同地址
                if k in relationNodes:
                    relationNodes[k] += copy.deepcopy(v)
                else:
                    relationNodes[k] = copy.deepcopy(v)
    return curNode, input_tables, relationNodes, index


def get_projection_tree_node_CH(root, projection_keys, group_keys, alias_table):
    mvPrePath = getRawPath(DataType.MV)
    if mvPrePath is None:
        print("Please check the config file", sys._getframe().f_code.co_name)
        exit(1)
        return

    get_projection_child_node_CH(root, projection_keys, group_keys, alias_table)

    if 'Plans' in root:
        for plan in root['Plans']:
            get_projection_tree_node_CH(plan, projection_keys, group_keys, alias_table)


# def getTreeNode_CH(queryid, root, alias2table, clusters, itemOfTable, relations, analyze=True, index=0):
#     mvPrePath = getRawPath(DataType.MV)
#     if mvPrePath is None:
#         print("Please check the config file", sys._getframe().f_code.co_name)
#         exit(1)
#         return
#     input_tables = []
#     relationNodes = {}
#     curNode = getChildNode_CH(root, alias2table, relations, index, itemOfTable)
#     if curNode is None:
#         return None, None, None, index
#     if curNode.left is not None and curNode.left.data.type == NodeType.RELATION:
#         input_tables.append(curNode.left.data.relation)
#         index += 1
#         relationNodes[curNode.left.data.relation] = [curNode.left]
#
#     if 'Plans' in root:
#         for plan in root['Plans']:
#             childNode, childTables, childRelNods, index = getTreeNode_CH(queryid, plan, alias2table, clusters,
#                                                                          itemOfTable,
#                                                                          relations, analyze, index)
#             if childNode is None:
#                 return None, None, None, index
#             # index = len(childTables)
#             # CLICKHOUSE的express和连续两个filter节点时，可以看做一个完整的子树
#             if childNode.data is not None and childNode.data.type is not None \
#                     and (childNode.data.type == NodeType.EXPRESSION
#                          or (childNode.data.type == NodeType.FILTER and childNode.left.data.type == NodeType.FILTER)):
#                 if analyze is True:
#                     getTreeConditions_CH(childNode)
#                 childNode.leaveNodes = childRelNods
#                 childNode.plan = plan
#                 clusters.append(childNode)
#                 # print("\tgetTreeNode_CH:childNode.inputTables:{}".format(childTables))
#                 # print("\tgetTreeNode_CH:childNode.relations:  {}".format(childNode.relations.keys()))
#                 if set(childNode.relations.keys()) != set(childTables):
#                     for rel in set(set(childTables) - set(childNode.relations.keys())):
#                         childNode.relations[rel] = {}
#                     for rel in set(set(childNode.relations.keys()) - set(childTables)):
#                         del childNode.relations[rel]
#                 # print("\tAFTER ADJUST: getTreeNode_CH:childNode.inputTables:{}".format(childTables))
#                 # print("\tAFTER ADJUST: getTreeNode_CH:childNode.relations:  {}".format(childNode.relations.keys()))
#             if childNode.data is not None and childNode.data.type is not None:
#                 childNode.ancestor = curNode
#             if childNode.data is None and childNode.left is not None:
#                 if curNode.left is None:
#                     curNode.left = childNode.left
#                 else:
#                     curNode.right = childNode.left
#                 childNode.left.ancestor = curNode
#             else:
#                 if curNode.left is None:
#                     curNode.left = childNode
#                 else:
#                     curNode.right = childNode
#             input_tables += childTables
#             for k, v in childRelNods.items():
#                 if k in relationNodes:
#                     relationNodes[k] += v
#                 else:
#                     relationNodes[k] = v
#     return curNode, input_tables, relationNodes, index


def buildPlanTree_CH(planFile, sqlFile, analyze=True):
    filename, ext = os.path.splitext(os.path.basename(planFile))
    if ext != ".json":
        return None, None, True
    # if -1 != filename.find("_") or -1 != filename.find("-"):
    #     return None, None, True

    key, alias2table, relations, itemOfTable, join_lst = buildQueryID(sqlFile)
    # 如果alias2table，那么可能是单表情况，不处理
    if not alias2table:
        return build_plan_tree_projection_CH(planFile, sqlFile, key, alias2table, itemOfTable, relations, analyze)
    else:
        return build_plan_tree_table_CH(planFile, sqlFile, key, alias2table, itemOfTable, relations, join_lst, analyze)




# 多表的情况
def build_plan_tree_table_CH(planFile, sqlFile, key, alias2table, itemOfTable, relations, join_lst, analyze=True):
    f_open = open(planFile, "r", encoding='utf-8')
    root = json.load(f_open)
    f_open.close()
    try:
        root = root['Plan']
    except:
        root = root[0]['Plan']

    clusters = []
    childNode, referredRels, _, _ = getTreeNode_CH(key, root, alias2table, clusters, itemOfTable, relations, analyze)
    if childNode is None:
        return key, [], True
    clusters.append(childNode)
    getTreeConditions_CH(childNode)
    # 直接从SQL文件中读取每个表涉及的列名，放在build出来的tree结构中。
    if referredRels is not None:
        referredKeys = getReferredKeys(alias2table, sqlFile, referredRels, g_table.data)
        for tree in clusters:
            for rel in tree.relations.keys():
                tree.referKeys[rel] = referredKeys[rel]
    # 判断一些缺少的列，加入其中
    # for cluster in clusters:
    #     if len(cluster.relations.keys()) != 2:
    #         continue
    #     flag = False
    #     for rel, colCondition in cluster.relations.items():
    #         if flag:
    #             break
    #         for col, conditions in colCondition.items():
    #             if flag:
    #                 break
    #             if col == "variableNames":
    #                 continue
    #             pattern = r"([\w]+)Of([\w]+)[ ]*==[ ]*([\w]+)Of([\w]+)"
    #             for condition in conditions:
    #                 key_lst = re.findall(pattern, condition)
    #                 if len(key_lst) > 0:
    #                     flag = True
    #                     break
    #     if not flag:
    #         table1, table2 = list(cluster.relations.keys())[0], list(cluster.relations.keys())[1]
    #         for join_node in join_lst:
    #             if join_node.table_l == table1 and join_node.table_r == table2:
    #                 if join_node.token_l in cluster.relations[table1]:
    #                     cluster.relations[table1][join_node.token_l].append(join_node.filter)
    #                 else:
    #                     cluster.relations[table1][join_node.token_l] = [join_node.filter]
    #                 if join_node.token_r in cluster.relations[table2]:
    #                     cluster.relations[table2][join_node.token_r].append(join_node.filter)
    #                 else:
    #                     cluster.relations[table2][join_node.token_r] = [join_node.filter]
    #                 break
    #             elif join_node.table_l == table2 and join_node.table_r == table1:
    #                 if join_node.token_l in cluster.relations[table2]:
    #                     cluster.relations[table2][join_node.token_l].append(join_node.filter)
    #                 else:
    #                     cluster.relations[table2][join_node.token_l] = [join_node.filter]
    #                 if join_node.token_r in cluster.relations[table1]:
    #                     cluster.relations[table1][join_node.token_r].append(join_node.filter)
    #                 else:
    #                     cluster.relations[table1][join_node.token_r] = [join_node.filter]
    #                 break

    # 删除一些不存在的列
    for cluster in clusters:
        conditionList = []
        for rel, colCondition in cluster.relations.items():
            for col, conditions in colCondition.items():
                if col == "variableNames":
                    continue

                for condition in conditions:
                    patten = re.compile('\w+Of\w+')
                    colOfTables = patten.findall(condition)
                    for colOfTable in list(set(colOfTables)):
                        column, table = colOfTable.split("Of")
                        if table != rel and table not in cluster.relations.keys():
                            break
                        # condition = condition.replace(colOfTable, table+"."+column)
                        condition = re.sub(r"\b" + colOfTable + r"\b", table + "." + column, condition)
                    # print("condition:{}, resetCondition:{}".format(condition,resetCondition2Sql(condition)))
                    # print("condition:{}".format(condition))
                    # if len(colOfTables) == 0:
                    # condition = re.sub(r"\b"+col+r"\b", rel + "." + col, condition)
                    condition = resetCondition2Sql(condition)
                    cons = []
                    for c in condition.split("And"):
                        cons.append(c)
                    condition = " And ".join(cons)

                    if condition != '':
                        conditionList.append(condition.replace("==", "=").replace("\"", "'"))
        # 找到join条件，删除没有join的表
        pattern = r"([\w]+)\.[\w]+[ ]*\=[ ]*([\w]+)\.[\w]+"
        table_list = []
        for condition in conditionList:
            key_list = re.findall(pattern, condition)
            if len(key_list) > 0:
                table1, table2 = key_list[0][0], key_list[0][1]
                if table1 not in cluster.referKeys.keys() or table2 not in cluster.referKeys.keys():
                    continue
                table_list.append(table1)
                table_list.append(table2)
        # # 删除condition
        # for rel, colCondition in cluster.relations.items():
        #     for col, conditions in colCondition.items():
        #         if col == "variableNames":
        #             continue
        #         for condition in conditions:
        #             pattern = r"([\w]+)\.[\w]+"
        #             key = re.findall(pattern, condition)
        #             if len(key) > 0:
        #                 if len(key[0]) == 1:
        #                     table = key[0][0]
        #                     if table not in table_list:
        #                         cluster.relations[rel][col].remove(condition)

        table_list = list(set(table_list))
        for table in list(cluster.referKeys.keys()):
            if table not in table_list:
                del cluster.referKeys[table]
                del cluster.relations[table]
    # 删除cluster
    for cluster in clusters:
        if cluster.relations == {} or len(cluster.referKeys) == 0:
            clusters.remove(cluster)
            continue
        num = 0
        for rel, colCondition in cluster.relations.items():
            for col, conditions in colCondition.items():
                if col == "variableNames":
                    continue
                for condition in conditions:
                    # 注意，relations里是字段of表的形式
                    pattern = r"[\w]+Of[\w]+[ ]*==[ ]*[\w]+Of[\w]+"
                    key_list = re.findall(pattern, condition)
                    if len(key_list) == 0:
                        num += 1
        if num == 0:
            clusters.remove(cluster)

    return key, clusters, True


# 单表的情况
def build_plan_tree_projection_CH(planFile, sqlFile, key, alias2table, itemOfTable, relations, analyze=True):
    # 读取文件
    f_open = open(planFile, "r", encoding='utf-8')
    root = json.load(f_open)
    f_open.close()
    try:
        root = root['Plan']
    except:
        root = root[0]['Plan']
    # 执行算法
    clusters = []
    childNode, referredRels, _, _ = get_tree_node_projection_CH(key, root, alias2table, clusters, itemOfTable, relations, analyze)
    if childNode is None:
        return key, []
    clusters.append(childNode)
    # 直接从SQL文件中读取每个表涉及的列名，放在build出来的tree结构中。
    # if referredRels is not None:
    #     referredKeys = getReferredKeys(alias2table, sqlFile, referredRels, g_table.data)
    #     for tree in clusters:
    #         for rel in tree.relations.keys():
    #             tree.referKeys[rel] = referredKeys[rel]
    return key, clusters, False


def build_projection_tree_CH(planFile, sqlFile, analyze=True):
    filename, ext = os.path.splitext(os.path.basename(planFile))
    if ext != ".json":
        return None, None
    if -1 != filename.find("_"):
        return None, None

    f_open = open(planFile, "r", encoding='utf-8')
    root = json.load(f_open)
    f_open.close()
    try:
        root = root['Plan']
    except:
        root = root[0]['Plan']
    clusters = []
    projection_keys = []
    group_keys = []
    alias_table = {}
    get_projection_tree_node_CH(root, projection_keys, group_keys, alias_table)
    # 将group_keys里的别名替换
    for i, key in enumerate(group_keys):
        if key in alias_table.keys():
            group_keys[i] = alias_table[key]
    return projection_keys, group_keys


def mergeTree_CH(planTree, pipelineTree):
    curPipeNode = pipelineTree
    curPlanNode = planTree

    def maxCost(samePipeNodes):
        maxTime = 0.0
        for pipeNode in samePipeNodes:
            if not pipeNode:
                continue
            if pipeNode.cost > maxTime:
                maxTime = pipeNode.cost
        return maxTime

    def mergeNode(planNode, pipeNode):
        # print("planNode:{}, \npipeNode:{}".format(planNode.data, pipeNode.label))
        if planNode.data is None:
            return
        if pipeNode.label not in pipeline2Plan:
            if len(pipeNode.children) == 1:
                mergeNode(planNode, pipeNode.children[0])
            elif len(list(set([child.label for child in pipeNode.children]))) == 1:
                # 子节点相同（多线程）、子节点不同（layout时）两种情况
                planNode.data.cost = maxCost(pipeNode.children)
                # planNode.data.cost = pipeNode.cost
                planNode.left.data.cost = maxCost(child.children[0] if len(child.children) > 0 else None for child in pipeNode.children)
            else:
                for child in pipeNode.children:
                    if len(child.children) == 0:
                        continue
                    mergeNode(planNode, child)

        elif pipeline2Plan[pipeNode.label] == planNode.data.type.name:
            planNode.data.cost = pipeNode.cost
            if planNode.left is not None and len(pipeNode.children) != 0:
                mergeNode(planNode.left, pipeNode.children[0])
                if planNode.right is not None:
                    mergeNode(planNode.right, pipeNode.children[1])
        else:
            for child in pipeNode.children:
                mergeNode(planNode, child)

    if curPlanNode is None:
        return
    if curPipeNode.key is not None and curPlanNode.data is not None:
        mergeNode(curPlanNode, curPipeNode)


def buildOnePlanTree_CH(sqlFile, jsonFile, pipelineFile, analyze=False, threshold=0.5):
    pipeTree = buildPipelineTree(pipelineFile)
    # 如果整个查询的时间小于阈值，不做候选视图的解析
    if pipeTree.cost < threshold:
        return None, None, True
    queryId, planTrees, projection_flag = buildPlanTree_CH(jsonFile, sqlFile, analyze)
    if queryId is None:
        return None, None, True
    if len(planTrees) == 0:
        return None, None, True
    mergeTree_CH(planTrees[-1], pipeTree)
    return queryId, planTrees, projection_flag


