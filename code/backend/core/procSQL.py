import os
import re
import hashlib
import mviewParser as sqlparser

from parseSchema import *

g_table = tableSchema()

keyword_columns = []
for relation, columns in g_table.data.items():
    for column in columns:
        keyword_columns.append(relation + "." + column)


def getGlobalReplaceStr():
    orgString = r"#HW_REPLACE_STRING#"
    md5 = hashlib.md5()
    md5.update(orgString.encode("utf8"))
    return md5.hexdigest()


SPACE_REPLACE_STRING = getGlobalReplaceStr()


# SQL标准化
# 将换行用空格代替，连续空格只保留一个空格
def normalizeSql(sql):
    # remove the /* */ comments
    q = re.sub(r"/\*[^*]*\*+(?:[^*/][^*]*\*+)*/", "", sql)

    # remove whole line -- comments
    lines = [line for line in q.splitlines() if not re.match("^\s*(--)", line)]

    # remove trailing -- comments
    q = " ".join([re.split("--", line)[0] for line in lines])

    oneLineSql = q.replace("\n", " ")
    # 将判断条件中的空格用特殊字符代替
    # pattern = r"['\"](.*?\s.*?)['\"]"
    pattern = re.compile(r" ['\"](.*?)['\"] ")
    # mg = re.search(pattern, oneLineSql)
    replaced_sql = oneLineSql
    mg = pattern.findall(oneLineSql)
    for m in mg:
        mr = m.replace(" ", SPACE_REPLACE_STRING)
        replaced_sql = replaced_sql.replace(m, mr)

    # 将其他地方的多余空格去除只保留一个空格
    noSpaceSql = replaced_sql.split()
    densitySql = " ".join(noSpaceSql)
    # 还原判断条件中的空格
    normalizedSql = densitySql.replace(SPACE_REPLACE_STRING, " ")
    return normalizedSql


def sql2md5(sql):
    # sqlStandard = normalizeSql(sql)
    # print(sqlStandard)
    md5 = hashlib.md5()
    md5.update(sql.encode("utf8"))
    return md5.hexdigest()


def buildAlias2TableBySql(sql, alias2table):
    relations = []
    oneLineSql = sql.replace("\n", " ")
    # 匹配所有from和where之间的字符
    pattern = re.compile(r" [Ff][Rr][Oo][Mm]( [a-zA-Z].*? )[Ww][Hh][Ee][Rr][Ee]")
    mg = pattern.findall(oneLineSql)
    for m in mg:
        # 表之间用逗号分隔
        for t in m.split(","):
            # 表和别名之间用空格分隔
            # 如果没有别名，不处理
            if len(t.split()) == 1:
                relations.append(t.split(".")[-1].strip())
                continue
            # 表名和别名之间有可能有AS关键字，有可能没有。
            # alias不取第二个元素，而是取最后一个，适配两种情况
            alias = t.split()[-1]
            table = t.split()[0].split(".")[-1]
            alias2table[alias] = table
            relations.append(table)
            # relations.append(alias)
    return relations


# ===========================================================
# 将别名替换为原表名
# ===========================================================
def changeAlias2Table(column, alias2table):
    aliasName = column.split('.')[0]
    if len(column.split('.')) > 1 and aliasName.strip() in alias2table:
        columnName = column.split('.')[1]
        return alias2table[aliasName.strip()] + '.' + columnName
    else:
        return column


# 列名到表名的对应关系
def buildItemOfTable(parser, sql, alias2table=None, relations=None):
    # print("sql:{}, \n alias2table:{}, \n relations:{}".format(sql, alias2table, relations))
    fields = None
    try:
        # fields = sqlmeta.get_query_columns(sql)
        fields = parser.columns
        # print("fields:{}".format(fields))
    except Exception as e:
        print(e)
        if fields is None:
            pattern = r'[a-zA-Z]\w*[.][a-zA-z]\w*'
            fields = re.findall(pattern, sql)
    item_table = defaultdict(set)
    table_column = defaultdict(set)

    for field in fields:
        if field.find(".") == -1:
            continue
        # 根据'.'分割表和字段
        # if alias2table is None:
        #     data = field.split('.')
        # else:
        data = field.split('.')
        if relations is not None:
            mainData = data[-2]
            if mainData not in relations and mainData not in alias2table:
                continue
        data = changeAlias2Table(".".join(data[-2:]), alias2table).split('.')

        # item_table[data[0]].add(data[1])
        item_table[data[-1]].add(data[-2])
        table_column[data[-2]].add(data[-1])
    # 处理为list
    for item, itemSets in item_table.items():
        item_table[item] = [str(itemSet) for itemSet in itemSets]

    return item_table, table_column


class CH_Join_Node:
    def __init__(self, alas_l, token_l, alas_r, token_r, table_l, table_r, filter):
        self.alas_l = alas_l
        self.token_l = token_l
        self.alas_r = alas_r
        self.token_r = token_r
        self.table_l = table_l
        self.table_r = table_r
        self.filter = filter


def buildQueryID(sqlFile):
    key, ext = os.path.splitext(os.path.basename(sqlFile))
    if ext != ".sql":
        return None
    with open(sqlFile, "r", encoding='utf-8') as f:
        sql = f.read()
        sqlStandard = normalizeSql(sql)
        parser = sqlparser.MViewParser(sqlStandard)
        try:
            alias2table = parser.tables_aliases
            for a in list(alias2table.keys()):
                if a.lower() == "where":
                    del alias2table[a]
            # 直接使用函数获得的表名是去重之后的表名，在和执行计划匹配的时候，可能会出现不够匹配而越界的情况
            relations = parser.tables
            for i in range(len(relations)):
                relations[i] = relations[i].split(".")[-1]
            # 通过别名和表名互相比对，得到完整的表名
            j = 0
            for a, t in alias2table.items():
                if t.find(".") != -1:
                    t = t.split(".")[-1]
                    alias2table[a] = t
                if j == len(relations):
                    # 某个表重复
                    if t == relations[j - 1]:
                        relations.insert(j, t)
                        relations = relations
                        j += 1
                        continue
                if t == relations[j]:
                    j += 1
                    continue
                # 某个表重复
                if t in relations[:j]:
                    relations.insert(j, t)
                    relations = relations
                    j += 1
                    continue
                # 某些表没有别名
                while t != relations[j]:
                    j += 1
            # print("sql:{}, \n alias2table:{}".format(sqlStandard, alias2table))
            itemOfTable = buildItemOfTable(parser, sqlStandard, alias2table)
        except Exception as e:
            print(e)
            if e == "pop from empty list":
                alias2table = {}
                relations = buildAlias2TableBySql(sqlStandard, alias2table)
                itemOfTable = buildItemOfTable(sqlStandard, alias2table, relations)

        # 获取每个列的join条件
        join_lst = []
        if get_use_projection() == "False":
            token_list = parser.sqlparse_tokens[-1].tokens
            for token in token_list:
                token_value = token.value
                pattern = r"([\w]+)\.([\w]+)[ ]*\=[ ]*([\w]+)\.([\w]+)"
                key_lst = re.findall(pattern, token_value)
                if len(key_lst) == 0:
                    continue
                alas_l, token_l, alas_r, token_r = key_lst[0][0], key_lst[0][1], key_lst[0][2], key_lst[0][3]
                if alas_l in parser.tables_aliases and alas_r in parser.tables_aliases:
                    filter = token_l + "Of" + parser.tables_aliases[alas_l] + " = " + token_r + "Of" + parser.tables_aliases[alas_r]
                    join_lst.append(CH_Join_Node(alas_l, token_l, alas_r, token_r, parser.tables_aliases[alas_l], parser.tables_aliases[alas_r], filter))

        return sql2md5(sqlStandard), alias2table, relations, itemOfTable, join_lst


# 列名到表名的对应关系
def buildCol2Table(relations):
    col2Table = {}
    for rel in list(set(relations)):
        for col in g_table.data[rel.upper()]:
            if col in col2Table:
                col2Table[col].append(rel)
            else:
                col2Table[col] = [rel]
    return col2Table


def buildQueryIDs(path):
    fileNames = os.listdir(path)
    for file in fileNames:
        queryId, _ = buildQueryID(path + "/" + file)
        if queryId is None:
            continue
        print("file:{},its queryId is {} ".format(file, queryId))


def getTableFromItem(item, relations, schemaDict):
    schemaTable = [x.split(".")[-1] for x in schemaDict.keys()]
    for relation in relations:
        if relation.split(".")[-1].upper() in schemaTable:
            keyIdx = schemaTable.index(relation.split(".")[-1].upper())
            if item.upper() in schemaDict[list(schemaDict.keys())[keyIdx]]:
                return relation
    return None


def getReferredKeys(alias2table, sqlFile, relations, schemaDict):
    itemAll = defaultdict(list)
    # condAll = defaultdict(set)
    # keysAll = {}
    with open(sqlFile) as file:
        data = file.read()
        sql = normalizeSql(data)
        parser = sqlparser.MViewParser(sql)
        for col in parser.columns:
            if col.find(".") != -1:
                column = changeAlias2Table(col, alias2table)
                col = column.split(".")[-1]
                curTable = ".".join(column.split(".")[:-1])
                if curTable.upper() not in schemaDict.keys():
                    continue
            else:
                curTable = getTableFromItem(col, relations, schemaDict)
            if curTable is not None:
                # print("table:{}, col:{}".format(curTable, col))
                # print("list:{}\n==========".format(itemAll[curTable]))
                itemAll[curTable].append(col)
    return itemAll
# buildQueryIDs("D:/物化视图/REFERENCE/release/release/resource/queries/join-order-benchmark/")
# buildQueryID("D:/物化视图/REFERENCE/release/release/resource/queries/join-order-benchmark/10a.sql")
