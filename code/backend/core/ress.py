import re
import time
from typing import DefaultDict, Dict, List, Tuple
from collections import Counter, defaultdict
import mviewParser as sqlparser
from procSQL import getTableFromItem

sql = """
   SELECT MIN(mc.note) AS production_note,
       MIN(t.title) AS movie_title,
       MIN(t.production_year) AS movie_year
FROM company_type AS ct,
     info_type AS it,
     movie_companies AS mc,
     movie_info_idx AS mi_idx,
     title AS t
WHERE ct.kind = 'production companies'
  AND it.info = 'top 250 rank'
  AND mc.note NOT LIKE '%(as Metro-Goldwyn-Mayer Pictures)%'
  AND (mc.note LIKE '%(co-production)%'
       OR mc.note LIKE '%(presents)%')
  AND ct.id = mc.company_type_id
  AND t.id = mc.movie_id
  AND t.id = mi_idx.movie_id
  AND mc.movie_id = mi_idx.movie_id
  AND it.id = mi_idx.info_type_id;
    """


# 解析SQL语句
def get_itemTable(sql: str) -> Dict[str, set]:
    pattern = r'\w*[.]\w*'
    fields = re.findall(pattern, sql)
    item_table: DefaultDict[str, set] = {}
    item_table = defaultdict(set)

    for field in fields:
        # 根据'.'分割表和字段
        data = field.split('.', 1)
        item_table[data[0]].add(data[1])
    # 处理为list
    for item, itemsets in item_table.items():
        item_table[item] = [str(itemset) for itemset in itemsets]

    return item_table


# 新SQL解析，处理聚合函数，但是将聚合函数里的item也看做独立的项
def get_itemFunc(sql: str) -> Dict[str, set]:
    # 1.处理普通
    pattern = r'\w*[.]\w*'
    fields = re.findall(pattern, sql)
    item_table: DefaultDict[str, set] = {}
    item_table = defaultdict(set)
    for field in fields:
        # 根据'.'分割表和字段
        data = field.split('.', 1)
        item_table[data[0]].add(data[1])

    # 2.处理聚合函数
    pattern = r'\w*[(]\w*[.]\w*[)]'
    fields = re.findall(pattern, sql)
    for field in fields:
        # 聚合函数
        pat1 = r'\w*[(]'
        data1 = re.search(pat1, field)
        # 列
        pat2 = r'\w*[)]'
        data2 = re.search(pat2, field)
        # 收集的信息
        msg = data1.group() + data2.group()
        # 表名
        pat3 = r'\w*[.]'
        data3 = re.search(pat3, field)
        table_name = data3.group()[:-1]
        item_table[table_name].add(msg)

    # 处理为list
    for item, itemsets in item_table.items():
        item_table[item] = [str(itemset) for itemset in itemsets]

    return item_table


# 全新的SQL处理函数，不将聚合函数里的列看做单独item
def get_itemtable(sql: str) -> Dict[str, set]:
    # 1.处理普通
    pattern = r'[^ ]*[.][^ ]*'
    fields = re.findall(pattern, sql)
    item_table: DefaultDict[str, set] = {}
    item_table = defaultdict(set)
    # pattern，聚合函数测试
    patternTest = r'\w*[(]\w*[.]\w*[)]'
    for field in fields:
        # 根据'.'分割表和字段
        if re.search(patternTest, field) is not None:
            # 聚合函数
            pat1 = r'\w*[(]'
            data1 = re.search(pat1, field)
            # 列
            pat2 = r'\w*[)]'
            data2 = re.search(pat2, field)
            # 收集的信息
            msg = data1.group() + data2.group()
            # 表名
            pat3 = r'\w*[.]'
            data3 = re.search(pat3, field)
            table_name = data3.group()[:-1]
            item_table[table_name].add(msg)
        else:
            patt = r'\w*[.]\w*'
            reField = re.search(patt, field)
            field = reField.group()
            data = field.split('.', 1)
            item_table[data[0]].add(data[1])

    # 处理为list
    for item, itemsets in item_table.items():
        item_table[item] = [str(itemset) for itemset in itemsets]
    return item_table


def get_itemInCondition(sql: str, schemaDict, parser) -> Dict[str, set]:
    cond_table = defaultdict(set)
    # parser = sqlparser.Parser(sql)
    relations = parser.tables
    # print(parser.columns_dict)
    if "where" not in parser.columns_dict:
        return cond_table
    whereCol = parser.columns_dict['where']
    for col in whereCol:
        temp = col.split(".")
        if len(temp) > 2:
            cond_table[".".join(temp[:-1])].add(temp[-1])
        elif len(temp) > 1:
            cond_table[(temp[0])].add(temp[-1])
        else:
            curTable = getTableFromItem(temp[-1], relations, schemaDict)
            if curTable is not None:
                cond_table[curTable].add(temp[-1])

            # for table, cols in schemaDict.items():
            #     if temp[-1].upper() in cols:
            #         print(table)
            #         break
            # cond_table[relations].add(temp[-1])
    return cond_table
    wherePatt = re.compile(r'where ', re.IGNORECASE)
    whereMatch = wherePatt.search(sql)
    if "group_by" not in parser.columns_dict:
        # 没有grp语句，从where到结束
        conditionText = sql[whereMatch.span()[1]:]
    else:
        # 有group语句，从where到group by
        grpPatt = re.compile(r'group ', re.IGNORECASE)
        grpMatch = grpPatt.search(sql)
        conditionText = sql[whereMatch.span()[1]:grpMatch.span()[0]]

    # 处理所有条件
    # sepPatt = re.compile(r'and|or ', re.IGNORECASE)
    # sepMatch = sepPatt.search(conditionText).group()
    for tmpField in re.split(r'and |or ', conditionText, flags=re.I):
        # 提取条件中作用在列上的函数
        print(tmpField)

    # if sepMatch is None:
    #     tmpFeild = sql[whereMatch.span()[1]:]
    # else:
    #     tmpFeild = sql[whereMatch.span()[1]:sepMatch[0]]


# def get_subqueryItems(sql, schemaDict):
def get_itemstable(sql, schemaDict, withDict=None):
    itemAll = defaultdict(set)
    condAll = defaultdict(set)
    item_table = defaultdict(set)

    parser = sqlparser.MViewParser(sql)
    # 如果有with查询，获取每个with中取的字段名
    if len(parser.with_queries) != 0:
        if withDict is None:
            withDict = {}
        for withname, withq in parser.with_queries.items():
            item_table, cond_table = get_itemstable(withq, schemaDict)
            for k, v in item_table.items():
                withDict[withname] = {k : [i for i in v]}
    # 如果有子查询，获取每个子查询中的字段名
    if len(parser.subqueries) != 0:
        for subq in parser.subqueries.values():
            item_table, cond_table = get_itemstable(subq, schemaDict, withDict)
            for item_name, item in item_table.items():
                itemAll[item_name].update(item)
            for item_name, item in cond_table.items():
                condAll[item_name].update(item)
    cond_table = get_itemInCondition(sql, schemaDict, parser)
    # 解析本层的item和condition
    try:
        nickToNameMap = parser.tables_aliases
        relations = parser.tables
        columnAlias = parser.columns_aliases
        columns = parser.columns_dict["select"]
    except Exception as e:
        print(e)
    # 提取出select后的字段名
    tmpFields = None
    selPattern = r'[Ss][Ee][Ll][Ee][Cc][Tt] '
    matchLeft = re.search(selPattern, sql)
    fromPattern = r' [Ff][Rr][Oo][Mm][ (]'
    matchRight = re.search(fromPattern, sql)
    # 如果括号不匹配，再继续往后找
    newText = sql[matchLeft.span()[1]: matchRight.span()[0]]
    lastEndPos = matchRight.span()[1]
    # 如果是*，要判断后面是不是有跟着子查询
    # 如果没有跟着子查询，则是表中的所有字段.也有可能是with查询的所有字段
    # 如果有子查询，则获取子查询中的所有字段
    if newText.strip() == "*":
        if len(parser.subqueries) == 0:
            tmpFields = []
            for rel in relations:
                if rel.upper() in schemaDict:
                    tmpFields += [k for k in schemaDict[rel.upper()].keys()]
                elif rel in withDict:
                    tmpFields += [str(i) for k in withDict[rel].values() for i in k]
        # subSelPos = sql[lastEndPos:].upper().find("SELECT")
        # if subSelPos == -1 or subSelPos > 5:
        #     tmpFields = columns
        # else:
        #     matchLeft = re.search(selPattern, sql[lastEndPos:])
        #     matchRight = re.search(fromPattern, sql[lastEndPos:])
        #     newText = sql[matchLeft.span()[1] + lastEndPos: matchRight.span()[0] + lastEndPos]
        #     lastEndPos += matchRight.span()[1]
    while newText.count("(") != newText.count(")"):
        matchRight = re.search(fromPattern, sql[lastEndPos:])
        if matchRight is not None:
            newText = sql[matchLeft.span()[1]: matchRight.span()[0] + lastEndPos]
            lastEndPos += matchRight.span()[1]
        else:
            lastEndPos = sql.find(")", lastEndPos)+1
            newText = sql[matchLeft.span()[1]: lastEndPos]
    if tmpFields is None:
        tmpFields = newText.split(",")
    # 普通函数中需要多个参数的时候，也有可能有逗号
    fields = []
    i = 0
    while i < len(tmpFields):
        fields.append(tmpFields[i])
        if fields[-1].count("(") == fields[-1].count(")"):
            i = i + 1
            continue
        while fields[-1].count("(") != fields[-1].count(")"):
            for j in range(i + 1, len(tmpFields)):
                if tmpFields[j].find(")") == -1:
                    continue
                del fields[-1]
                fields.append(",".join(tmpFields[i:j + 1]))
                if fields[-1].count("(") == fields[-1].count(")"):
                    break
            i = j + 1

    # 以TPC-DS的query40为例，parser.tables可能不完整（显示的写出join的情况）
    # 提取from和where之间的字符
    if "join" in parser.columns_dict:
        fromPattern = re.compile(r' from[ (]', re.IGNORECASE)
        matchLeft = re.search(fromPattern, sql)
        matchRight = None
        if "where" in parser.columns_dict:
            wherePattern = re.compile(r' where ', re.IGNORECASE)
            matchRight = re.search(wherePattern, sql)
        elif "groupby" in parser.columns_dict:
            wherePattern = re.compile(r' group by ', re.IGNORECASE)
            matchRight = re.search(wherePattern, sql)
        elif "orderby" in parser.columns_dict:
            wherePattern = re.compile(r' order by ', re.IGNORECASE)
            matchRight = re.search(wherePattern, sql)
        if matchRight is not None:
            newText = sql[matchLeft.span()[1]: matchRight.span()[0]]
        else:
            newText = sql[matchLeft.span()[1]:]

        tmpRels = newText.split(",")
        for rel in tmpRels:
            if rel.find("(") != -1:
                continue
            relations.append(rel.strip().split()[0])
        relations = list(set(relations))
    # 提取where后的字段名（包括聚合函数）
    # pattern，聚合函数测试
    patternTest = r'\w*[(]["]\w*["][.]["]\w*["][)]'
    for fieldWithAlias in fields:
        field = ""
        # field = " ".join(fieldWithAlias.split()[:-1]).strip("AS").strip("as")
        asPatt = re.compile(r' as ', re.IGNORECASE)
        asMatch = None
        iterAs = re.finditer(asPatt, fieldWithAlias)
        # 这个for循环什么也不做，只为取最后一个as
        for asMatch in iterAs:
            pass
        if asMatch is not None:
            if fieldWithAlias[asMatch.span()[1]:].strip().strip("`") in columnAlias:
                field = fieldWithAlias[:asMatch.span()[0]].strip()
                fieldItem = columnAlias[fieldWithAlias[asMatch.span()[1]:].strip().strip("`")]
        if field == "":
            if fieldWithAlias.split()[-1] in columnAlias:
                field = fieldWithAlias.replace(" " + fieldWithAlias.split()[-1], "").strip()
                fieldItem = columnAlias[fieldWithAlias.split()[-1]]
            else:
                field = fieldWithAlias.strip()
                fieldItem = field
        if field.find("(") != -1:
            # # 聚合函数
            # 表名
            pat3 = r'["]?[a-zA-Z][a-zA-Z0-9_]*["]?[.]'
            # pat3 = r'["]?\w*["]?[.]'
            data3 = re.findall(pat3, field)
            # 表名为空的情况 count(1), count(*)等
            if len(data3) == 0:
                if type(fieldItem) == str:
                    fItem = fieldItem
                    curTable = getTableFromItem(fItem, relations, schemaDict)
                    if curTable is None:
                        for table in relations:
                            item_table[table].add(field)
                            if fItem != "*" and fItem != "1":
                                item_table[table].add(fItem)
                    else:
                        item_table[curTable].add(field)
                        if fItem != "*" and fItem != "1":
                            item_table[curTable].add(fItem)
                else:
                    for fItem in fieldItem:
                        curTable = getTableFromItem(fItem, relations, schemaDict)
                        if curTable is None:
                            for table in relations:
                                item_table[table].add(field)
                                if fItem != "*" and fItem != "1":
                                    item_table[table].add(fItem)
                        else:
                            item_table[curTable].add(field)
                            if fItem != "*" and fItem != "1":
                                item_table[curTable].add(fItem)
            elif len(data3) == 1:
                table_name = data3[0][:-1].strip("\"")
                item_table[nickToNameMap[table_name]].add(field.replace(data3[0], ""))
                # 聚合函数和原始的列名都加入
                item_table[nickToNameMap[table_name]].add(fieldItem.split(".")[-1])
            else:
                idx = 0
                for i in data3:
                    table_name = i[:-1].strip("\"")
                    item_table[nickToNameMap[table_name]].add(field.replace(i, ""))
                    # 聚合函数和原始的列名都加入
                    item_table[nickToNameMap[table_name]].add(fieldItem[idx].split(".")[-1])
                    idx += 1
        else:
            patt = r'["]?\w*["]?[.]["]?\w*["]?'
            reField = re.search(patt, field)
            if reField is not None:
                field = reField.group()
                data = field.split('.', 1)
                if data[0].strip("\"") in nickToNameMap:
                    item_table[nickToNameMap[data[0].strip("\"")]].add(data[1].strip("\""))
                else:
                    item_table[data[0].strip("\"")].add(data[1].strip("\""))
            else:
                # patt = r'["]?\w*["]?'
                # reField = re.search(patt, field)
                if withDict is not None:
                    withRel = [str(i) for k in withDict.values() for i in k.keys()]
                    relations = list(set(relations).union(set(withRel)))
                curTable = getTableFromItem(field, relations, schemaDict)
                if curTable is not None:
                    item_table[curTable].add(field.strip("\""))

    # 处理为list
    # for item, itemsets in item_table.items():
    #     item_table[item] = [str(itemset).strip() for itemset in itemsets]
    for item_name, item in item_table.items():
        itemAll[item_name].update(item)
    for item_name, item in cond_table.items():
        condAll[item_name].update(item)
    return itemAll, condAll



if __name__ == "__main__":
    item_table = get_itemstable(sql)
    for item, itemset in item_table.items():
        print(item + " , " + str(itemset))
