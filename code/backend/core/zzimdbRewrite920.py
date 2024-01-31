import copy

import sqlglot
from sqlglot import parse_one, exp, transpile
# from helpers import files, facts, dimensions, features, get_files
import pandas as pd
import numpy as np
import re
import os
import time

table_job = {
    'aka_name',
    'aka_title',
    'cast_info',
    'char_name',
    'comp_cast_type',
    'company_name',
    'company_type',
    'complete_cast',
    'info_type',
    'keyword',
    'kind_type',
    'link_type',
    'movie_companies',
    'movie_info',
    'movie_info_idx',
    'movie_keyword',
    'movie_link',
    'name',
    'person_info',
    'role_type',
    'title'
}

def generate_key(key, counter):
    if key in counter:
        counter[key] += 1
    else:
        counter[key] = 1
    return f"{key}_{counter[key]}"

def map_tables_to_aliases(tables, aliases):
    counter = {}
    result = {}
    for table, alias in zip(tables, aliases):
        new_key = generate_key(table, counter)
        result[new_key] = alias
    return result

def reverse_dict(input_dict):
    return {v: k for k, v in input_dict.items()}

def get_tables_alias(query):
    parsed = parse_one(query)
    tables = parsed.find_all(exp.Table)
    aliases = parsed.find_all(exp.TableAlias)

    table_list = []
    alias_list = []
    for table in tables:
        name = table.alias_or_name
        table_list.append(name)

    for alias in aliases:
        name = alias.alias_or_name
        alias_list.append(name)

    alias_table_map  = dict(zip(alias_list, table_list))
    table_alias_map = reverse_dict(alias_table_map)

    return alias_table_map,table_alias_map


def get_column_alias(query):
    ala_col = {}
    parsed = parse_one(query)
    alias = parsed.find_all(exp.Alias)
    for i in alias:
        ala_col[i.alias] = str(i.this)

    col_ala = reverse_dict(ala_col)
    return ala_col,col_ala

def get_columns(query):
    col_res = set()
    parsed = parse_one(query)
    columns = parsed.find_all(exp.Column)
    for i in columns:
        col_res.add(str(i))

    return col_res

def get_tables_alias00(query):
    parsed = parse_one(query)
    tables = parsed.find_all(exp.Table)
    aliases = parsed.find_all(exp.TableAlias)

    table_list = []
    alias_list = []
    for table in tables:
        name = table.alias_or_name
        table_list.append(name)

    for alias in aliases:
        name = alias.alias_or_name
        alias_list.append(name)

    mapped_dict = map_tables_to_aliases(table_list,alias_list)
    print(11,mapped_dict)

    reversed_dict = reverse_dict(mapped_dict)

    table_list_n = [i for i in mapped_dict]


    alias_table_n_map  = dict(zip(alias_list, table_list))


    return reversed_dict,alias_table_n_map,mapped_dict


def get_tables(query):
    parsed = parse_one(query)
    tables = parsed.find_all(exp.Table)

    table_list = []
    for table in tables:
        name = table.alias_or_name
        table_list.append(name)

    return table_list

def get_wheres(query):
    parsed = parse_one(query)
    wheres = parsed.find_all(exp.Where)

    where_list = []
    for where in wheres:
        conditions = re.split('AND|OR', where.sql()[6:])
        where_list.append(conditions)

    return where_list

def contains_two_mv(mv, s):
    return len(re.findall(rf"\b{mv}\b", s)) == 2

def remove_brackets_and_spaces(s):

    s = s.strip()
    if s.startswith('(') and s.endswith(')'):
        return s[1:-1].strip()
    return s

def getMvName(sql):
    # 得到mv的名字
    pattern = re.compile(r"EXISTS (\w+) AS")
    match = pattern.search(sql)
    mv_name = match.group(1)
    return mv_name

def final(sql,mv,sqln_col_tmp,col_ala_v):
    print(1,sql)
    for col in sqln_col_tmp:
        print(111122331231, col)
        if col in sql and col in col_ala_v:
            sql = sql.replace(col, f" {mv}." + col_ala_v[col])
    return sql


def final0(sql,mv):
    parsed = parse_one(sql)
    tables = parsed.find_all(exp.Table)
    aliases = parsed.find_all(exp.TableAlias)

    table_list = []
    alias_list = []
    for table in tables:
        name = table.alias_or_name
        table_list.append(name)

    for alias in aliases:
        name = alias.alias_or_name
        alias_list.append(name)

    match = re.findall(r'(?<=MIN\()(\w+?(?=\.\w))', sql)

    for m in match:
        if m not in alias_list:
            sql = sql.replace(f"MIN({m}.", f"MIN({mv}.")

    return sql
def get_join_where(query,eq):
    eq_res = []
    parsed = parse_one(query)
    col = parsed.find_all(exp.Column)
    col_list = []
    for c in col:
        col_list.append(str(c))

    for e in eq:
        if e.split(" = ")[0] in col_list and e.split(" = ")[1] in col_list:
            eq_res.append(e)

    return eq_res


def get_eq_where(query):
    eq = []
    parsed = parse_one(query)
    eqTmp = parsed.find_all(exp.EQ)

    # if "company_name.id" in list(eqTmp):
    for i in eqTmp:
        eq.append(str(i))

    return eq

def pretty_sql(sql):
    return transpile(sql, pretty=True)[0]

def load_file(file_path):
    f = open(file_path)
    content = f.read()
    f.close()
    return content

def save_sql_to_file(sql_str, file_path):
    """
    Save a SQL string to a .sql file.

    Args:
    - sql_str (str): The SQL string to be saved.
    - file_path (str): The path to the file where the SQL string will be saved.

    Returns:
    None
    """
    with open(file_path, 'w') as file:
        file.write(sql_str)

def isSqlSame(sql1,sql2):

    sql1 = transpile(sql1)[0]
    sql2 = transpile(sql2)[0]

    return sql1 == sql2






def are_conditions_equivalent(cond1, cond2):
    def simplify_condition(condition):

        condition = condition.strip().replace('(', '').replace(')', '').strip()
        parts = condition.split('=')

        return [part.strip() for part in parts]

    if ('=' in cond1) != ('=' in cond2): # 一个有= 一个没有=
        return 0

    if ('=' in cond1) and ('=' in cond2):

        simplified_cond1 = simplify_condition(cond1)
        simplified_cond2 = simplify_condition(cond2)

        return set(simplified_cond1) == set(simplified_cond2)

    if ('=' not in cond1) and ('=' not in cond2):

        cond1 = remove_brackets_and_spaces(cond1)
        cond2 = remove_brackets_and_spaces(cond2)

        return cond1 == cond2

def getSqlType(path, sqls, sameTabType, betweenType ,orType):
    # 判断sql特殊类型
    for q in sqls:
        # path = "/home/xgz/chenzheng/demo_code_for_PG_test_MV_effect/IMDB-JOB113/q"

        sql = load_file(os.path.join(path, q))
        tabLst = get_tables(sql)
        if "BETWEEN" in sql:
            betweenType.add(q)
        if len(tabLst) != len(set(tabLst)):
            sameTabType.add(q)
        if "OR" in sql:
            orType.add(q)
    return


# def getMvType(qvmap, sameTabType, betweenType ,orType):
#     # 判断sql特殊类型
#     for i in qvmap:
#         v = i.split("\t")[1]
#         path = "/home/xgz/chenzheng/demo_code_for_PG_test_MV_effect/IMDB-scale/"
#         f = "mv" + v + ".sql"
#         sql = load_file(os.path.join(path, f))
#         tabLst = get_tables(sql)
#
#         if "BETWEEN" in sql:
#             betweenType.add(v)
#         if len(tabLst) != len(set(tabLst)):
#             sameTabType.add(v)
#         if "OR" in sql:
#             orType.add(v)
#
#     return

def simplify_relations(relations):
    simplified = []

    for pair in relations:
        found = None
        for s in simplified:
            if pair[0] in s or pair[1] in s:
                found = s
                break

        if found:
            found.update(pair)
        else:
            simplified.append(set(pair))

    # 以上代码可能会导致一些集合有重叠，以下代码确保它们合并。
    i = 0
    while i < len(simplified):
        j = i + 1
        while j < len(simplified):
            if simplified[i] & simplified[j]:
                simplified[i].update(simplified[j])
                simplified.pop(j)
            else:
                j += 1
        i += 1

    return [list(s) for s in simplified]


def start3(sql,mv):
    # 直接在sql_n上修改
    sql = transpile(sql,pretty = True)[0]
    mv = transpile(mv,pretty = True)[0]
    sqln = sql
    mvn = mv
    print("------------------------------------------------------")

    # 得到mv的名字
    mv_name = getMvName(mvn)

    # 得到mv的所有表(除了mv本身)
    mv_tables = set(get_tables(mvn))
    mv_tables.remove(mv_name)
    # print("当前mv的所有表：",mv_tables)

    # 得到sql的所有表和别名
    ala_tab_sql,tab_ala_sql= get_tables_alias(sql)
    ala_col_v,col_ala_v= get_column_alias(mv)

    sqlColumns = get_columns(sql)
    print("alias_tables >>>>>>>>>> ",ala_tab_sql)
    print("tables_alias >>>>>>>>>> ",tab_ala_sql)

    print("sqlColumns >>>>>>>>>> ",sqlColumns)

    for ala, tab in ala_tab_sql.items():
        sqln = sqln.replace(" " + ala+".", f" {tab}.")
        sqln = sqln.replace("MIN(" + ala+".", f"MIN({tab}.")
    # print("11 >>>>>>>>>>>>> \n",sqln)

    # select中加mv的名字
    sqln = sqln.replace("\nWHERE", f", {mv_name} \nWHERE")
    for tab in mv_tables:
        sqln = sqln.replace(tab + " AS " + tab_ala_sql[tab] + ", ", "")

    # print("22 >>>>>>>>>>>>> \n",sqln)

    sql_wheres = get_wheres(sqln)[0]
    mv_wheres = get_wheres(mvn)[0]

    # 替换sqln中的where为""
    sqln = transpile(sqln)[0]
    for mvw in mv_wheres:
        for sqlw in sql_wheres:
            if are_conditions_equivalent(mvw,sqlw):
                sqln = sqln.replace(sqlw.strip(),"")

    def clean_sql_advanced(sql):
        while re.search(r' AND\s+AND', sql):
            sql = re.sub(r' AND\s+AND', ' AND', sql)

        sql = re.sub(r'WHERE\s+AND', 'WHERE', sql)
        sql = re.sub(r'AND\s*$', '', sql)

        return sql

    sqln = clean_sql_advanced(sqln)
    sqln = transpile(sqln,pretty = True)[0]

    # print("33 >>>>>>>>>>>>> \n",sqln)


    eq = get_eq_where(mv)
    join_where = get_join_where(mv,eq)
    # print("所有eq",eq)
    # print("join的eq",join_where)

    eq_set = []
    for i in join_where:
        tmp = []
        tmp.append(i.split(" = ")[0])
        tmp.append(i.split(" = ")[1])
        eq_set.append(tmp)
    # print(eq_set)

    simplified_relations = simplify_relations(eq_set)
    # print(simplified_relations)
    # print(col_ala_v)

    result = dict(col_ala_v)

    for sub_list in simplified_relations:
        found_alias = None
        for item in sub_list:
            if item in col_ala_v:
                found_alias = col_ala_v[item]
                break

        for item in sub_list:
            result[item] = found_alias if found_alias else item

    # print(result)
    for col,ala in result.items():
        # sqln = sqln.replace(f" {col} ",f" {mv_name}."+ala+" ")
        sqln = sqln.replace(f" {col}",f" {mv_name}."+ala+"")
        sqln = sqln.replace(f"({col})",f"({mv_name}."+ala+")")

    # print(sqln)

    sql_wheres = get_wheres(sqln)[0]
    for w in sql_wheres:
        if contains_two_mv(mv_name,w):
            sqln = sqln.replace(w.strip(),"")
    sqln = clean_sql_advanced(sqln)
    # print(sqln)


    for tab, ala in tab_ala_sql.items():
        sqln = sqln.replace(" " + tab+".", f" {ala}.")
        sqln = sqln.replace("MIN(" + tab+".", f"MIN({ala}.")
    # print("11 >>>>>>>>>>>>> \n",sqln)
    if sqln.strip().endswith("WHERE"):
        sqln = sqln.strip()[:-5]
        sqln = transpile(sqln, pretty=True)[0]
        # print(sqln)
        return sqln


    sqln = transpile(sqln)[0]
    # print("66 >>>>>>>>>>>>> \n",sql_wheres)
    sql_wheres = get_wheres(sqln)[0]
    for sqlw in sql_wheres:
        if " = " in sqlw:
            sqlTmp = sqlw.strip().split(" = ")
            sqlTmp.sort()
            print(sqlTmp)
            sqln = sqln.replace(sqlw.strip(),sqlTmp[0]+" = "+sqlTmp[1])
    sqln = transpile(sqln,pretty = True)[0]

    # print(sqln)

    lines = sqln.strip().split("\n")  # 按换行符拆分字符串
    unique_lines = list(set(lines))  # 删除重复的行
    sorted_unique_lines = sorted(unique_lines, key=lines.index)  # 按原来的顺序对唯一的行进行排序
    sqln = "\n".join(sorted_unique_lines)
    # print(sqln)
    # print(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>")
    # print("rewrite")

    # print(transpile(sqln, pretty=True)[0])
    # #todo 最后可以再加一个条件去重例如1d7
    return transpile(sqln,pretty = True)[0]


def read_q_mv_csv_into_sqlr104(path):
    with open(path) as csv:
        pairs = csv.readlines()
        return pairs


def query_rewrite_old():
    # 获取文件夹下所有imdbSQL的文件名
    sql_path = "resources/PG/sql/"
    mv_path = "resources/PG/mv/topmv/"
    q_mv_path = "resources/PG/q_mv/tmp_q_mv_sql"
    q_mv_csv_path = "resources/data/tmp_query-mvdata.csv"

    os.makedirs(sql_path, exist_ok=True)
    os.makedirs(mv_path, exist_ok=True)
    os.makedirs(q_mv_path, exist_ok=True)

    sqls = os.listdir(sql_path)
    sameTabType = set()
    betweenType = set()
    orType = set()
    getSqlType(sql_path, sqls, sameTabType, betweenType, orType)
    print("sameTabType", len(sameTabType), sameTabType)
    print("betweenType", len(betweenType), betweenType)
    print("orType", len(orType), orType)
    allDelSql = sameTabType | betweenType | orType
    remainSql = set(sqls) - allDelSql
    # print("删除特殊类型后还剩下的条数：", remainSql)

    # todo 上面删除了特殊类型sql，下一步再基于remainSql生成候选视图。
    n = 0

    sqlr104 = read_q_mv_csv_into_sqlr104(q_mv_csv_path)

    for i in sqlr104:
        i = i.strip()
        q = i.split(",")[0]
        v = i.split(",")[1]

        # if (q in sameTabType) or (q in betweenType) or (q in orType):
        if q + ".sql" in (sameTabType | betweenType | orType):
            continue
        n += 1
        print(f"start rewrite {n} sql.")

        f = q + ".sql"
        sql = load_file(os.path.join(sql_path, f))
        cur_mv_path = os.path.join(mv_path, 'mv' + v + ".sql")
        mv = load_file(cur_mv_path)
        # print(f, v)
        print(f"rewrite {f} and mv{v}.")

        res = start3(sql, mv)
        q_mv_save_path = q_mv_path + f"/{q}_{v}.sql"
        save_sql_to_file(res, q_mv_save_path)

        print(f"rewrite {f} with mv{v} end.")

        time.sleep(0.5)


if __name__ == '__main__':
    query_rewrite_old()



