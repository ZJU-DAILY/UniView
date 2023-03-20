import os, re, subprocess
from procConf import *
from procSQL import *


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


# 初始化json
def normalize_json(file_name):
    new_json_file = ""
    with open(file_name, 'r', encoding="utf-8") as f:
        lines = f.readlines()
        for line in lines:
            if line.find("QUERY PLAN") != -1:
                continue
            if line == "\n":
                continue
            if line.find("-------------") != -1:
                continue
            if re.findall(r"\(.*row\)", line):
                continue
            line = re.sub(r"[ ]*\+", "", line)
            new_json_file += line
    return new_json_file


def get_json_file_PG(query_path = "resources/PG/q_mv"):
    sql_path = query_path + "/sql"
    database = getDatabase()
    n = 0
    succ = 0
    os.makedirs(query_path + "/json", exist_ok=True)
    for root, dirs, files in os.walk(sql_path):
        for f in files:
            n += 1
            print("query>>>",f)
            sql_file = os.path.join(root, f)
            key, ext = os.path.splitext(os.path.basename(sql_file))
            # str
            sql = open(os.path.join(root, f)).read()
            sql = normalizeSql(sql)
            sql = "explain (format json) " + sql
            sh_command = "psql " \
                         "-p 5432 " \
                         f"-d {database} " \
                         f"-c \"{sql}\""
            try:
                retcode, output = subprocess.getstatusoutput(sh_command)
            except:
                print("error!")
            if retcode == 0:
                with open(query_path + "/json/" + key + ".json", 'w', encoding="utf-8") as f:
                    f.write(output)
                output = normalize_json(query_path + "/json/" + key + ".json")
                with open(query_path + "/json/" + key + ".json", 'w', encoding="utf-8") as f:
                    f.write(output)
                succ += 1
            else:
                print(f, "create fail", n)
                print(output)


def get_json_file_para_PG(input_path = "resources/PG/q_mv/sql", output_path="resources/PG/q_mv/json"):
    database = getDatabase()
    n = 0
    succ = 0
    os.makedirs(input_path, exist_ok=True)
    os.makedirs(output_path, exist_ok=True)
    for root, dirs, files in os.walk(input_path):
        for f in files:
            n += 1
            print("query>>>",f)
            sql_file = os.path.join(root, f)
            key, ext = os.path.splitext(os.path.basename(sql_file))
            # str
            sql = open(os.path.join(root, f)).read()
            sql = normalizeSql(sql)
            sql = "explain (format json) " + sql
            sh_command = "psql " \
                         "-p 5432 " \
                         f"-d {database} " \
                         f"-c \"{sql}\""
            try:
                retcode, output = subprocess.getstatusoutput(sh_command)
            except:
                print("error!")
            if retcode == 0:
                with open(output_path + "/" + key + ".json", 'w', encoding="utf-8") as f:
                    f.write(output)
                output = normalize_json(output_path + "/" + key + ".json")
                with open(output_path + "/" + key + ".json", 'w', encoding="utf-8") as f:
                    f.write(output)
                succ += 1
            else:
                print(f, "create fail", n)
                print(output)


if __name__ == "__main__":
    get_json_file_PG()