#!/usr/bin/env python
# -*- coding: utf-8 -*-
import copy
import os
import re
import json
import sys
import pandas as pd
import subprocess
from configparser import ConfigParser
import time
from func_timeout import func_set_timeout
import func_timeout

def run_task(task):
    tasks = ["get_query_time","create_view","remove_view","evaluate"]
    if task not in tasks:
        raise Exception(f"task {task} is not supported, only following tasks are supported: {tasks}")

    if task == "get_query_time":
        run_get_query_time()
    elif task == "create_view":
        create_view()
    elif task == "remove_view":
        remove_view()
    elif task == "evaluate":
        test_report()
        # pass

def run_get_query_time():
    flag = sys.argv[2]
    get_running_time(flag)

def get_running_time(flag):
    '''
获取查询时间和日志，保存到文件中
    :param flag:
    :return:
    '''
    if flag == "1":
        sql_path = cfg.get("file","imdb_sql")
    elif flag == "2":
        sql_path = cfg.get("file","imdb_sql_rewrite")

    sql_running_time_result = {}
    sql_query_id = {}
    succ = []
    fail = []
    for root, dirs, files in os.walk(sql_path):
        for n,f in enumerate(files):
            print("正在查询>>>",f)
            sql_file = os.path.join(root, f)
            sh_query = f"clickhouse client " \
                       f"-h {get_CH_IP()} " \
                       f"-u {get_CH_USER()} " \
                       f"--password {get_CH_PASSWD()} " \
                       f"--send_logs_level=trace " \
                       f"--database {database} " \
                       f"-t " \
                       f"--multiquery  < {sql_file}"#--send_logs_level=trace 从日志中取到query_id
    # f"--port 9000 " \
            print("running cmd:",sh_query)
            retcode, output = subprocess.getstatusoutput(sh_query)
            if retcode == 0:
                try:
                    time_taken = re.findall(r"[-+]?\d*\.\d+|\d+", output)[-1]
                    re_query_id = r' ] {(.*?)} <'
                    query_id = re.findall(re_query_id, output)[0]
                    sql_running_time_result[f] = float(time_taken)
                    sql_query_id[f] = query_id
                    print(f, "success", time_taken)
                    succ.append(f)
                except Exception as e:
                    fail.append(f)
                    print(e)
                    print("解析日志失败。>>", f)
            else:
                fail.append(f)
                print("查询失败。>>", f)
                # print(output)
                print(f, "Command execution failed", n )
    time.sleep(10)
    sql_query_log_map = {}
    for key,val in sql_query_id.items():
        sql_query_log_map[key] = get_query_log_by_query_id(val)
    save_as_json(f"./result/imdb_query_time_{flag}.json",sql_running_time_result)
    save_as_json(f"./result/imdb_query_log_{flag}.json",sql_query_log_map)
    print(f"查询结束。成功{len(succ)}条，失败{n-len(succ)+1}条。")
    # return sql_running_time_result

def create_view():
    create_view_time_result = {}
    succ = []
    fail = []
    sql_query_id ={}
    view_size = {}
    mv_path = cfg.get("file","topmv_imdb")
    database = cfg.get("database","database")
    for root, dirs, files in os.walk(mv_path):
        for f in files:
            print("正在创建视图>>", f)
            sql_file_create_view = os.path.join(root, f)
            sh_create = "clickhouse client " \
                        f"-h {get_CH_IP()} " \
                        f"-u {get_CH_USER()} " \
                        f"--password {get_CH_PASSWD()} " \
                        "--port 9000 " \
                        f"--send_logs_level=trace " \
                        f"--database {database} " \
                        f"-t --multiquery  < {sql_file_create_view}"
            print("cmd:",sh_create)
            # try:
            retcode, output = run_create_cmd(sh_create)
            # except func_timeout.exceptions.FunctionTimedOut as e:
            #     print(e)
            #     print(f,">> Time out!")
            #     continue
            if retcode == 0:
                try:
                    time_taken = re.findall(r"[-+]?\d*\.\d+|\d+", output)[-1]
                    re_query_id = r' ] {(.*?)} <'
                    query_id = re.findall(re_query_id, output)[0]
                    create_view_time_result[f] = float(time_taken)
                    print(f, "create success! Time is >>",time_taken)
                    view_size[f] = get_size_of_table_CH(database,f[:-4])
                    succ.append(f)
                    sql_query_id[f] = query_id
                except Exception as e:
                    print(e)
                    print("解析<创建视图>日志失败。>>", f)
            else:
                # print(output)
                print(f, "Command execution failed" )
                fail.append(f)
    time.sleep(10)
    sql_query_log_map = {}
    for key,val in sql_query_id.items():
        sql_query_log_map[key] = get_query_log_by_query_id(val)
    save_as_json("./result/imdb_create_log.json",sql_query_log_map)
    save_as_json("./result/imdb_mv_size.json",view_size)
    print(f"TOP视图创建结束。成功{len(succ)}个，失败{len(fail)}个。")
    print(f"成功的视图有：{succ}")
    print(f"失败的视图有：{fail}")
    save_as_json(f"./result/imdb_mv_time.json",create_view_time_result)
    return create_view_time_result

# @func_set_timeout(60)
def run_create_cmd(cmd):
    retcode, output = subprocess.getstatusoutput(cmd)
    return retcode, output

def run_cmd(args_list):
    """
    run linux commands
    """
    # print('Running system command: {0}'.format(' '.join(args_list)))
    proc = subprocess.Popen(args_list, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    s_output, s_err = proc.communicate()
    s_return = proc.returncode
    s_output = s_output.decode('utf-8').split('\n')
    return s_return, s_output, s_err

# def get_query_log_by_query_id(query_id):
#     args_list = ["clickhouse client", "--port", "9000", "--query", f"select query_duration_ms, read_rows, read_bytes, result_rows, result_bytes, memory_usage from system.query_log where query_id ='{query_id}';"]
#     (ret, out, err) = run_cmd(args_list)
#     if ret == 0:
#         info = out[1].split('\t')
#     else:
#         info = None
#     return info

def get_query_log_by_query_id(query_id):
    sql_content = f"select query_duration_ms, read_rows, read_bytes, result_rows, result_bytes, memory_usage from system.query_log where query_id ='{query_id}';"
    sh_query = f"clickhouse client " \
               f"-h {get_CH_IP()} " \
               f"-u {get_CH_USER()} " \
               f"--password {get_CH_PASSWD()} " \
               f"--port 9000 " \
               f"-t " \
               f"--multiquery --query \"{sql_content}\""
    retcode, output = subprocess.getstatusoutput(sh_query)
    # print(output)
    s_output = output.split('\n')
    info = s_output[1].split('\t')
    return info

def remove_view():
    database = cfg.get("database","database")
    for n in range(500):
        mv = "mv"+str(n)
        sh_remove = "clickhouse client " \
                    f"-h {get_CH_IP()} " \
                    f"-u {get_CH_USER()} " \
                    f"--password {get_CH_PASSWD()} " \
                    f"-d {database} " \
                    f"--query \"drop table {mv};\""
        retcode, output = subprocess.getstatusoutput(sh_remove)
        if retcode == 0:
            print(mv, "remove success .")
        elif retcode == 60:
            print(mv, "view does not exist.")
        else:
            print(mv, "remove fail.")
            print(output)
    print("历史视图删除完成。")

# def average_yield(t1, t2):
#     '''
#     平均benefit_rate计算
#     :return:
#     '''
#     time_profit = 0
#     for sql, tkn in t2.items():
#         if "-" in sql:
#             sql_name = sql.split("-")[0]+".sql"
#         elif "_" in sql:
#             sql_name = sql.split("_")[0]+".sql"
#         time_profit += (t1[sql_name] - tkn )
#     sum_time_taken2 = sum(t2.values())
#     # pos = [(t1[i] - t2[i]) for i in t2.keys() if (t1[i] - t2[i]) > 0]
#     # neg = [(t1[i] - t2[i]) for i in t2.keys() if (t1[i] - t2[i]) <= 0]
#     return round(time_profit / sum_time_taken2, 4)#,len(pos),len(neg)

def test_report():
    sql_path = cfg.get("file","imdb_sql")#imdb_sql_rewrite
    sql_rewrite_path = cfg.get("file","imdb_sql_rewrite")#imdb_sql_rewrite
    mv_path = cfg.get("file","topmv_imdb")
    database = cfg.get("database","database")
    # 读取sql查询时间
    time_taken1 = load_json("./result/imdb_query_time_1.json")  # 第1次查询时间
    time_taken2 = load_json("./result/imdb_query_time_2.json")  # 第2次查询时间
    mv_time = load_json("./result/imdb_mv_time.json")  # 视图创建时间

    cost1 = load_json("./result/imdb_query_log_1.json")
    cost2 = load_json("./result/imdb_query_log_2.json")
    costmv = load_json("./result/imdb_create_log.json")

    # 计算算子个数
    operator_num  = {}
    for root, dirs, files in os.walk(sql_path):
        for f in files:
            sql = load_file(os.path.join(root, f))
            operator_num[f] = get_number_of_operators_CH(database,sql)

    mv_operator_num  = {}
    for root, dirs, files in os.walk(mv_path):
        for f in files:
            # print(os.path.join(root, f))
            sql = load_file(os.path.join(root, f))
            mv_operator_num[f] = get_number_of_operators_CH(database,sql)

    operator_num_rw  = {}
    for root, dirs, files in os.walk(sql_rewrite_path):
        for f in files:
            sql = load_file(os.path.join(root, f))
            operator_num_rw[f] = get_number_of_operators_CH(database,sql)

    df = pd.DataFrame(columns=["original_sql_id","original_execution_time","rewrite_sql_id","rewrite_mv_id","rewrite_execution_time","benefit_time","benefit_rate"])
    for key,val in time_taken2.items():
        if "-" in key:
            sqlID = key.split("-")[0]+".sql"
            mvID = "mv"+key.split("-")[1][:-4]
        elif "_" in key:
            sqlID = key.split("_")[0]+".sql"
            mvID = "mv"+key.split("_")[1][:-4]
        s = {
            "original_sql_id": sqlID,
            "original_execution_time": time_taken1[sqlID],
            "rewrite_sql_id": key,
            "rewrite_mv_id": mvID,
            "rewrite_execution_time": val,
            "benefit_time": (time_taken1[sqlID] - val),
            "benefit_rate": '%.2f%%' % (round((time_taken1[sqlID] - val) / val, 4) * 100)
        }
        df = df.append(s, ignore_index=True)
    df.to_excel("./result/时间性能测试结果_imdb.xlsx",encoding="utf-8")

    df_ = pd.DataFrame(columns=["original_sql_id","query_duration_ms", "read_rows", "read_bytes", "result_rows",
                                "result_bytes", "memory_usage","原始执行成本","rewrite_mv_id","视图创建成本","rewrite_sql_id","query_duration_ms_rewrite",
                                "read_rows_rewrite", "read_bytes_rewrite", "result_rows_rewrite",
                                "result_bytes_rewrite", "memory_usage_rewrite","改写后执行成本"])
    for key,val in cost2.items():
        if "-" in key:
            sqlID = key.split("-")[0]+".sql"
            mvID = "mv"+key.split("-")[1][:-4]
        elif "_" in key:
            sqlID = key.split("_")[0]+".sql"
            mvID = "mv"+key.split("_")[1][:-4]
        s = {
            "original_sql_id":sqlID,
            "query_duration_ms":cost1[sqlID][0],
            "read_rows":cost1[sqlID][1],
            "read_bytes": cost1[sqlID][2],
            "result_rows":cost1[sqlID][3],
            "result_bytes": cost1[sqlID][4],
            "memory_usage": cost1[sqlID][5],
            "原始执行成本":get_cost_by_query_log(cost1[sqlID],operator_num[sqlID]),
            "rewrite_mv_id": mvID,
            "视图创建成本": get_cost_by_query_log(costmv[mvID+".sql"], mv_operator_num[mvID+".sql"],mvID+".sql"),
            "rewrite_sql_id": key,
            "query_duration_ms_rewrite":val[0],
            "read_rows_rewrite":val[1],
            "read_bytes_rewrite":val[2],
            "result_rows_rewrite":val[3],
            "result_bytes_rewrite":val[4],
            "memory_usage_rewrite":val[5],
            "改写后执行成本":get_cost_by_query_log(val,operator_num_rw[key])
        }
        df_ = df_.append(s, ignore_index=True)

    # df_.to_excel("./result/执行成本测试结果_imdb.xlsx",encoding="utf-8")

    t2 = copy.deepcopy(time_taken2)
    for i in time_taken1.keys():
        tmp = [i.split("_")[0]+".sql" for i in list(time_taken2.keys())]
        if i not in tmp:
            t2[i] = time_taken1[i]
    # 获取改写sql平均benefit_rate
    # time_profit_rate = average_yield(time_taken1, t2,mv_time)
    print("第一次总时间：",sum(time_taken1.values()))
    print("第二次总时间：",sum(t2.values()))
    print("视图创建时间：",sum(mv_time.values()))
    print("平均benefit_rate为：",(sum(time_taken1.values()) - sum(t2.values()) - sum(mv_time.values()))/sum(t2.values()))


def save_as_txt(name,string):
    with open('{}.txt'.format(name),'w') as f:
       f.write(string)

def save_as_json(file_name,data):
    with open(file_name, 'w',encoding="utf-8") as f:
        json.dump(data, f,ensure_ascii=False)

def load_file(file_path):
    f = open(file_path)
    content = f.read()
    f.close()
    return content

def get_sqlstr(sql_file):
    sql = open(sql_file, 'r', encoding='utf8')
    sqltxt = sql.readlines()
    sql.close()
    sql = "".join(sqltxt)
    return sql

def get_number_of_operators_CH(database, sql):
    if "SELECT" in sql:
        explain_sql = "explain " + sql[sql.rfind('SELECT'):]
    elif "select" in sql:
        explain_sql = "explain " + sql[sql.rfind('select'):]
    sh_exec = f"clickhouse client -h {get_CH_IP()} -u {get_CH_USER()} --password {get_CH_PASSWD()} -d {database} --query \"{explain_sql}\""
    retcode, output = subprocess.getstatusoutput(sh_exec)
    # print(retcode)
    if retcode == 0:
        count = output.count("\n")
        return count + 1
    else:
        # print('Running system command: {0}'.format(sh_exec))
        # print(output)
        # print(sql)
        return None

def get_cost_by_query_log(ql, operator,mvid=""):
    if mvid == "":
        read_bytes = ql[2]
        read_rows = ql[1]
        memory_usage = ql[5]
        cpu_cost = float(read_bytes) / 4096 + float(read_rows) * 0.01 + float(operator) * 0.0025
        cost = cpu_cost * 0.1 + float(memory_usage) * 0.001
        return cost
    elif mvid :
        mv_size = load_json("./result/imdb_mv_size.json")
        read_bytes = ql[2]
        read_rows = ql[1]
        memory_usage = ql[5]
        cpu_cost = float(read_bytes) / 4096 + float(read_rows) * 0.01 + float(operator) * 0.0025
        cost = cpu_cost * 0.1 + float(memory_usage) * 0.001+ 0.000167* float(mv_size[mvid])
        return cost

def load_json(file):
    with open(file, "r") as f:
        data = json.load(f)
    return data

def get_size_of_table_CH(database, table):
    '''
    格式化输出sql，表，未压缩，压缩，压缩率：
    SELECT table,
           formatReadableSize(sum(data_compressed_bytes)) AS tc,
           formatReadableSize(sum(data_uncompressed_bytes)) AS tu,
           round((sum(data_compressed_bytes) / sum(data_uncompressed_bytes)) * 100,2) AS ratio
      FROM system.columns
     WHERE database = 'ssb_test'
       and table = 'customer'
     GROUP BY table
     ORDER BY sum(data_compressed_bytes) ASC

    当前函数使用的：
    SELECT sum(data_compressed_bytes) AS tc FROM system.columns WHERE database = 'ssb_test' and table = 'customer'
    :return:
    '''

    sql_content = f"SELECT sum(data_compressed_bytes) AS tc FROM system.columns WHERE database = '{database}' and table = '{table}';"
    sh_query = f"clickhouse client " \
               f"-h {get_CH_IP()} " \
               f"-u {get_CH_USER()} " \
               f"--password {get_CH_PASSWD()} " \
               f"--port 9000 " \
               f"-t " \
               f"--multiquery --query \"{sql_content}\""
    retcode, output = subprocess.getstatusoutput(sh_query)
    if retcode == 0:
        mv_table_size = int(output[0])
    else:
        print(output)
        mv_table_size = None
    return mv_table_size

def get_CH_IP():
    return cfg.get("module_test", "CH_IP")

def get_CH_USER():
    return cfg.get("module_test", "CH_USER")

def get_CH_PASSWD():
    return cfg.get("module_test", "CH_PASSWD")
if __name__ == '__main__':

    ini_file = "conf.ini"
    cfg = ConfigParser()
    cfg.read(ini_file)
    database = cfg.get("database","database")

    task = sys.argv[1]

    run_task(task)
