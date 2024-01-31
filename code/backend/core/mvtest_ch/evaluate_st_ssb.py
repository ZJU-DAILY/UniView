#!/usr/bin/env python
# -*- coding: utf-8 -*-
import os
import re
import json
import sys
import pandas as pd
import subprocess
import time
from configparser import ConfigParser
import copy

def run_task(task):
    tasks = ["get_query_time","create_view","remove_view","evaluate"]
    if task not in tasks:
        raise Exception(f"task {task} is not supported, only following tasks are supported: {tasks}")

    if task == "get_query_time":
        flag  = sys.argv[2]
        get_running_time(flag)
    elif task == "create_view":
        create_view()
    elif task == "remove_view":
        remove_view()
    elif task == "evaluate":
        test_report()

def is_rewrite_CH_st(database,sql):
    explain_sql = "explain " + sql
    sh_exec = f"clickhouse client -h {get_CH_IP()} -u {get_CH_USER()} --password {get_CH_PASSWD()} -d {database} --query \"{explain_sql}\""
    # print('Running system command: {0}'.format(sh_exec))
    retcode, output = subprocess.getstatusoutput(sh_exec)
    if retcode == 0:
        if "projection" in output:
            return True
    else:
        print(output)
        return None

def get_running_time(flag):
    sql_path = cfg.get("file","ssb_sql")
    database = cfg.get("database","database_st")
    tmp = "set allow_experimental_projection_optimization = 0;" if flag == "1" else ""
    rewrite_list = []
    for root, dirs, files in os.walk(sql_path):
        for f in files:
            if is_rewrite_CH_st(database, load_file(os.path.join(root, f))):
                rewrite_list.append(f)
    print("被rewrite_sql_id：",rewrite_list)
    save_as_json(f"./result/rewrite_list_ssb.json",rewrite_list)
    succ = 0; fail = 0
    sql_running_time_result = {}
    sql_query_id = {}
    for root, dirs, files in os.walk(sql_path):
        for f in files:
            print("正在查询>>>",f)
            sql_content = tmp+load_file(os.path.join(root, f))
            sh_query = f"clickhouse client " \
                       f"-h {get_CH_IP()} " \
                       f"-u {get_CH_USER()} " \
                       f"--password {get_CH_PASSWD()} " \
                       f"--send_logs_level=trace " \
                       f"--port 9000 " \
                       f"--database {database} " \
                       f"-t " \
                       f"--multiquery --query \"{sql_content}\""
            # print('Running system command: {0}'.format(sh_query))
            retcode, output = subprocess.getstatusoutput(sh_query)
            if retcode == 0:
                time_taken = re.findall(r"[-+]?\d*\.\d+|\d+", output)[-1]
                re_query_id = r' ] {(.*?)} <'
                query_id = re.findall(re_query_id, output)[-1]
                sql_running_time_result[f] = float(time_taken)
                sql_query_id[f] = query_id
                if f in rewrite_list:
                    print(f, "rewrite success",time_taken)
                    succ +=1
                else:
                    print(f, "rewrite fail",time_taken)
                    fail += 1
            else:
                print(f, "fail")
                print(output)
                fail += 1
    time.sleep(10)
    sql_query_log = {}
    for key,val in sql_query_id.items():
        sql_query_log[key] = get_query_log_by_query_id(val)
    save_as_json(f"./result/第{flag}次查询_ssb.json",sql_running_time_result)
    save_as_json(f"./result/第{flag}次查询日志_ssb.json",sql_query_log)
    print(f"查询结束。改写{succ}条，未改写{fail}条。")
    return sql_running_time_result

def create_view():
    create_view_time_result = {}
    succ = 0
    fail = 0
    mv_path = cfg.get("file","topmv_ssb")
    database = cfg.get("database","database_st")
    start_time = time.time()
    for root, dirs, files in os.walk(mv_path):
        for f in files:
            # try:
            sql_file_create_view = os.path.join(root, f)
            sh_create = "clickhouse client " \
                        f"-h {get_CH_IP()} " \
                        f"-u {get_CH_USER()} " \
                        f"--password {get_CH_PASSWD()} " \
                        "--port 9000 " \
                        f"--database {database} " \
                        f"-t --multiquery  < {sql_file_create_view}"
            retcode, output = subprocess.getstatusoutput(sh_create)
            if retcode == 0:
                print(f, "create success.")
                table_name = f.split("-")[0]
                projection_name = f.split("-")[1][:-4]
                sh = f"clickhouse client -h {get_CH_IP()} -u {get_CH_USER()} --password {get_CH_PASSWD()} -d {database} --query \"alter table {table_name} MATERIALIZE PROJECTION {projection_name}\""
                print(sh)
                retcode, output = subprocess.getstatusoutput(sh)
                if retcode == 0:
                    print("对历史数据进行物化。")
                    pass
                succ += 1
            else:
                print(f, "create fail")
                print(output)
                fail +=1
    # time.sleep(100)
    sql_content = "SELECT  is_done FROM system.mutations"
    sh_detect = "clickhouse client " \
                f"-h {get_CH_IP()} " \
                f"-u {get_CH_USER()} " \
                f"--password {get_CH_PASSWD()} " \
                "--port 9000 " \
                f"--multiquery --query \"{sql_content}\""
    print('Running system command: {0}'.format(sh_detect))

    # while True:
    #     retcode, output = subprocess.getstatusoutput(sh_detect)
    #     if retcode == 0:
    #         if "0" in output:
    #             print("正在等待物化完成...")
    #             time.sleep(0.1)
    #         else:
    #             print("物化完成。")
    #             break
    #     else:
    #         print(output)
    # end_time = time.time()
    # create_time = end_time - start_time
    # save_as_json(f"./result/create_time.json",create_time)
    print(f"TOP视图创建结束。成功{succ}个，失败{fail}个.")
    # print(f"TOP视图创建结束。成功{succ}个，失败{fail}个。视图创建及物化数据时间为>>",create_time)
    return create_view_time_result

def remove_view():
    database = cfg.get("database","database_st")
    mv_path = cfg.get("file","topmv_ssb")
    for root, dirs, files in os.walk(mv_path):
        for f in files:
            table_name = f.split("-")[0]
            projection_name = f.split("-")[1][:-4]
            sh_remove = f"clickhouse client -h {get_CH_IP()} -u {get_CH_USER()} --password {get_CH_PASSWD()} -d {database} --query \"ALTER TABLE {table_name} DROP PROJECTION {projection_name};\" "
            print("running cmd:",sh_remove)
            retcode, output = subprocess.getstatusoutput(sh_remove)
            if retcode == 0:
                print(table_name+"."+projection_name, "remove success.")
            # elif retcode == 60:
            #     print(table_name+"."+projection_name, "view does not exist.")
            else:
                print(table_name+"."+projection_name, "remove fail.")
                print(output)
    print("历史视图删除完成。")

def average_yield(t1, t2):
    '''
    平均benefit_rate计算
    :return:
    '''
    time_profit = 0

    for sql, tkn in t2.items():
        time_profit += (t1[sql] - tkn )
    sum_time_taken2 = sum(t2.values())
    # pos = [(t1[i] - t2[i]) for i in t2.keys() if (t1[i] - t2[i]) > 0]
    # neg = [(t1[i] - t2[i]) for i in t2.keys() if (t1[i] - t2[i]) <= 0]
    return round(time_profit / sum_time_taken2, 4)#,len(pos),len(neg)

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
    print(output)
    s_output = output.split('\n')
    info = s_output[1].split('\t')
    return info
# def get_query_log_by_query_id(query_id):
#     args_list = ["clickhouse-client", "--port", "9000", "--query", f"select query_duration_ms, read_rows, read_bytes, result_rows, result_bytes, memory_usage from system.query_log where query_id ='{query_id}';"]
#     (ret, out, err) = run_cmd(args_list)
#     if ret == 0:
#         info = out[1].split('\t')
#     else:
#         info = None
#     return info

def test_report():
    sql_path = cfg.get("file","ssb_sql")
    database = cfg.get("database","database_st")
    # 读取sql查询时间
    time_taken1 = load_json("./result/第1次查询_ssb.json")  # 第1次查询时间
    time_taken2 = load_json("./result/第2次查询_ssb.json")  # 第2次查询时间
    rewrite_list = load_json("./result/rewrite_list_ssb.json")

    cost1 = load_json("./result/第1次查询日志_ssb.json")
    cost2 = load_json("./result/第2次查询日志_ssb.json")

    #计算算子个数
    operator_num  = {}
    for root, dirs, files in os.walk(sql_path):
        for f in files:
            sql = load_file(os.path.join(root, f))
            operator_num[f] = get_number_of_operators_CH(database,sql)
    # print("operator_num",operator_num)


    # 获取改写sql平均benefit_rate
    time_profit_rate = average_yield(time_taken1, time_taken2)

    df = pd.DataFrame(columns=["original_sql_id","original_execution_time","rewrite_sql_id","rewrite_execution_time","benefit_time","benefit_rate"])
    for key,val in time_taken2.items():
        if key in rewrite_list:
            s = {
                "original_sql_id":key,
                "original_execution_time":time_taken1[key],
                "rewrite_sql_id":key,
                "rewrite_execution_time":val,
                "benefit_time":(time_taken1[key]-val),
                "benefit_rate":'%.2f%%' % (round((time_taken1[key]-val)/val,4) * 100)
            }
            df = df.append(s, ignore_index=True)
    df.to_excel("./result/时间性能测试结果_ssb.xlsx",encoding="utf-8")

    df_ = pd.DataFrame(columns=["original_sql_id","query_duration_ms", "read_rows", "read_bytes", "result_rows",
                                "result_bytes", "memory_usage","原始执行成本","rewrite_sql_id","query_duration_ms_rewrite",
                                "read_rows_rewrite", "read_bytes_rewrite", "result_rows_rewrite",
                                "result_bytes_rewrite", "memory_usage_rewrite","改写后执行成本"])
    for key,val in cost2.items():
        s = {
            "original_sql_id":key,
            "query_duration_ms":cost1[key][0],
            "read_rows":cost1[key][1],
            "read_bytes": cost1[key][2],
            "result_rows":cost1[key][3],
            "result_bytes": cost1[key][4],
            "memory_usage": cost1[key][5],
            "原始执行成本":get_cost_by_query_log(cost1[key],operator_num[key]),
            "rewrite_sql_id": key,
            "query_duration_ms_rewrite":val[0],
            "read_rows_rewrite":val[1],
            "read_bytes_rewrite":val[2],
            "result_rows_rewrite":val[3],
            "result_bytes_rewrite":val[4],
            "memory_usage_rewrite":val[5],
            "改写后执行成本":get_cost_by_query_log(val,operator_num[key])
        }
        df_ = df_.append(s, ignore_index=True)
    # df_.to_excel("./result/执行成本测试结果_ssb.xlsx",encoding="utf-8")


    # 获取改写sql平均benefit_rate
    # time_profit_rate = average_yield(time_taken1, t2,mv_time)
    print("第一次总时间：",sum(time_taken1.values()))
    print("第二次总时间：",sum(time_taken2.values()))
    # print("视图创建时间：",sum(mv_time.values())) #
    print("平均benefit_rate为：",(sum(time_taken1.values()) - sum(time_taken2.values()))/sum(time_taken2.values()))
    # print("平均benefit_rate为：",time_profit_rate)

def save_as_txt(name,string):
    with open('{}.txt'.format(name),'w') as f:
       f.write(string)

def save_as_json(file_name,data):
    with open(file_name, 'w',encoding="utf-8") as f:
        json.dump(data, f,ensure_ascii=False)

def get_cost_by_query_log(ql, operator):
    read_bytes = ql[2]
    read_rows = ql[1]
    memory_usage = ql[5]
    cpu_cost = float(read_bytes) / 4096 + float(read_rows) * 0.01 + float(operator) * 0.0025
    cost = cpu_cost * 0.1 + float(memory_usage) * 0.001
    return cost

def get_number_of_operators_CH(database, sql):
    explain_sql = "explain " + sql
    sh_exec = f"clickhouse client -h {get_CH_IP()} -u {get_CH_USER()} --password {get_CH_PASSWD()} -d {database} --query \"{explain_sql}\""
    # print('Running system command: {0}'.format(sh_exec))
    retcode, output = subprocess.getstatusoutput(sh_exec)
    if retcode == 0:
        count = output.count("\n")
        return count + 1
    else:
        print(output)
        return None

def get_sqlstr(sql_file):
    sql = open(sql_file, 'r', encoding='utf8')
    sqltxt = sql.readlines()
    sql.close()
    sql = "".join(sqltxt)
    return sql

def load_file(file_path):
    f = open(file_path)
    content = f.read()
    f.close()
    return content

def load_json(file):
    with open(file, "r") as f:
        data = json.load(f)
    return data

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

    task = sys.argv[1]

    run_task(task)
