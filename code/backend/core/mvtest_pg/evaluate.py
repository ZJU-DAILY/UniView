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
    tasks = ["get_query_time","create_view","remove_view","evaluate","add_explain_analyze"]
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
    elif task == "add_explain_analyze":
        add_explain_analyze()

def get_running_time(flag):
    if flag == "1":
        sql_path = cfg.get("file","queries")
    elif flag == "2":
        sql_path = cfg.get("file","queries_rewrite")

    sql_running_time_result = {}
    sql_cost_result = {}
    succ = 0
    for root, dirs, files in os.walk(sql_path):
        for n,f in enumerate(files):
            print("正在查询>>>",f)
            sql_file = os.path.join(root, f)
            sh_command = "psql " \
                         "-p 5432 " \
                         f"-d {database} " \
                         f"-f {sql_file}"
            # print("running cmd:",sh_command)
            retcode, output = subprocess.getstatusoutput(sh_command)

            if retcode == 0:
                try:
                    re_ = r"Execution Time: (.*?) ms"
                    time_taken = float(re.findall(re_, output)[0])

                    exec_cost = get_cost_by_plan(output)

                    sql_running_time_result[f] = time_taken
                    sql_cost_result[f] = exec_cost
                    print(f, ">> success. time>>", time_taken, n)
                    succ += 1
                except:
                    print(f, ">> 解析输出失败或未命中视图.",n)
            else:
                print(retcode, output)
                print(f, "Command execution failed", n)

    save_as_json(f"./result/query_time_{flag}.json",sql_running_time_result)
    save_as_json(f"./result/query_cost_{flag}.json",sql_cost_result)
    print(f"查询结束。成功{succ}条，失败{n-succ+1}条。")
    # return sql_running_time_result

def create_view():
    create_view_time_result = {}
    mv_cost_result = {}
    succ = 0
    fail = 0
    mv_path = cfg.get("file","topmv")
    database = cfg.get("database","database")
    for root, dirs, files in os.walk(mv_path):
        for f in files:
            if "mv" in f:
                mv_name = f
            else:
                mv_name = "mv"+f

            sql_file_create_view = os.path.join(root, f)
            sh_create = "psql " \
                         "-p 5432 " \
                         f"-d {database} " \
                         f"-f {sql_file_create_view}"
            print(sh_create)
            retcode, output = subprocess.getstatusoutput(sh_create)
            print(retcode, output)
            if retcode == 0:
                # print(output,1111111)
                try:
                    re_ = r"Execution Time: (.*?) ms"
                    time_taken = float(re.findall(re_, output)[0])
                    create_view_time_result[f] = time_taken

                    mv_cost = get_cost_by_plan(output)
                    mv_cost_result[mv_name] = mv_cost
                    print(mv_name, "create success! Time>>",time_taken)
                    succ += 1
                except Exception as e:
                    print(output)
                    print(mv_name, "create fail.",e)
                    continue
            else:
                print(f, "create fail")
                fail += 1
    save_as_json("./result/mvCOST.json",mv_cost_result)
    save_as_json("./result/create_view_time.json",create_view_time_result)
    print(f"TOP视图创建结束。成功{succ}个，失败{fail}个。")
    # return create_view_time_result

def remove_view():
    database = cfg.get("database","database")
    for n in range(1000):
        mv = "mv"+str(n)
        # try：
        sh_remove = "psql -p 5432 " \
                    f"-d {database} " \
                    f"-c 'drop materialized view {mv};'"
        retcode, output = subprocess.getstatusoutput(sh_remove)
        if retcode == 0:
            print(mv, "remove success .")
        elif retcode == 1:
            print(mv, "view does not exist.")
        else:
            # print(retcode)
            print(mv, "remove fail.")
            print(output)
    print("历史视图删除完成。")

def add_explain_analyze():
    s = "EXPLAIN ANALYZE "
    def save_as_sql(name, string):
        with open('./topmv/{}'.format(name), 'w') as f:
            f.write(string)
    for root, dirs, files in os.walk("./topmv_original"):
        for f in files:
            str = s + \
                  "\n" + \
                  open(os.path.join(root, f)).read()
            save_as_sql(f, str)

def get_cost_by_plan(plan):
    '''
    从执行计划中获取cost
    :param plan:
    :return:
    '''
    re_ = "cost=(.*?) rows="
    total_cost= re.findall(re_, plan)[0]
    cost = float(total_cost.split("..")[1])
    return round(cost,4)

def average_yield(t1, t2):
    '''
    平均收益率计算
    :return:
    '''
    time_profit = 0

    for sql, tkn in t2.items():
        if "-" in sql:
            sql_name = sql.split("-")[0]+".sql"
        elif "_" in sql:
            sql_name = sql.split("_")[0]+".sql"
        time_profit += (t1[sql_name] - tkn )
    sum_time_taken2 = sum(t2.values())
    # pos = [(t1[i] - t2[i]) for i in t2.keys() if (t1[i] - t2[i]) > 0]
    # neg = [(t1[i] - t2[i]) for i in t2.keys() if (t1[i] - t2[i]) <= 0]
    return round(time_profit / sum_time_taken2, 4)#,len(pos),len(neg)

def test_report():
    # 读取sql查询时间
    time_taken1 = load_json("./result/query_time_1.json")  # 第1次查询时间
    time_taken2 = load_json("./result/query_time_2.json")  # 第2次查询时间
    mv_time = load_json("./result/create_view_time.json")  # 创建视图时间

    cost1 = load_json("./result/query_cost_1.json")  # 第1次查询时间
    cost2 = load_json("./result/query_cost_2.json")  # 第2次查询时间

    mvcost = load_json("./result/mvCOST.json")  # 第2次查询时间

    # 获取改写sql平均收益率
    time_profit_rate = average_yield(time_taken1, time_taken2)

    df = pd.DataFrame(columns=["原始sqlID","原始执行耗时(ms)","改写sqlID","改写视图ID","改写后执行耗时(ms)","收益时间(ms)","收益率"])
    for key,val in time_taken2.items():
        if "-" in key:
            sqlID = key.split("-")[0]+".sql"
            mvID = "mv"+key.split("-")[1][:-4]
        elif "_" in key:
            sqlID = key.split("_")[0]+".sql"
            mvID = "mv"+key.split("_")[1][:-4]
        s = {
            "原始sqlID":sqlID,
            "原始执行耗时(ms)":time_taken1[sqlID],
            "改写sqlID":key,
            "改写视图ID":mvID,
            "改写后执行耗时(ms)":val,
            "收益时间(ms)":(time_taken1[sqlID]-val),
            "收益率":'%.2f%%' % (round((time_taken1[sqlID]-val)/val,4) * 100)
        }
        df = df.append(s, ignore_index=True)
    df.to_excel("./result/时间性能测试结果.xlsx",encoding="utf-8")

    df_ = pd.DataFrame(columns=["原始sqlID","原始执行成本","改写sqlID","改写视图ID","视图创建开销","改写后执行成本"])
    for key,val in cost2.items():
        if "-" in key:
            sqlID = key.split("-")[0]+".sql"
            mvID = "mv"+key.split("-")[1][:-4]
        elif "_" in key:
            sqlID = key.split("_")[0]+".sql"
            mvID = "mv"+key.split("_")[1][:-4]
        s = {
            "原始sqlID":sqlID,
            "原始执行成本":cost1[sqlID],
            "改写sqlID":key,
            "改写视图ID": mvID,
            "视图创建开销":mvcost[mvID+".sql"],
            "改写后执行成本":val,
        }
        df_ = df_.append(s, ignore_index=True)
    # df_.to_excel("./result/执行成本测试结果.xlsx",encoding="utf-8")
    t2 = copy.deepcopy(time_taken2)
    for i in time_taken1.keys():
        tmp = [i.split("_")[0]+".sql" for i in list(time_taken2.keys())]
        if i not in tmp:
            t2[i] = time_taken1[i]
    # 获取改写sql平均收益率
    # time_profit_rate = average_yield(time_taken1, t2,mv_time)
    print("第一次总时间：",sum(time_taken1.values()))
    print("第二次总时间：",sum(t2.values()))
    print("视图创建时间：",sum(mv_time.values()))
    print("平均收益率为：",(sum(time_taken1.values()) - sum(t2.values()) - sum(mv_time.values()))/sum(t2.values()))

def save_as_txt(name,string):
    with open('{}.txt'.format(name),'w') as f:
       f.write(string)

def save_as_json(file_name,data):
    with open(file_name, 'w',encoding="utf-8") as f:
        json.dump(data, f,ensure_ascii=False)

def get_sqlstr(sql_file):
    sql = open(sql_file, 'r', encoding='utf8')
    sqltxt = sql.readlines()
    sql.close()
    sql = "".join(sqltxt)
    return sql

def load_json(file):
    with open(file, "r") as f:
        data = json.load(f)
    return data

if __name__ == '__main__':

    ini_file = "./conf.ini"
    cfg = ConfigParser()
    cfg.read(ini_file)
    database = cfg.get("database","database")


    task = sys.argv[1]

    run_task(task)
