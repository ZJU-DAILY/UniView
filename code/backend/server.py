#!/usr/bin/python3.8
# -*- coding: utf-8 -*-
# @Author  : xzr
# @Email   : xuzhenrong@zju.edu.cn
# @Software: PyCharm
# @Time    : 2023/3/5 16:26
# @File    : server.py.py
from flask import Flask, jsonify, request, make_response
from flask_apscheduler import APScheduler
import datetime, subprocess
from init import *
import re
from flask_cors import CORS, cross_origin


class Config(object):
    SCHEDULER_API_ENABLED = True


scheduler = APScheduler()

app = Flask(__name__)

cors = CORS(app)
# 使通过jsonify返回的中文显示正常，否则显示为ASCII码
app.config["JSON_AS_ASCII"] = False

'''
parameter configuration
'''


@app.route('/')
def index():
    return 'Hello World'


# check stage
@scheduler.task('interval', id='do_job_1', seconds=10, misfire_grace_time=900)
def job1():
    global system_start, system_stage, last_system_stage
    print(str(datetime.datetime.now()) + ' Job 1 executed')
    if not system_start:
        return
    # stage change
    if system_stage != last_system_stage:
        if system_stage == 1:
            subprocess.call(
                "cd /root/work_space/code/backend/core; nohup python -u main.py get_candidates PG > /root/work_space/code/backend/log/stage1_output.log 2>&1 &",
                shell=True)
        elif system_stage == 2:
            subprocess.call(
                "cd /root/work_space/code/backend/core; nohup python -u main.py build_data PG > /root/work_space/code/backend/log/stage2_output.log 2>&1 &",
                shell=True)
        elif system_stage == 3:
            subprocess.call(
                "cd /root/work_space/code/backend/core; nohup python -u main.py cost_estimation PG > /root/work_space/code/backend/log/stage3_output.log 2>&1 &",
                shell=True)
        elif system_stage == 4:
            subprocess.call(
                "cd /root/work_space/code/backend/core; nohup python -u main.py recommend PG > /root/work_space/code/backend/log/stage4_output.log 2>&1 &",
                shell=True)
        last_system_stage = system_stage
        print(str(datetime.datetime.now()) + f" stage change, current stage is {system_stage}!")
        return
    # check if stage end
    ret = subprocess.Popen(f"grep 'run success' log/stage{system_stage}_output.log", shell=True, stdout=subprocess.PIPE)
    out = ret.stdout.read().decode("utf-8")
    print(out)
    if out.strip() == "run success!":
        print(str(datetime.datetime.now()) + f" stage {system_stage} is over!")
        system_stage += 1
        return
    last_system_stage = system_stage


# get parameter
@app.route("/save_parameters", methods=['POST'])
def save_parameters():
    engine = request.json.get("engine")
    recommend_method = request.json.get("recommend_method")
    single_or_multiple = request.json.get("single_or_multiple")
    epoch = request.json.get("epoch")
    num_train = request.json.get("num_train")
    cnt_limit = request.json.get("cnt_limit")
    space_limit = request.json.get("space_limit")
    refer_threshold = request.json.get("refer_threshold")
    table_size_upper_bound = request.json.get("table_size_upper_bound")
    table_size_lower_bound = request.json.get("table_size_lower_bound")

    # check
    if not engine:
        return jsonify({"code": 401, "msg": "engine is None!"})
    if engine not in engine_set:
        return jsonify({"code": 401, "msg": "engine is unsupported!"})
    if not recommend_method:
        return jsonify({"code": 401, "msg": "recommend_method is None!"})
    if recommend_method not in recommend_method_set:
        return jsonify({"code": 401, "msg": "recommend_method is unsupported!"})
    if not single_or_multiple:
        return jsonify({"code": 401, "msg": "single_or_multiple is None!"})
    if recommend_method not in recommend_method_set:
        return jsonify({"code": 401, "msg": "single_or_multiple must be Single or Multiple!"})
    ok = check_valueisInt(epoch, "epoch")
    if ok:
        return jsonify({"code": 401, "msg": ok})
    ok = check_valueisInt(num_train, "num_train")
    if ok:
        return jsonify({"code": 401, "msg": ok})
    ok = check_valueisInt(cnt_limit, "cnt_limit")
    if ok:
        return jsonify({"code": 401, "msg": ok})
    ok = check_valueisInt(space_limit, "space_limit")
    if ok:
        return jsonify({"code": 401, "msg": ok})
    ok = check_valueisInt(refer_threshold, "refer_threshold")
    if ok:
        return jsonify({"code": 401, "msg": ok})
    ok = check_valueisInt(table_size_upper_bound, "table_size_upper_bound")
    if ok:
        return jsonify({"code": 401, "msg": ok})
    ok = check_valueisInt(table_size_lower_bound, "table_size_lower_bound")
    if ok:
        return jsonify({"code": 401, "msg": ok})

    response = make_response(jsonify({"code": 200, "msg": "save parameters success!"}))
    # # 设置响应请求头
    # response.headers["Access-Control-Allow-Origin"] = '*'  # 允许使用响应数据的域。也可以利用请求header中的host字段做一个过滤器。
    # response.headers["Access-Control-Allow-Methods"] = '*'  # 允许的请求方法
    # response.headers["Access-Control-Allow-Headers"] = "x-requested-with,content-type"  # 允许的请求header

    return response


# submit
@app.route("/submit", methods=["GET"])
def submit():
    global system_start, system_stage
    system_start = True
    system_stage = 1
    # ret = subprocess.Popen("ls -l", shell=True, stdout=subprocess.PIPE)
    # out = ret.stdout.read().decode("utf-8")
    # print(out)
    # print(type(out))
    print(str(datetime.datetime.now()) + ' submit success!')

    response = make_response(jsonify({"code": 200, "msg": "submit success!"}))
    # 设置响应请求头
    response.headers["Access-Control-Allow-Origin"] = '*'  # 允许使用响应数据的域。也可以利用请求header中的host字段做一个过滤器。
    response.headers["Access-Control-Allow-Methods"] = 'GET'  # 允许的请求方法
    response.headers["Access-Control-Allow-Headers"] = "x-requested-with,content-type"  # 允许的请求header

    return response


# get cpu usage
@app.route("/get_cpu_usage", methods=["GET"])
def get_cpu_usage():
    ret = subprocess.Popen("vmstat", shell=True, stdout=subprocess.PIPE)
    out = ret.stdout.read().decode("utf-8")
    # print(out)
    ans = out.split("\n")
    para_name, para = ans[1].strip(), ans[2].strip()
    para_list = para.split(" ")
    para_list = [para for para in para_list if para != '']
    cpu_usage = 100 - int(para_list[14])

    response = make_response(jsonify({"code": 200, "msg": "get_cpu_usage success!", "cpu_usage": cpu_usage}))
    # 设置响应请求头
    response.headers["Access-Control-Allow-Origin"] = '*'  # 允许使用响应数据的域。也可以利用请求header中的host字段做一个过滤器。
    response.headers["Access-Control-Allow-Methods"] = 'GET'  # 允许的请求方法
    response.headers["Access-Control-Allow-Headers"] = "x-requested-with,content-type"  # 允许的请求header

    return response


# get memory usage
@app.route("/get_memory_usage", methods=["GET"])
def get_memory_usage():
    ret = subprocess.Popen("free -m", shell=True, stdout=subprocess.PIPE)
    out = ret.stdout.read().decode("utf-8")
    print(out)
    ans = out.split("\n")[1]
    para_list = ans.split(" ")
    para_list = [para for para in para_list if para != '']
    # print(para_list)
    total, used = int(para_list[1]), int(para_list[2])
    memory_usage = int(used * 100 / total)

    response = make_response(jsonify({"code": 200, "msg": "get_memory_usage success!", "memory_usage": memory_usage}))
    # 设置响应请求头
    response.headers["Access-Control-Allow-Origin"] = '*'  # 允许使用响应数据的域。也可以利用请求header中的host字段做一个过滤器。
    response.headers["Access-Control-Allow-Methods"] = 'GET'  # 允许的请求方法
    response.headers["Access-Control-Allow-Headers"] = "x-requested-with,content-type"  # 允许的请求header

    return response


# get stage
@app.route("/get_stage", methods=["GET"])
def get_stage():
    global system_stage
    cur_stage = system_stage - 1 if system_stage > 2 else system_stage

    response = make_response(jsonify({"code": 200, "msg": "get_stage success!", "stage": cur_stage}))
    # 设置响应请求头
    response.headers["Access-Control-Allow-Origin"] = '*'  # 允许使用响应数据的域。也可以利用请求header中的host字段做一个过滤器。
    response.headers["Access-Control-Allow-Methods"] = 'GET'  # 允许的请求方法
    response.headers["Access-Control-Allow-Headers"] = "x-requested-with,content-type"  # 允许的请求header

    return response


# get log
@app.route("/get_log", methods=["GET"])
def get_log():
    global system_stage
    cur_stage = system_stage - 1 if system_stage > 2 else system_stage
    if system_stage == 3:
        path1 = f"/root/work_space/code/backend/log/stage2_output.log"
        path2 = f"/root/work_space/code/backend/log/stage3_output.log"
        ans_json1 = trans_file_to_log(path1)
        ans_json2 = trans_file_to_log(path2)
        return jsonify(ans_json1 + ans_json2)
    path = f"/root/work_space/code/backend/log/stage{system_stage}_output.log"
    ans_json = trans_file_to_log(path)
    # print(ans_json)
    print(f"the system stage: {system_stage}")

    response = make_response(jsonify(ans_json))
    # 设置响应请求头
    response.headers["Access-Control-Allow-Origin"] = '*'  # 允许使用响应数据的域。也可以利用请求header中的host字段做一个过滤器。
    response.headers["Access-Control-Allow-Methods"] = 'GET'  # 允许的请求方法
    response.headers["Access-Control-Allow-Headers"] = "x-requested-with,content-type"  # 允许的请求header

    return response


def trans_file_to_log(path):
    ans_json = []
    try:
        with open(path) as f:
            for line in f.readlines():
                one_ans = {"info": line.strip()}
                ans_json.append(one_ans)
    except:
        pass

    return ans_json


# Flask应用程序实例的 run 方法 启动 WEB 服务器
if __name__ == '__main__':
    app.config.from_object(Config())
    scheduler.init_app(app)
    scheduler.start()

    app.run(host="0.0.0.0", port=5000, debug=True)
