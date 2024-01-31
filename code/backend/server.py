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
from collections import defaultdict
from init import *
import re, os, json
import logging
from flask_cors import CORS, cross_origin
from sql_parser import *


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


code_path = '/home/xgz/xzr/code/mul_demo'
evaluate_result_path = f"{code_path}/backend/core/resources/result/JOB113_PG.json"
mv_history_current_path = f"{code_path}/backend/core/resources/result/MV_history_PG_v2.json"
mv_history_former_path = f"{code_path}/backend/core/resources/result/MV_history_v2.json"


os.makedirs(f"{code_path}/backend/log", exist_ok=True)

sql_parser=Sql_parser()

# 获取所有mv的信息
def get_candidate_mv_json():
    mv_json = {}
    mv_dir_path = f"{code_path}/backend/core/resources/PG/mv/mv_original/"
    for root, dirs, files in os.walk(mv_dir_path):
        for file in files:
            file_path = os.path.join(root, file)
            file_name = file[:-4]
            with open(file_path, 'r') as f:
                file_content = f.read()
            file_content = sql_parser.format(file_content)
            mv_json[file_name] = file_content

    return mv_json


# compute topn coverage
def compute_topn_coverage(json_dict, topn):
    ans_json = defaultdict(int)

    for item in json_dict:
        mv_id = item['rewrite_mv_id']
        ans_json[mv_id] += 1

    ans_list = sorted(ans_json.items(), key=lambda x: x[1], reverse=True)
    ans_list = ans_list[:topn]

    return dict(ans_list)


# check stage
@scheduler.task('interval', id='do_job_1', seconds=10, misfire_grace_time=900)
def job1():
    global system_start, system_stage, last_system_stage, log_msg
    logging.info(str(datetime.datetime.now()) + ' Job 1 executed')
    if not system_start:
        # after generate mv
        if system_stage == 1:
            # check if stage end
            ret = subprocess.Popen(f"grep 'run success' log/stage{system_stage}_output.log", shell=True,
                                   stdout=subprocess.PIPE)
            out = ret.stdout.read().decode("utf-8")

            if out.strip() == "run success!":
                print(str(datetime.datetime.now()) + f" stage {system_stage} is over!")
                # log some mv msg
                # mv_json = get_candidate_mv_json()
                log_msg = 'generate mv success.'
                # system_stage = 2
                return
        return
    # stage change
    if system_stage != last_system_stage:
        if system_stage == 1:
            subprocess.call(
                f"cd {code_path}/backend/core; nohup python -u main.py get_candidates PG > {code_path}/backend/log/stage1_output.log 2>&1 &",
                shell=True)
        elif system_stage == 2:
            subprocess.call(
                f"cd {code_path}/backend/core; nohup python -u main.py build_data PG > {code_path}/backend/log/stage2_output.log 2>&1 &",
                shell=True)
        elif system_stage == 3:
            subprocess.call(
                f"cd {code_path}/backend/core; nohup python -u main.py cost_estimation PG > {code_path}/backend/log/stage3_output.log 2>&1 &",
                shell=True)
        elif system_stage == 4:
            subprocess.call(
                f"cd {code_path}/backend/core; nohup python -u main.py recommend PG > {code_path}/backend/log/stage4_output.log 2>&1 &",
                shell=True)
        elif system_stage == 5:
            subprocess.call(
                f"cd {code_path}/backend/core; nohup python -u main.py query_rewrite_old PG > {code_path}/backend/log/stage5_output.log 2>&1 &",
                shell=True)

        last_system_stage = system_stage
        print(str(datetime.datetime.now()) + f" stage change, current stage is {system_stage}!")
        logging.info(f" stage change, current stage is {system_stage}!")
        return
    # check if stage end
    ret = subprocess.Popen(f"grep 'run success' log/stage{system_stage}_output.log", shell=True, stdout=subprocess.PIPE)
    out = ret.stdout.read().decode("utf-8")
    # print(out)
    if out.strip() == "run success!":
        print(str(datetime.datetime.now()) + f" stage {system_stage} is over!")
        logging.info(f" stage {system_stage} is over!")
        system_stage += 1
        if system_stage == 6:
            print(str(datetime.datetime.now()) + f" task end!")
            system_stage = 5
            system_start = False
        return
    last_system_stage = system_stage


# get parameter
@app.route("/save_parameters", methods=['POST'])
def save_parameters():
    engine = request.json.get("engine")
    recommend_method = request.json.get("recommend_method")
    single_or_multiple = request.json.get("single_or_multiple")
    epoch = request.json.get("epoch")
    batch_size = request.json.get("batch_size")
    cnt_limit = request.json.get("cnt_limit")
    upload_files = request.json.get('datasets')

    # space_limit = request.json.get("space_limit")
    # refer_threshold = request.json.get("refer_threshold")
    # table_size_upper_bound = request.json.get("table_size_upper_bound")
    # table_size_lower_bound = request.json.get("table_size_lower_bound")

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
    ok = check_valueisInt(batch_size, "batch_size")
    if ok:
        return jsonify({"code": 401, "msg": ok})
    ok = check_valueisInt(cnt_limit, "cnt_limit")
    if ok:
        return jsonify({"code": 401, "msg": ok})
    # ok = check_valueisInt(space_limit, "space_limit")
    # if ok:
    #     return jsonify({"code": 401, "msg": ok})
    # ok = check_valueisInt(refer_threshold, "refer_threshold")
    # if ok:
    #     return jsonify({"code": 401, "msg": ok})
    # ok = check_valueisInt(table_size_upper_bound, "table_size_upper_bound")
    # if ok:
    #     return jsonify({"code": 401, "msg": ok})
    # ok = check_valueisInt(table_size_lower_bound, "table_size_lower_bound")
    # if ok:
    #     return jsonify({"code": 401, "msg": ok})

    response = make_response(jsonify({"code": 200, "msg": "save parameters success!"}))
    # 设置响应请求头
    response.headers["Access-Control-Allow-Origin"] = '*'  # 允许使用响应数据的域。也可以利用请求header中的host字段做一个过滤器。
    response.headers["Access-Control-Allow-Methods"] = '*'  # 允许的请求方法
    response.headers["Access-Control-Allow-Headers"] = "x-requested-with,content-type"  # 允许的请求header

    print(f"save_parameters ok!")

    return response


# submit
@app.route("/submit", methods=["POST"])
def submit():
    global system_start, system_stage, log_msg
    # old code
    # system_start = True
    # system_stage = 1
    # print(str(datetime.datetime.now()) + ' submit success!')

    click_cnt = request.json.get("click_cnt")
    if click_cnt == 0:
        # stage = 1
        system_stage = 1
        # 执行候选试图生成算法
        subprocess.call(
            f"cd {code_path}/backend/core; nohup python -u main.py get_candidates PG > {code_path}/backend/log/stage1_output.log 2>&1 &",
            shell=True)
        response = make_response(jsonify({"code": 200, "msg": "submit success!", 'status': 'stage1'}))
    elif click_cnt == 1:
        system_stage = 2
        system_start = True
        log_msg = 'log'
        response = make_response(jsonify({"code": 200, "msg": "submit success!", 'status': 'stage_final'}))

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
    # print(out)
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
        path1 = f"{code_path}/backend/log/stage2_output.log"
        path2 = f"{code_path}/backend/log/stage3_output.log"
        # ans_json1 = trans_file_to_log(path1)
        # ans_json2 = trans_file_to_log(path2)
        ans_json = trans_2_file_to_log(path1, path2)
        return jsonify(ans_json)

    path = f"{code_path}/backend/log/stage{system_stage}_output.log"
    ans_json = trans_file_to_log(path)
    # print(ans_json)
    logging.info(f"the system stage: {system_stage}")

    response = make_response(jsonify(ans_json))
    # 设置响应请求头
    response.headers["Access-Control-Allow-Origin"] = '*'  # 允许使用响应数据的域。也可以利用请求header中的host字段做一个过滤器。
    response.headers["Access-Control-Allow-Methods"] = 'GET'  # 允许的请求方法
    response.headers["Access-Control-Allow-Headers"] = "x-requested-with,content-type"  # 允许的请求header

    return response


# get stage
@app.route("/get_generate_mv_result", methods=["GET"])
def get_generate_mv_result():
    global log_msg

    mv_json = {}
    if log_msg == 'generate mv success.':
        mv_json = get_candidate_mv_json()

    response = make_response(jsonify({"code": 200, "msg": "mv_json success!", "mv_json": mv_json}))
    # 设置响应请求头
    response.headers["Access-Control-Allow-Origin"] = '*'  # 允许使用响应数据的域。也可以利用请求header中的host字段做一个过滤器。
    response.headers["Access-Control-Allow-Methods"] = 'GET'  # 允许的请求方法
    response.headers["Access-Control-Allow-Headers"] = "x-requested-with,content-type"  # 允许的请求header

    return response


# get coverage
@app.route("/get_coverage", methods=["POST"])
def get_coverage():
    with open(evaluate_result_path) as f:
        json_dict = json.load(f)

    topn = request.json.get('topn')
    ans_json = compute_topn_coverage(json_dict, topn)

    # change format
    ans_json = [{'value': cnt, 'name': mv_id} for mv_id, cnt in ans_json.items()]

    response = make_response(jsonify({"code": 200, "msg": "mv_json success!", "mv_json": ans_json}))
    # 设置响应请求头
    response.headers["Access-Control-Allow-Origin"] = '*'  # 允许使用响应数据的域。也可以利用请求header中的host字段做一个过滤器。
    response.headers["Access-Control-Allow-Methods"] = 'GET'  # 允许的请求方法
    response.headers["Access-Control-Allow-Headers"] = "x-requested-with,content-type"  # 允许的请求header

    return response



@app.route("/evaluate_result", methods=["GET"])
def evaluate_result():
    # load json file
    with open(evaluate_result_path) as f:
        json_dict = json.load(f)

    response = make_response(json_dict)
    # 设置响应请求头
    response.headers["Access-Control-Allow-Origin"] = '*'  # 允许使用响应数据的域。也可以利用请求header中的host字段做一个过滤器。
    response.headers["Access-Control-Allow-Methods"] = 'GET'  # 允许的请求方法
    response.headers["Access-Control-Allow-Headers"] = "x-requested-with,content-type"  # 允许的请求header

    return response


@app.route("/mv_history_current", methods=["GET"])
def mv_history_current():
    # load json file
    with open(mv_history_current_path) as f:
        json_dict = json.load(f)

    response = make_response(json_dict)
    # 设置响应请求头
    response.headers["Access-Control-Allow-Origin"] = '*'  # 允许使用响应数据的域。也可以利用请求header中的host字段做一个过滤器。
    response.headers["Access-Control-Allow-Methods"] = 'GET'  # 允许的请求方法
    response.headers["Access-Control-Allow-Headers"] = "x-requested-with,content-type"  # 允许的请求header

    return response


@app.route("/mv_history_former", methods=["GET"])
def mv_history_former():
    # load json file
    with open(mv_history_former_path) as f:
        json_dict = json.load(f)

    response = make_response(json_dict)
    # 设置响应请求头
    response.headers["Access-Control-Allow-Origin"] = '*'  # 允许使用响应数据的域。也可以利用请求header中的host字段做一个过滤器。
    response.headers["Access-Control-Allow-Methods"] = 'GET'  # 允许的请求方法
    response.headers["Access-Control-Allow-Headers"] = "x-requested-with,content-type"  # 允许的请求header

    return response


'''
result:
[
{"info": "xxxxxxxx"},
{"info": "yyyyyyyy"}
]
'''
def trans_file_to_log(path):
    ans_json = {}
    ans_result = []
    try:
        with open(path) as f:
            for line in f.readlines():
                one_ans = {"info": line.strip()}
                ans_result.append(one_ans)
            ans_json['data'] = ans_result
    except:
        pass

    ans_json['msg'] = log_msg

    return ans_json


def trans_2_file_to_log(path1, path2):
    ans_json = {}
    ans_result = []
    try:
        with open(path1) as f:
            for line in f.readlines():
                one_ans = {"info": line.strip()}
                ans_result.append(one_ans)
    except:
        pass

    try:
        with open(path2) as f:
            for line in f.readlines():
                one_ans = {"info": line.strip()}
                ans_result.append(one_ans)
    except:
        pass

    ans_json['data'] = ans_result
    ans_json['msg'] = log_msg

    return ans_json



# Flask应用程序实例的 run 方法 启动 WEB 服务器
if __name__ == '__main__':
    # log
    logging.basicConfig(level=logging.INFO,
                        filename='log/server_imp.log',
                        filemode='a',
                        format='%(asctime)s %(levelname)s %(message)s',
                        datefmt='%Y-%m-%d %H:%M:%S')

    # app
    app.config.from_object(Config())
    scheduler.init_app(app)
    scheduler.start()

    app.run(host="0.0.0.0", port=5000, debug=True)

