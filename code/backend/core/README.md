# Mview
## Getting started
### 拉取源码
```shell
git clone git@172.1.15.26:kunpeng/mview.git
cd mview/
mkdir resources
cd resources
mkdir data
mkdir 
mkdir tmp
cd ..
```
### 配置Anaconda环境
```shell
conda create -n ENV_NAME python=3.8
conda activate ENV_NAME
pip install -r requirements.txt
```
### 修改配置文件
`windows_config.cfg` 程序运行所涉及的所有运行时参数均在此文件中，包括数据存放位置、模型训练参数和Spark等参数设置
### schema文件准备
将当前数据库的schema文件（包含当前数据库的所有建表语句）移动到路径 `./resources/spark/` 下
### 生成第一批TOPN视图
```shell
python main.py get_candiates spark standalone
```
此时，在路径 `./resources/spark/mv/topMV/` 下，有第一批推荐视图。
### 成本估计模型训练
```shell
python main.py cost_estimation spark standalone
```
此时可进行视图创建，视图所在的路径为 `./resources/spark/mv/topMV/` 
### 生成第二批（第二周期）推荐视图
此时可进入query的第二个执行周期，所有业务sql再次执行；记录下执行所有sql的时间区间，填写到 `windows_config.cfg` 的`q_log_start_time`和
`q_log_end_time`参数当中。执行以下命令：
```shell
python main.py get_candiates spark standalone
python main.py recommend spark standalone
```
在路径下 `./reources/data/recommend-mvdata.csv` 中含有第二批推荐的视图编号，编号对应的sql文件位于 `./resources/spark/mv/sql/` 路径下，用户可按需创建
### 生成第三周期的推荐视图
此时可进入query的第二个执行周期，所有业务sql再次执行；记录下执行所有sql的时间区间，所有步骤同上一周期，以后周期均相同。
### 模型更新
待完成



