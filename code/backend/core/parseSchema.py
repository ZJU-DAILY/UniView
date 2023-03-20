from collections import defaultdict
from procConf import *


class tableSchema(object):
    def __init__(self, config=None):
        if config is None:
            self.config = initConfig()
        else:
            self.config = config
        # print("init")
        schemaFile = self.config.get("schema", "fullpath")
        self.data = {}
        with open(schemaFile, 'r', encoding='utf-8') as r:
            while '' != r.read(1):
                r.seek(r.tell() - 1)
                self.buildTable(r)

    def buildTable(self, SchemaFile):
        name = ""
        keyname = {}
        partition = {}
        index = 0
        while True:
            line = SchemaFile.readline()
            upperLine = line.upper()
            if '\n' == line:
                break
            # print(upperLine)
            if -1 != upperLine.find("CREATE") and -1 != upperLine.find("TABLE"):
                name = upperLine.strip().split()[2].split(".")[-1]
                continue
            if -1 != upperLine.find(";"):
                break
            if 0 == len(upperLine.strip()):
                continue
            if 2 > len(upperLine.strip().split()):
                continue
            if upperLine.endswith(",\n") or lastLine.endswith(",\n"):
                if get_engine() == "CH" and get_use_projection() == "True":
                    key = line.strip().split()[0]
                    type = line.strip().split()[1]
                else:
                    key = upperLine.strip().split()[0]
                    type = upperLine.strip().split()[1]
                if type.find("(") != -1:
                    type = type[type.find("(") + 1:type.find(")")]
            if upperLine.strip().startswith("PARTITION BY") or upperLine.strip().startswith("PARTITIONED BY"):
                # 没有函数作用在key上的时候，如果只有一个key也有可能没有用括号括起来
                if upperLine.find("(") == -1:
                    partition_key = [upperLine.strip().split()[-1]]
                    partition_desc = upperLine.strip().split()[-1]
                else:
                    partition_key = [upperLine[upperLine.find("(") + 1:upperLine.find(")")].split(",")]
                    if upperLine.strip().startswith("PARTITION BY"):
                        partition_desc = line[upperLine.index("PARTITION BY") + len("PARTITION BY") + 1:line.rfind(")") + 1]
                    else:
                        partition_desc = line[
                                         upperLine.index("PARTITIONED BY") + len("PARTITIONED BY") + 1:line.rfind(")") + 1]
                partition["key"] = partition_key
                partition["desc"] = partition_desc
            # if -1 != type.find("("):
            #     type = type.split("(")[1].split(")")[0]
            # keyname[key] = index
            keyname[key] = type
            lastLine = upperLine
            index = index + 1
        if name != "":
            self.data[name] = keyname
            if partition:
                self.data[name]["infoOfPartition"] = partition
        # print(keyname)
        return


# 获取表的元数据信息
# 输入数据库名、表名
# 返回列名数组
def getTableSchema(dbname, tableName):
    print(dbname)
    # 从配置文件中读取
    schemaFile = get_schema_file()
    with open(schemaFile, 'r') as r:
        while True:
            line = r.readline()
            if '' == line:
                break
            upperLine = line.upper()
            if -1 != upperLine.find("CREATE") and -1 != upperLine.find("TABLE"):
                name = upperLine.strip().split()[2]
                if -1 != name.find("."):
                    db, table = name.split(".")
                else:
                    db = dbname
                    table = name
                if db == dbname.upper() and table == tableName.upper():
                    r.seek(r.tell() - len(line))
                    return getTableCols(r)
    return


def getTableCols(SchemaFile):
    # name = ""
    keyname = []
    # index = 0
    while True:
        line = SchemaFile.readline()
        upperLine = line.upper()
        if '' == line:
            break
        if -1 != upperLine.find(";"):
            break
        if 0 == len(upperLine.strip()):
            continue
        key = upperLine.strip().split()[0]
        keyname.append(key)
    # print(keyname)
    return keyname


if __name__ == '__main__':
    # print(getTableSchema("UserOper_local", "ads_useroper_compet_idc_hiaudio_mod_g1o_sales_ds"))
    test_table = tableSchema()
    print(test_table.data.items())
