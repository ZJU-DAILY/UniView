import re


'''
2. 解析DotFile文件
'''

# def parseDotFile(parentPath):
#     dotPath = parentPath + "/dot/"
#     fileNames = os.listdir(dotPath)
#     for file in fileNames:
#         dotFile = dotPath + "/" + file
#         # 将它们分割
#         key, ext = os.path.splitext(os.path.basename(dotFile))
#         print(dotFile)
#         relationDict = getOneRelationTable(dotFile)

def getOneDotFile(dotFile):
    subIdMap = {}
    with open(dotFile, encoding="utf-8") as f:
        while True:
            line = f.readline()
            if not line:
                break
            # 节点关系->：子->父
            line = line.strip()
            if line.find("subgraph cluster") != -1:
                # print(line)
                keyId = line.strip().strip("{").split("subgraph cluster")[1].strip()
                subIdList = []
                while True:
                    line = f.readline()
                    if not line:
                        break
                    if line.find("}") != -1:
                        break
                    pattern = r"[0-9]+[ ]*\["
                    if not re.search(pattern, line):
                        continue
                    subId = line.strip().split("[")[0].strip()
                    subIdList.append(subId)
                subIdMap[keyId] = subIdList
    return subIdMap

if __name__ == "__main__":
    subIdMap = getOneDotFile("resource/queries/job/q/plan-log/dot//DotFile-application_1656680820985_0005.lz4")