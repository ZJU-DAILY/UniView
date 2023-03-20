from graphviz import Digraph
from random import sample
import re
import os

# os.environ["PATH"] += os.pathsep + r"D:\NotMove\graphviz\bin"

'''
0.将生成的树显示出来
'''


class SparkNode(object):
    def __init__(self, key, label):
        self.key = key
        self.label = label
        self.children = []
        self.ancestor = []
        self.dot = Digraph(comment="SparkNode")

    def __str__(self):
        return self.key + " name: " + str(self.label)

    def printTree(self, save_path="./PlanTree.gv"):
        colors = ["skyblue", "tomato", "orange", "purple", "green", "yellow", "pink"]

        def printNode(node, nodeTag):
            color = sample(colors, 1)[0]
            for child in node.children:
                if len(child.children) == 0:
                    self.dot.node(child.key, str(child).replace("), ", "),\n"),
                                  style="filled", color=color, shape='rectangle', width='2')
                # self.dot.edge(node.left.tag, node.left.ancestor.tag, label="ancestor", style="dashed")
                else:
                    self.dot.node(child.key, str(child).replace("), ", "),\n"),
                                  style="filled", color=color, width='2')
                self.dot.edge(nodeTag, child.key)
                # self.dot.edge(node.left.tag, nodeTag, label="ancestor", style="dashed")
                # if node.left.visited is False:
                printNode(child, child.key)

        if self.key is not None:
            self.dot.node(self.key, str(self), style="filled", color=sample(colors, 1)[0], shape='octagon')
            printNode(self, self.key)

        self.dot.render(save_path)


def dotTreePrint(file):
    dot = Digraph(comment='The Tree')
    nodes = {}
    with open(file, encoding="utf-8") as f:
        while True:
            line = f.readline()
            if not line:
                break
            line = line.strip()
            # 1.处理每个结点
            pattern = r"id:([0-9]+)[ ]*name:([\w0-9]+)"
            key = re.findall(pattern, line)
            if len(key) > 0:
                id = key[0][0].strip()
                name = key[0][1].strip()
                if name == "WholeStageCodegen":  # 该结点并不参与树的构成
                    continue
                # 生成SparkNode结点
                node = SparkNode(id, name)
                nodes[id] = node
            # 2.处理->
            pattern = r"([\w]+)->([\w]+)"
            key = re.findall(pattern, line)
            if len(key) > 0:
                son = key[0][0].strip()
                father = key[0][1].strip()
                nodeSon = nodes[son]
                nodeFather = nodes[father]
                nodeSon.ancestor.append(nodeFather)
                nodeFather.children.append(nodeSon)

    # 找出根节点
    for key, node in nodes.items():
        if node.ancestor == []:
            # node.printTree(save_path="10b.gv")
            return node


def printAllNodeTree(parentPath):
    nodePath = parentPath + "/node/"
    printPath = parentPath + "/mv/gv/"
    fileNames = os.listdir(nodePath)
    for file in fileNames:
        nodeFile = nodePath + file
        key, ext = os.path.splitext(os.path.basename(nodeFile))
        node = dotTreePrint(nodeFile)
        printFile = printPath + key[-4:] + ".gv"
        node.printTree(save_path=printFile)

if __name__ == "__main__":
    # node = dotTreePrint("resource/queries/job/q/plan-log/node/node-application_1656680820985_0008.lz4")
    # node.printTree(save_path="resource/queries/job/q/plan-log/mv/gv/test0008_2.gv")

    printAllNodeTree("resources/spark")