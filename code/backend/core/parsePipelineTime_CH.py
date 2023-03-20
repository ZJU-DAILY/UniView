import os
from random import sample
from graphviz import Digraph


class PipelineNode(object):
    def __init__(self, key, label, cost=0.0):
        self.key = key
        self.label = label
        self.cost = cost
        self.children = []
        self.ancestor = None
        self.dot = Digraph(comment="pipelineTree")

    def __str__(self):
        return self.label + "\ncost: " + str(self.cost)

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


def buildNode(line):
    key, labelStr = line.split("[")
    labelStr = labelStr.replace("label=\"", "")
    label, detail, _ = labelStr.split("(")
    # 耗时的单位时秒，
    timeCost = detail.split(",")[1].split(":")[1].replace("sec.", "").strip()
    # if -1 != timeCost.find("e"):
    #     timeCost = timeCost.replace("e-0","e+")
    timeCost = float(timeCost)
    node = PipelineNode(key.strip(), label, timeCost)
    return key.strip(), node


def buildEdge(line, nodes):
    key1, key2 = line.split("->")
    node1 = nodes[key1.strip()]
    node2 = nodes[key2.strip().strip(";")]
    node2.children.append(node1)
    node1.ancestor = node2


def updateTreeTime(root):
    def maxCost(samePipeNodes):
        maxTime = 0.0
        for pipeNode in samePipeNodes:
            if pipeNode.cost > maxTime:
                maxTime = pipeNode.cost
        return maxTime

    if root.ancestor is not None:
        return

    def procPipeNode(node):
        # childCost = 0

        if len(node.children) == 0:
            return
        if len(node.children) > 1 and len(list(set([child.label for child in node.children]))) == 1:
            childCost = maxCost(node.children)
            node.cost += childCost
            return
        for child in node.children:
            procPipeNode(child)
            node.cost += child.cost
        return

    procPipeNode(root)


def buildPipelineTree(pipelinePath):
    key, ext = os.path.splitext(os.path.basename(pipelinePath))
    if ext != ".pipeline":
        return None
    nodes = {}
    with open(pipelinePath, "r") as f:
        line = f.readline()
        while line:
            # 只需要关心label行和->行
            if line.find("label=") != -1:
                key, node = buildNode(line)
                nodes[key] = node
            elif line.find("->") != -1:
                buildEdge(line, nodes)
            line = f.readline()
    for key, node in nodes.items():
        if node.ancestor is None:
            updateTreeTime(node)
            return node
    return None


if __name__ == '__main__':
    node = buildPipelineTree("D:/物化视图/HW/resource/click-house-new/q/pipeline/1a.pipeline")
    # node.printTree(save_path="noupdate-10b.gv")
    # updateTreeTime(node)
    node.printTree(save_path="tree/update-1a.gv")
