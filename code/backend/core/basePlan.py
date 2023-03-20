from enum import Enum
import uuid
from random import sample
from graphviz import Digraph


class NodeType(Enum):
    RELATION = 0
    SELECT = 1
    PROJECT = 2
    JOIN = 3
    AGGREGATE = 4
    ROOT = 5
    EXPRESSION = 6
    FILTER = 7
    MERGE = 8


class RelationNode(object):
    def __init__(self, rel):
        self.relation = rel
        self.type = NodeType.RELATION
        self.condition = None

    def __str__(self):
        return self.type.name + ":" + self.relation

    def __eq__(self, node):
        if self.type != node.type:
            return False
        # 不区分大小写
        if self.relation.upper() != node.relation.upper():
            return False
        return True


class SelectNode(object):
    def __init__(self, condition=None, cost=0.0):
        self.condition = condition
        self.type = NodeType.SELECT
        self.cost = cost

    def __str__(self):
        if self.condition is None:
            return self.type.name + "\n cost:" + str(self.cost)
        return self.type.name + ":" + self.condition + "\n cost:" + str(self.cost)

    def __eq__(self, node):
        if self.type != node.type:
            return False
        return True


class FilterNode(object):
    def __init__(self, condition=None, cost=0.0):
        self.condition = condition
        self.type = NodeType.FILTER
        self.cost = cost

    def __str__(self):
        if self.condition is None:
            return self.type.name + "\n cost:" + str(self.cost)
        return self.type.name + ":" + self.condition + "\n cost:" + str(self.cost)

    def __eq__(self, node):
        if self.type != node.type:
            return False
        return True


class ProjectNode(object):
    def __init__(self, attribute="", cost=0.0):
        self.attribute = attribute
        self.type = NodeType.PROJECT
        self.cost = cost

    def __str__(self):
        return self.type.name + ":" + str(self.attribute) + "\n cost:" + str(self.cost)

    def __eq__(self, node):
        if self.type != node.type:
            return False
        return True


class JoinNode(object):
    def __init__(self, condition=None, cost=0.0):
        self.type = NodeType.JOIN
        self.condition = condition
        self.cost = cost

    def __str__(self):
        if self.condition is None:
            return self.type.name + "\n cost:" + str(self.cost)
        return self.type.name + ":" + self.condition + "\n cost:" + str(self.cost)

    def __eq__(self, node):
        if self.type != node.type:
            return False
        if self.condition is not None and node.condition is not None and self.condition.upper() == node.condition.upper():
            return True
        return False


class Node(object):
    def __init__(self, name):
        self.type = NodeType.ROOT
        self.name = name

    def __str__(self):
        return self.name

    def __eq__(self, node):
        if self.type != node.type:
            return False
        return True


class ExpressNode(object):
    def __init__(self, attribute="", cost=0.0, condition=None):
        self.attribute = attribute
        self.type = NodeType.EXPRESSION
        self.cost = cost
        self.condition = condition

    def __str__(self):
        if self.attribute == "":
            return self.type.name + "\n cost:" + str(self.cost)
        return self.type.name + ":\n" + self.attribute + "\n cost:" + str(self.cost)

    def __eq__(self, node):
        if self.type != node.type:
            return False
        return True


class MergeNode(object):
    def __init__(self, attribute="", relation=None, cost=0.0, condition=None):
        self.relation = relation
        self.attribute = attribute
        self.type = NodeType.MERGE
        self.cost = cost
        self.condition = condition

    def __str__(self):
        if len(self.attribute) == 0:
            if self.relation is None:
                return self.type.name + "\n cost:" + str(self.cost)
            else:
                return self.type.name + ":" + self.relation + "\n cost:" + str(self.cost)
        else:
            if self.relation is None:
                return self.type.name + ":" + self.attribute + "\n cost:" + str(self.cost)
            else:
                return self.type.name + ":" + self.relation + self.attribute + "\n cost:" + str(self.cost)

    def __eq__(self, node):
        if self.type != node.type:
            return False
        return True


class PlanTree(object):
    def __init__(self, data=None, left=None, right=None):
        self.data = data
        self.left = left
        self.right = right
        # self.visited = False
        self.tag = str(uuid.uuid1())
        self.relations = {}
        self.clusterID = None
        self.ancestor = None
        # CH中使用该字段
        self.leaveNodes = None
        # 把子树的执行计划也保存一下，在生成候选视图的时候可以直接dump
        self.plan = None
        self.referKeys = {}
        self.dot = Digraph(comment="planTree")

    def leaves(self):
        if self.data is None:
            return None
        if self.left is None:
            if self.right is None:
                print(self.data + " ")
            else:
                self.right.leaves()
        else:
            if self.right is None:
                self.left.leaves()
            else:
                self.left.leaves()
                self.right.leaves()

    def printTree(self, save_path="./PlanTree.gv", label=False):
        colors = ["skyblue", "tomato", "orange", "purple", "green", "yellow", "pink"]

        def printNode(node, nodeTag):
            color = sample(colors, 1)[0]

            if node.left is not None:
                if hasattr(node.left.data, "type") and node.left.data.type == NodeType.RELATION:
                    self.dot.node(node.left.tag,
                                  str(node.left.data).replace(" AND ", "\nAND ").replace(" OR ", "\nOR "),
                                  style="filled", color=color, shape='rectangle', width='2')
                    # self.dot.edge(node.left.tag, node.left.ancestor.tag, label="ancestor", style="dashed")
                else:
                    self.dot.node(node.left.tag,
                                  str(node.left.data).replace(" AND ", "\nAND ").replace(" OR ", "\nOR "),
                                  style="filled", color=color, width='2')
                label_string = ""
                if label:
                    label_string = "L"
                self.dot.edge(nodeTag, node.left.tag, label=label_string)
                # self.dot.edge(node.left.tag, nodeTag, label="ancestor", style="dashed")
                # if node.left.visited is False:
                printNode(node.left, node.left.tag)
            if node.right is not None:
                if hasattr(node.right.data, "type") and node.right.data.type == NodeType.RELATION:
                    self.dot.node(node.right.tag,
                                  str(node.right.data).replace(" AND ", "\nAND ").replace(" OR ", "\nOR "),
                                  style="filled", color=color, shape='rectangle', width='2')
                    # self.dot.edge(node.right.tag, node.right.ancestor.tag, label="ancestor", style="dashed")
                else:
                    self.dot.node(node.right.tag,
                                  str(node.right.data).replace(" AND ", "\nAND ").replace(" OR ", "\nOR "),
                                  style="filled", color=color, width='2')

                label_string = ""
                if label:
                    label_string = "R"
                self.dot.edge(nodeTag, node.right.tag, label=label_string)
                # self.dot.edge(node.right.tag, nodeTag, label="ancestor", style="dashed")
                # if node.right.visited is False:
                printNode(node.right, node.right.tag)
            # node.visited = True

        if self.data is not None:
            if self.data.type is NodeType.ROOT:
                self.dot.node(self.tag, str(self.data), style="filled", color=sample(colors, 1)[0], shape='star')
            else:
                self.dot.node(self.tag, str(self.data), style="filled", color=sample(colors, 1)[0], shape='octagon')
            printNode(self, self.tag)

        self.dot.render(save_path)



