__author__ = 'alvaro'

import sys
import networkx as nx
import pylab as p
import random
import os


def main():
    graphs = []
    if os.path.isdir(sys.argv[1]):
        for file in os.listdir(sys.argv[1]):
            graphs.append(nx.read_gml(sys.argv[1] + '/' + file))
    elif os.path.isfile(sys.argv[1]):
        graphs.append(nx.read_gml(sys.argv[1]))

    if len(sys.argv) > 1:
        for graph in graphs:
            for i in range(len(graph.node)):
                label = sys.argv[2]
                current_label = graph.node[i]['label']
                if current_label == label:
                    p.figure(label)
                    nx.draw_networkx(nx.bfs_tree(graph, i), with_labels=False)

    else:
        i = 0
        for graph in graphs:
            p.figure(i)
            nx.draw_networkx(graph, with_labels=False)
            ++i

    p.show()
    return


if __name__ == '__main__':
    main()