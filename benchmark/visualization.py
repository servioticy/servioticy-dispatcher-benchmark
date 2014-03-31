__author__ = 'alvaro'

import sys
import networkx as nx
import pylab as p


def main():
    graph = nx.read_gml(sys.argv[1])
    nx.draw_networkx(graph, with_labels=False)
    p.show()
    return


if __name__ == '__main__':
    main()