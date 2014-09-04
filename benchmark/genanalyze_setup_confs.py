import sys
import os
from benchmark import visualization
from benchmark import setup

import networkx as nx

def main():
    for i in range(int(sys.argv[1]), int(sys.argv[2])+1):
        sos = str(i)
        for j in range(1, 5*int(sos)+1):
            operators = "random.expovariate(1/"+str(j)+")"
            if j == 1:
                su = setup.Setup('../benchmark.ini', False, sos, str(j))
                for k in range(len(su.topologies)):
                    visualization.show_graphs({'graph':su.topologies[k].stream_graph}, csvfile=sys.argv[3], show_graphs=False, prependcsv=[sos,str(j)])

            su = setup.Setup('../benchmark.ini', False, sos, operators)
            for k in range(len(su.topologies)):
                visualization.show_graphs({'graph':su.topologies[k].stream_graph}, csvfile=sys.argv[3], show_graphs=False, prependcsv=[sos,operators])

            if j == 5 * int(sos):
                su = setup.Setup('../benchmark.ini', False, sos, str(j))
                for k in range(len(su.topologies)):
                    visualization.show_graphs({'graph':su.topologies[k].stream_graph}, csvfile=sys.argv[3], show_graphs=False, prependcsv=[sos,str(j)])
    return


if __name__ == '__main__':
    main()