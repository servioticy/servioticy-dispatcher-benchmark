import sys
import os
from benchmark import visualization
from benchmark import setup

import networkx as nx



def clean_dir(dir):
    for the_file in os.listdir(dir):
        file_path = os.path.join(dir, the_file)
        if os.path.isfile(file_path):
            os.unlink(file_path)


def main():
    for i in range(int(sys.argv[1]), int(sys.argv[2])+1):
        sos = str(i)
        for j in range(1, 5*int(sos)+1):
            operators = "random.expovariate(1/"+str(j)+")"
            if j == 1:
                su = setup.Setup('../benchmark.ini', False, sos, str(j))
                clean_dir(su.config['TOPOLOGIES']['GraphsDir'])
                for i in range(len(su.topologies)):
                    nx.write_gml(su.topologies[i].stream_graph,
                                 su.config['TOPOLOGIES']['GraphsDir'] + 'stream_graph_' + str(i) + '.gml')
                visualization.show(su.config['TOPOLOGIES']['GraphsDir'], csvfile="space.csv", show_graphs=False, prependcsv=[sos,str(j)])

            su = setup.Setup('../benchmark.ini', False, sos, operators)
            clean_dir(su.config['TOPOLOGIES']['GraphsDir'])
            for i in range(len(su.topologies)):
                nx.write_gml(su.topologies[i].stream_graph,
                             su.config['TOPOLOGIES']['GraphsDir'] + 'stream_graph_' + str(i) + '.gml')
            visualization.show(su.config['TOPOLOGIES']['GraphsDir'], csvfile="space.csv", show_graphs=False, prependcsv=[sos,operators])

            if j == 5 * int(sos):
                su = setup.Setup('../benchmark.ini', False, sos, str(j))
                clean_dir(su.config['TOPOLOGIES']['GraphsDir'])
                for i in range(len(su.topologies)):
                    nx.write_gml(su.topologies[i].stream_graph,
                                 su.config['TOPOLOGIES']['GraphsDir'] + 'stream_graph_' + str(i) + '.gml')
                visualization.show(su.config['TOPOLOGIES']['GraphsDir'], csvfile="space.csv", show_graphs=False, prependcsv=[sos,str(j)])
    return


if __name__ == '__main__':
    main()