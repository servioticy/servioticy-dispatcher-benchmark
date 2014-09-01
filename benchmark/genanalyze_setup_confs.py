import sys
import os

import networkx as nx


def clean_dir(dir):
    for the_file in os.listdir(dir):
        file_path = os.path.join(dir, the_file)
        if os.path.isfile(file_path):
            os.unlink(file_path)


def main():
    for i in range(int(sys.argv[1])):
        for j in range(5 * sys.argv[1]):
            if j == 1:
                clean_dir(setup.config['TOPOLOGIES']['GraphsDir'])
                setup = setup.Setup('../benchmark.ini', i, j)
                for i in range(len(setup.topologies)):
                    nx.write_gml(setup.topologies[i].so_graph,
                                 setup.config['TOPOLOGIES']['GraphsDir'] + 'so_graph_' + str(i) + '.gml')
                    nx.write_gml(setup.topologies[i].stream_graph,
                                 setup.config['TOPOLOGIES']['GraphsDir'] + 'stream_graph_' + str(i) + '.gml')

    return


if __name__ == '__main__':
    main()