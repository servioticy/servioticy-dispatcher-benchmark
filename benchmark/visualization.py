__author__ = 'alvaro'

import sys
import os
import numpy
import networkx as nx
import pylab as p

def main():
    graphs = {}
    printed_graphs = {}
    if os.path.isdir(sys.argv[1]):
        for file in os.listdir(sys.argv[1]):
            graphs[file] = nx.read_gml(sys.argv[1] + '/' + file)
    elif os.path.isfile(sys.argv[1]):
        graphs[sys.argv[1]] = nx.read_gml(sys.argv[1])

    if len(sys.argv) > 2:
        label = sys.argv[2]
        if len(sys.argv) > 3:
            label += ":" + sys.argv[3]
        for graph_key in graphs.keys():
            for i in range(len(graphs[graph_key].node)):
                current_label = graphs[graph_key].node[i]['label']
                if current_label == label:
                    graph_name = graph_key + " - " + label
                    printed_graphs[graph_name] = nx.bfs_tree(graphs[graph_key], i)
                    for node in printed_graphs[graph_name].node:
                        printed_graphs[graph_name].add_edges_from(nx.bfs_edges(graphs[graph_key], node))

    else:
        for graph_key in graphs.keys():
            printed_graphs[graph_key] = graphs[graph_key]
    for graph_key in printed_graphs:
        G = printed_graphs[graph_key]
        sources = []
        sinks = []
        in_degrees = G.in_degree(G)
        out_degrees = G.out_degree(G)
        for n in range(len(G.nodes())):
            if in_degrees[G.nodes()[n]] == 0:
                sources.append(n)
            if out_degrees[G.nodes()[n]] == 0:
                sinks.append(n)
        node_colors = ['r'] * len(G.node)
        for i in sinks:
            node_colors[i] = 'g'
        for i in sources:
            node_colors[i] = 'b'
        p.figure(graph_key)
        nx.draw_spring(G, with_labels=True, node_color=node_colors)

        simple_paths = []
        for i in range(len(graph_key)+4):
            print('*', end="")
        print('\n* '+graph_key+' *')
        for i in range(len(graph_key)+4):
            print('*', end="")
        print()
        print("Nodes: " + str(len(G.node)))
        print("Edges: " + str(G.number_of_edges()))
        print("Connectivity (weak): " + str(nx.node_connectivity(G.to_undirected())))
        print("Edge-connectivity (weak): " + str(nx.edge_connectivity(G.to_undirected())))
        print("Sources: " + str(len(sources)))
        print("Sinks: " + str(len(sinks)))
        print("Density (DAG): " + str(nx.density(G.to_undirected())))
        out_degrees = []
        for node in G.nodes():
            out_degrees.append(G.out_degree(node))
            out_degrees = sorted(out_degrees)
        print("Out degrees max: " + str(out_degrees[-1]))
        print("Out degrees mean: " + str(numpy.mean(out_degrees, axis=0)))
        print("Out degrees standard deviation: " + str(numpy.std(out_degrees, axis=0)))
        for id in in_degrees:
            if in_degrees[id] != 0:
                continue
            for od in out_degrees:
                if out_degrees[od] != 0:
                    continue
                if nx.has_path(G, source=id, target=od):
                    simple_paths.extend(nx.all_simple_paths(G, source=id, target=od))
        print("Paths (from a source to a sink): " + str(len(simple_paths)))
        simple_paths_len = []
        for i in range(len(simple_paths)):
            simple_paths_len.append(len(simple_paths[i]))
        simple_paths_len = sorted(simple_paths_len)
        if len(simple_paths_len) > 0:
            print("Vertex per path min: " + str(simple_paths_len[0]))
            print("Vertex per path max: " + str(simple_paths_len[-1]))
            print("Vertex per path mean: " + str(numpy.mean(simple_paths_len, axis=0)))
            print("Vertex per path standard deviation: " + str(numpy.std(simple_paths_len, axis=0)))
        print("Degree in-assortativity coefficient: " + str(nx.degree_assortativity_coefficient(G, x="in", y="in")))
        print("Degree out-assortativity coefficient: " + str(nx.degree_assortativity_coefficient(G, x="out", y="out")))
        print()
    p.show()
    return


if __name__ == '__main__':
    main()