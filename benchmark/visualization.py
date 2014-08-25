__author__ = 'alvaro'

import sys
import os

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
                    G = nx.bfs_tree(graphs[graph_key], i)

                    node_colors = ['r'] * len(G.node)
                    node_colors[0] = 'b'
                    for node in G.node:
                        G.add_edges_from(nx.bfs_edges(graphs[graph_key], node))
                    p.figure(graph_key + " - " + label)
                    nx.draw_spring(G, with_labels=True, node_color=node_colors)
                    printed_graphs[graph_key + " - " + label] = G

    else:
        for graph_key in graphs.keys():
            node_colors = ['r'] * len(graphs[graph_key].node)
            in_degrees = graphs[graph_key].in_degree(graphs[graph_key])
            for n in graphs[graph_key].nodes():
                if in_degrees[n] == 0:
                    node_colors[n] = 'b'

            p.figure(graph_key)
            nx.draw_spring(graphs[graph_key], with_labels=True, node_color=node_colors)
            printed_graphs[graph_key] = graphs[graph_key]
    for graph_key in printed_graphs:
        sources = 0
        sinks = 0
        simple_paths = []
        G = printed_graphs[graph_key]
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
        in_degrees = G.in_degree(G)
        out_degrees = G.out_degree(G)
        for n in G.nodes():
            if in_degrees[n] == 0:
                sources += 1
            if out_degrees[n] == 0:
                sinks += 1
        print("Sources: " + str(sources))
        print("Sinks: " + str(sinks))
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
        print("Vertex in shortest path: " + str(simple_paths_len[0]))
        print("Vertex in longest path: " + str(simple_paths_len[-1]))
        print("Average vertex per path: " + str(sum(simple_paths_len)/len(simple_paths_len)))
        print("Degree in-assortativity coefficient: " + str(nx.degree_assortativity_coefficient(G, x="in", y="in")))
        print("Degree out-assortativity coefficient: " + str(nx.degree_assortativity_coefficient(G, x="out", y="out")))

        print()
    p.show()
    return


if __name__ == '__main__':
    main()