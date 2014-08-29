__author__ = 'alvaro'

import sys
import os
import numpy
import networkx as nx
import pylab as p

def all_simple_paths(G, sources, targets, cutoff=None):

    if not isinstance(sources, list):
        sources = [sources]
    if not isinstance(targets, list):
        targets = [targets]

    for source in sources:
        if source not in G:
            raise nx.NetworkXError('source node %s not in graph'%sources)

    for target in targets:
        if target not in G:
            raise nx.NetworkXError('target node %s not in graph'%targets)

    if cutoff is None:
        cutoff = len(G)-1
    by_input = {}
    result = []
    for source in sources:
        _targets = list(targets)
        by_input[source] = nx.dfs_tree(G, source)
        for node in G.node:
            by_input[source].add_edges_from(nx.dfs_edges(G, node))
        _targets[:] = [target for target in _targets if target in by_input[source].nodes()]
        result.extend(_all_simple_paths_graph(by_input[source], source=source, targets=_targets, cutoff=cutoff))
    return result

def _all_simple_paths_graph(G, source, targets, cutoff=None):
        if cutoff < 1:
            return
        visited = [source]
        stack = [iter(G[source])]
        while stack and targets:
            children = stack[-1]
            child = next(children, None)
            if child is None:
                stack.pop()
                visited.pop()
            elif len(visited) < cutoff:
                if child in targets:
                    yield visited + [child]
                elif child not in visited:
                    visited.append(child)
                    stack.append(iter(G[child]))
            else: #len(visited) == cutoff:
                if child in targets:
                        yield visited + [child]
                for target in targets:
                    if target in children:
                        yield visited + [target]
                stack.pop()
                visited.pop()

def main():
    by_input = {}
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
                    by_input[graph_name] = {}
                    printed_graphs[graph_name] = nx.bfs_tree(graphs[graph_key], i)
                    for node in printed_graphs[graph_name].node:
                        printed_graphs[graph_name].add_edges_from(nx.dfs_edges(graphs[graph_key], node))
                    by_input[i] = printed_graphs[graph_name]
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
        simple_paths.extend(all_simple_paths(G, sources=sources, targets=sinks))

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