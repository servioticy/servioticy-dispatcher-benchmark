__author__ = 'alvaro'

import sys
import os
import threading
import queue
import multiprocessing

import numpy
import networkx as nx
import pylab as p


def all_simple_paths_len(G, sources):

    if not isinstance(sources, list):
        sources = [sources]

    for source in sources:
        if source not in G:
            raise nx.NetworkXError('source node %s not in graph'%sources)
    result = []
    workers = []
    q = queue.Queue()
    for source in sources:
        workers.append(threading.Thread(target=_all_simple_paths_graph_worker_len, args=(q, result, G, source)))
        workers[-1].daemon = True
        workers[-1].start()
    for worker in workers:
        q.get()
    return result


def _all_simple_paths_graph_worker_len(q, result, G, source):
    for path in _all_simple_paths_graph(G, source=source):
        result.append(path)
    q.put(('done'))
    return result


def _all_simple_paths_graph_worker(G, source, visited, q, used_workers):
    q.put(_all_simple_paths_graph(G, source, visited, used_workers))


def _all_simple_paths_graph(G, source, visited=1, used_workers=queue.Queue()):
    used_workers.put(1)
    workers = []
    worker_results = queue.Queue()
    result = sorted([])
        stack = [iter(G[source])]
    while stack:
            children = stack[-1]
            child = next(children, None)
            if child is not None:
                if len(G[child]) == 0:
                    visited += 1
                    result.append(visited)
                else:
                    visited += 1
                cores = multiprocessing.cpu_count()
                if cores > used_workers.qsize():
                    workers.append(threading.Thread(target=_all_simple_paths_graph_worker,
                                                    args=(G, child, visited, worker_results, used_workers)))
                    workers[-1].daemon = True
                    workers[-1].start()
                    visited -= 1
                else:
                    stack.append(iter(G[child]))
            else:
                stack.pop()
                visited -= 1
    used_workers.get()
    for worker in workers:
        result.extend(worker_results.get())
    return sorted(result)


# def dag_paths_lens(G, source, partlen=0):
# path_lens = sorted([])
#     partlen += 1
#     for node in G[source]:
#         path_lens.extend(dag_paths_lens(G, node, partlen))
#     if len(G[source]) == 0:
#         path_lens.append(partlen)
#     return path_lens

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
        in_degrees = []
        for node in G.nodes():
            in_degrees.append(G.in_degree(node))
            in_degrees = sorted(in_degrees)
        print("In degrees max: " + str(in_degrees[-1]))
        print("In degrees mean: " + str(numpy.mean(in_degrees, axis=0)))
        print("In degrees standard deviation: " + str(numpy.std(in_degrees, axis=0)))
        out_degrees = []
        for node in G.nodes():
            out_degrees.append(G.out_degree(node))
            out_degrees = sorted(out_degrees)
        print("Out degrees max: " + str(out_degrees[-1]))
        print("Out degrees mean: " + str(numpy.mean(out_degrees, axis=0)))
        print("Out degrees standard deviation: " + str(numpy.std(out_degrees, axis=0)))
        print("It is a DAG: " + str(nx.is_directed_acyclic_graph(G)))
        # simple_paths = dag_paths_lens(G, sources[0])
        simple_paths.extend(all_simple_paths_len(G, sources=sources))

        print("Paths (from a source to a sink): " + str(len(simple_paths)))
        # simple_paths_len = []
        # for i in range(len(simple_paths)):
        # simple_paths_len.append(len(simple_paths[i]))
        if len(simple_paths) > 0:
            print("Vertex per path min: " + str(simple_paths[0]))
            print("Vertex per path max: " + str(simple_paths[-1]))
            print("Vertex per path mean: " + str(numpy.mean(simple_paths, axis=0)))
            print("Vertex per path standard deviation: " + str(numpy.std(simple_paths, axis=0)))
        print("Degree in-assortativity coefficient: " + str(nx.degree_assortativity_coefficient(G, x="in", y="in")))
        print("Degree out-assortativity coefficient: " + str(nx.degree_assortativity_coefficient(G, x="out", y="out")))
        print()
    p.show()
    return


if __name__ == '__main__':
    main()