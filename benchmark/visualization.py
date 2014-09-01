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
    q = queue.Queue()
    for source in sources:
        result.extend(_all_simple_paths_graph_len(G, source))

    return result

def _all_simple_paths_graph_worker(G, source, visited, q, used_workers):
    q.put(_all_simple_paths_graph_len(G, source, visited, used_workers))


def _all_simple_paths_graph_len(G, source, visited=1, used_workers=queue.Queue()):
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

def show(graphs_dir, initso=None, initstream=None, csvfile=None, show_graphs=True):
    by_input = {}
    graphs = {}
    printed_graphs = {}
    if os.path.isdir(graphs_dir):
        for file in os.listdir(graphs_dir):
            graphs[file] = nx.read_gml(graphs_dir + '/' + file)
    elif os.path.isfile(graphs_dir):
        graphs[graphs_dir] = nx.read_gml(graphs_dir)

    if initso != None:
        label = initso
        if initstream != None:
            label += ":" + initstream
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
        if show:
            node_colors = ['r'] * len(G.node)
            for i in sinks:
                node_colors[i] = 'g'
            for i in sources:
                node_colors[i] = 'b'
            p.figure(graph_key)
            nx.draw_spring(G, with_labels=True, node_color=node_colors)

        in_degrees = []
        for node in G.nodes():
            in_degrees.append(G.in_degree(node))
            in_degrees = sorted(in_degrees)

        out_degrees = []
        for node in G.nodes():
            out_degrees.append(G.out_degree(node))
            out_degrees = sorted(out_degrees)

        simple_paths = []
        simple_paths.extend(all_simple_paths_len(G, sources=sources))

        graph_info = [
            str(len(G.node)),
            str(nx.density(G.to_undirected())),
            str(G.number_of_edges()),
            str(nx.node_connectivity(G.to_undirected())),
            str(nx.edge_connectivity(G.to_undirected())),
            str(len(sources)),
            str(len(sinks)),
            str(in_degrees[-1]),
            str(numpy.mean(in_degrees, axis=0)),
            str(numpy.std(in_degrees, axis=0)),
            str(out_degrees[-1]),
            str(numpy.mean(out_degrees, axis=0)),
            str(numpy.std(out_degrees, axis=0)),
            str(nx.is_directed_acyclic_graph(G)),
            str(len(simple_paths)),
            str(simple_paths[0] if len(simple_paths) > 0 else 0),
            str(simple_paths[-1] if len(simple_paths) > 0 else 0),
            str(numpy.mean(simple_paths) if len(simple_paths) > 0 else 0),
            str(numpy.std(simple_paths) if len(simple_paths) > 0 else 0),
            str(nx.degree_assortativity_coefficient(G, x="in", y="in")),
            str(nx.degree_assortativity_coefficient(G, x="out", y="out"))
        ]
        if csvfile == None:
            for i in range(len(graph_key) + 4):
                print('*', end="")
            print('\n* ' + graph_key + ' *')
            for i in range(len(graph_key) + 4):
                print('*', end="")
            print()
            info_pos = iter(graph_info)
            print("Nodes: " + next(info_pos))
            print("Edges: " + next(info_pos))
            print("Connectivity (weak): " + next(info_pos))
            print("Edge-connectivity (weak): " + next(info_pos))
            print("Sources: " + next(info_pos))
            print("Sinks: " + next(info_pos))
            print("Density (DAG): " + next(info_pos))
            print("In degrees max: " + next(info_pos))
            print("In degrees mean: " + next(info_pos))
            print("In degrees standard deviation: " + next(info_pos))
            print("Out degrees max: " + next(info_pos))
            print("Out degrees mean: " + next(info_pos))
            print("Out degrees standard deviation: " + next(info_pos))
            print("It is a DAG: " + next(info_pos))
            print("Paths (from a source to a sink): " + next(info_pos))
            # simple_paths_len = []
            # for i in range(len(simple_paths)):
            # simple_paths_len.append(len(simple_paths[i]))
            if len(simple_paths) > 0:
                print("Vertex per path min: " + next(info_pos))
                print("Vertex per path max: " + next(info_pos))
                print("Vertex per path mean: " + next(info_pos))
                print("Vertex per path standard deviation: " + next(info_pos))
            print("Degree in-assortativity coefficient: " + next(info_pos))
            print("Degree out-assortativity coefficient: " + next(info_pos))
            print()
    if show:
        p.show()
    return


def main():
    if len(sys.argv) == 2:
        show(graphs_dir=sys.argv[1])
    elif len(sys.argv) == 3:
        show(graphs_dir=sys.argv[1], initso=sys.argv[2])
    elif len(sys.argv) == 4:
        show(graphs_dir=sys.argv[1], initso=sys.argv[2], initstream=sys.argv[3])
    return

if __name__ == '__main__':
    main()