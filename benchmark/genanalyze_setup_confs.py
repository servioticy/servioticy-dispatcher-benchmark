import sys
from benchmark import visualization
from benchmark import setup
import csv

def main():
    with open(sys.argv[3], 'a', newline='') as f:
        writer = csv.writer(f, delimiter=',',
                            quotechar='"', quoting=csv.QUOTE_MINIMAL)
        writer.writerow([
            "SOs",
            "Operand distribution",
            "Nodes",
            "Paths (from a source to a sink)",
            "Density (DAG)",
            "Edges",
            "Connectivity (weak)",
            "Edge-connectivity (weak)",
            "Sources",
            "Sinks",
            "In degrees max",
            "In degrees mean",
            "In degrees standard deviation",
            "Out degrees max",
            "Out degrees mean",
            "Out degrees standard deviation",
            "DAG",
            "Vertex per path min",
            "Vertex per path max",
            "Vertex per path mean",
            "Vertex per path standard deviation",
            "Degree in-assortativity coefficient",
            "Degree out-assortativity coefficient"
        ])
    for i in range(int(sys.argv[1]), int(sys.argv[2])+1):
        sos = str(i)
        for j in range(1, 5*int(sos)+1):
            operators = "random.expovariate(1/"+str(j)+")"
            if j == 1:
                su = setup.Setup('../benchmark.ini', False, sos, str(j))
                for k in range(len(su.topologies)):
                    visualization.show_graph({'graph':su.topologies[k].stream_graph}, csvfile=sys.argv[3], show_graphs=False, prependcsv=[sos,str(j)])

            su = setup.Setup('../benchmark.ini', False, sos, operators)
            for k in range(len(su.topologies)):
                visualization.show_graph({'graph':su.topologies[k].stream_graph}, csvfile=sys.argv[3], show_graphs=False, prependcsv=[sos,operators])

            if j == 5 * int(sos):
                su = setup.Setup('../benchmark.ini', False, sos, str(j))
                for k in range(len(su.topologies)):
                    visualization.show_graph({'graph':su.topologies[k].stream_graph}, csvfile=sys.argv[3], show_graphs=False, prependcsv=[sos,str(j)])
    return


if __name__ == '__main__':
    main()