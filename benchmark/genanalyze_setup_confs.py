import sys
from benchmark import visualization
from benchmark import setup
import csv

def main():
    with open("raw."+sys.argv[3], 'a', newline='') as f:
        writer = csv.writer(f, delimiter=',',
                            quotechar='"', quoting=csv.QUOTE_MINIMAL)
        writer.writerow([
            "SOs",
            "Operands",
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
        for j in range(1, int(sos)+1):
            operands = "random.expovariate(1/"+str(j)+")"
            if j == 1:
                su = setup.Setup('../benchmark.ini', False, sos, str(j))
                for k in range(len(su.topologies)):
                    visualization.show_graph({'graph':su.topologies[k].stream_graph}, csvfile="raw."+sys.argv[3], show_graphs=False, prependcsv=[sos,str(j)])

            su = setup.Setup('../benchmark.ini', False, sos, operands)
            for k in range(len(su.topologies)):
                visualization.show_graph({'graph':su.topologies[k].stream_graph}, csvfile="raw."+sys.argv[3], show_graphs=False, prependcsv=[sos,operands])

            if j != 1 and j == int(sos):
                su = setup.Setup('../benchmark.ini', False, sos, str(j))
                for k in range(len(su.topologies)):
                    visualization.show_graph({'graph':su.topologies[k].stream_graph}, csvfile="raw."+sys.argv[3], show_graphs=False, prependcsv=[sos,str(j)])

    with open("raw."+sys.argv[3], 'r', newline='') as f:

        reader1 = csv.reader(f, delimiter=',', quotechar='"')

        with open(sys.argv[3], 'a', newline='') as f2:
            writer = csv.writer(f2, delimiter=',',
                                quotechar='"', quoting=csv.QUOTE_MINIMAL)
            for row1 in reader1:
                found = False
                with open("raw."+sys.argv[3], 'r', newline='') as f1:
                    reader2 = csv.reader(f1, delimiter=',', quotechar='"')
                    for row2 in reader2:
                        if row1 == row2:
                            break
                        if row1[3] == row2[3] and row1[4] == row2[4] and row1[5] == row2[5]:
                            found = True
                            break
                    if not found:
                        writer.writerow(row1)

    return


if __name__ == '__main__':
    main()