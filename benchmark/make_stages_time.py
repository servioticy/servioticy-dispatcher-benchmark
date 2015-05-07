import sys
import csv
import statistics

def main():
    ins = []
    outs = {}
    processes = []
    queues = []

    in_degrees = {}
    out_degrees = {}

    with open(sys.argv[2], 'r', newline='') as f:
        reader1 = csv.reader(f, delimiter=',', quotechar='"')
        for row in reader1:
            in_degrees[row[0]] = int(row[1])
    with open(sys.argv[3], 'r', newline='') as f:
        reader1 = csv.reader(f, delimiter=',', quotechar='"')
        for row in reader1:
            out_degrees[row[0]] = int(row[1])
    with open(sys.argv[1], 'r', newline='') as f:
        reader1 = csv.reader(f, delimiter=',', quotechar='"')
        for row in reader1:
            stream = row[1]+':'+row[2]
            time = int(row[4]) - int(row[3])
            newrow = [row[3], row[4], time]
            init = row[3]
            if row[0] == "in":
                newrow.append(in_degrees[stream])
                ins.append(newrow)
            elif row[0] == "out":
                newrow.append(out_degrees[stream])
                if stream not in list(outs.keys()):
                    outs[stream] = {}
                if init in list(outs[stream].keys()):
                    if time > outs[stream][init][2]:
                        outs[stream][init] = newrow
                else:
                    outs[stream][init] = newrow
            elif row[0] == "process":
                newrow.append(in_degrees[stream])
                processes.append(newrow)
            elif row[0] == "queue":
                queues.append(newrow)

        with open(sys.argv[1]+"-in.csv", 'a', newline='') as fin:
            writerin = csv.writer(fin, delimiter=',',
                                quotechar='"', quoting=csv.QUOTE_MINIMAL)
            for instage in ins:
                writerin.writerow(instage)
        with open(sys.argv[1]+"-out.csv", 'a', newline='') as fout:
            writerout = csv.writer(fout, delimiter=',',
                                  quotechar='"', quoting=csv.QUOTE_MINIMAL)
            for streamid in sorted(list(outs.keys())):
                for init in sorted(list(outs[streamid].keys())):
                    writerout.writerow(outs[streamid][init])
        with open(sys.argv[1]+"-process.csv", 'a', newline='') as fprocess:
            writerprocess = csv.writer(fprocess, delimiter=',',
                                  quotechar='"', quoting=csv.QUOTE_MINIMAL)
            for processstage in processes:
                writerprocess.writerow(processstage)
        with open(sys.argv[1]+"-queue.csv", 'a', newline='') as fqueue:
            writerqueue = csv.writer(fqueue, delimiter=',',
                                       quotechar='"', quoting=csv.QUOTE_MINIMAL)
            for queuestage in queues:
                writerqueue.writerow(queuestage)
    return

# def remove_old_active_path(active_paths, new_key):
#     for key in active_paths:
#         if key in new_key:
#             active_paths.pop(key, None)
#             break
#     return

if __name__ == '__main__':
    main()