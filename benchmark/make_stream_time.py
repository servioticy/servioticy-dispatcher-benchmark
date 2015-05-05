import sys
import csv
import statistics

def main():
    stream_execs = {}
    positions = {}
    with open(sys.argv[1], 'r', newline='') as f:

        reader1 = csv.reader(f, delimiter=',', quotechar='"')

        with open(sys.argv[2], 'a', newline='') as f2:
            writer = csv.writer(f2, delimiter=',',
                                quotechar='"', quoting=csv.QUOTE_MINIMAL)
            for row in reader1:
                for i in range(int((len(row)-5)/3)):
                    if row[5+i*3] == '':
                        break
                    streamid = row[5+i*3+1] + ':' + row[5+i*3+2]
                    startts = row[5+i*3]
                    stopts = row[2] if i == int((len(row)-5)/3)-1 or row[5+i*3+3] == '' else row[5+i*3+3]
                    if not streamid in stream_execs.keys():
                        stream_execs[streamid] = {}
                        positions[streamid] = {}
                    if not startts in stream_execs[streamid].keys():
                        stream_execs[streamid][startts] = 0
                        positions[streamid][startts] = 0
                    # Time taken to serve the last output for each input
                    if int(stopts) - int(startts) > stream_execs[streamid][startts]:
                        stream_execs[streamid][startts] = int(stopts) - int(startts)
                        positions[streamid][startts] = i
            for streamid in sorted(stream_execs.keys()):
                stream_execs[streamid] = [statistics.mean(list(stream_execs[streamid].values())), statistics.stdev(list(stream_execs[streamid].values()))]
                writer.writerow([streamid, stream_execs[streamid][0], stream_execs[streamid][1], positions[streamid]])


    return

# def remove_old_active_path(active_paths, new_key):
    for key in active_paths:
        if key in new_key:
            active_paths.pop(key, None)
            break
    return

if __name__ == '__main__':
    main()