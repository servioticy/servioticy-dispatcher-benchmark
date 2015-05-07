import sys
import csv
import statistics

def main():
    stream_execs_out = {}
    stream_execs_in = {}
    in_degrees = {}
    out_degrees = {}

    with open(sys.argv[3], 'r', newline='') as f:
        reader1 = csv.reader(f, delimiter=',', quotechar='"')
        for row in reader1:
            in_degrees[row[0]] = int(row[1])
    with open(sys.argv[4], 'r', newline='') as f:
        reader1 = csv.reader(f, delimiter=',', quotechar='"')
        for row in reader1:
            out_degrees[row[0]] = int(row[1])
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
                    if not streamid in stream_execs_out.keys():
                        stream_execs_in[streamid] = {}
                        stream_execs_out[streamid] = {}
                    if not startts in stream_execs_out[streamid].keys():
                        stream_execs_in[streamid][startts] = 99999999
                        stream_execs_out[streamid][startts] = 0 #{}
                    # if not stopts in stream_execs[streamid][startts].keys():
                    #     stream_execs[streamid][startts][stopts] = 0
                    # Time taken to serve the last output for each input
                    # stream_execs[streamid][startts][stopts] = int(stopts) - int(startts)
                    if int(stopts) - int(startts) > stream_execs_out[streamid][startts]:
                        stream_execs_out[streamid][startts] = int(stopts) - int(startts)
                    if int(stopts) - int(startts) < stream_execs_in[streamid][startts]:
                        stream_execs_in[streamid][startts] = int(stopts) - int(startts)

            for streamid in sorted(list(stream_execs_out.keys())):
                for startts in sorted(list(stream_execs_out[streamid].keys())):
                    # stream_execs_out[streamid] = [statistics.mean(list(stream_execs_in[streamid].values())),statistics.mean(list(stream_execs_out[streamid].values()))]
                    writer.writerow([streamid, stream_execs_in[streamid][startts], stream_execs_out[streamid][startts], in_degrees[streamid], out_degrees[streamid]])
            # for streamid in sorted(stream_execs.keys()):
            #     for startts in sorted(stream_execs[streamid].keys()):
            #         stream_execs[streamid][startts] = statistics.mean(list(stream_execs[streamid][startts].values()))
            #     stream_execs[streamid]= statistics.mean(list(stream_execs[streamid].values()))
            #     writer.writerow([streamid, stream_execs[streamid]])


    return

# def remove_old_active_path(active_paths, new_key):
#     for key in active_paths:
#         if key in new_key:
#             active_paths.pop(key, None)
#             break
#     return

if __name__ == '__main__':
    main()