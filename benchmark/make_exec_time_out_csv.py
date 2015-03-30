import operator
import sys
import csv

def main():
    lastUpdate = 0
    waits = []
    with open(sys.argv[1], 'r', newline='') as f:

        reader1 = csv.reader(f, delimiter=',', quotechar='"')
        sortedcsv = sorted(reader1, key=operator.itemgetter(1), reverse=False)

        with open(sys.argv[2], 'a', newline='') as f2:
            writer = csv.writer(f2, delimiter=',',
                                quotechar='"', quoting=csv.QUOTE_MINIMAL)
            su_rows = []
            partial_avgs = []
            for row in sortedcsv:
                if len(su_rows) == 0 or su_rows[-1][0] == row[0]:
                    su_rows.append(row)
                    continue

                times = []
                for su_row in su_rows:
                    times.append(int(su_row[2]) - int(su_row[5]))
                partial_avgs.append([len(su_rows),sum(times)/len(times)])
                su_rows = []
                su_rows.append(row)
                if lastUpdate != 0:
                    waits.append(int(row[1]) - int(lastUpdate))
                lastUpdate = row[1]
            times = []

            for su_row in su_rows:
                times.append(int(su_row[2]) - int(su_row[5]))
            partial_avgs.append([len(su_rows),sum(times)/len(times)])
            su_rows = []

            partial_avgs = sorted(partial_avgs, key=operator.itemgetter(0), reverse=False)

            lastNStreams = 0
            exec_times = []
            for p_avg in partial_avgs:
                if lastNStreams == 0:
                    lastNStreams = p_avg[0]
                if lastNStreams == p_avg[0]:
                    exec_times.append(p_avg[1])
                else:
                    new_row = [str(lastNStreams), sum(exec_times)/len(exec_times)]
                    writer.writerow(new_row)
                    exec_times = []
                    lastNStreams = p_avg[0]
            new_row = [str(lastNStreams), sum(exec_times)/len(exec_times)]
            writer.writerow(new_row)
            print(str(1000/(sum(waits)/len(waits))) + ' req/s\n')

    return

if __name__ == '__main__':
    main()