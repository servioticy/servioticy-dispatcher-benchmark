import sys
import csv

def main():
    lastUpdate = 0
    lastNStreams = 0
    waits = []
    exec_times = []
    with open(sys.argv[1], 'r', newline='') as f:

        reader1 = csv.reader(f, delimiter=',', quotechar='"')

        with open(sys.argv[2], 'a', newline='') as f2:
            writer = csv.writer(f2, delimiter=',',
                                quotechar='"', quoting=csv.QUOTE_MINIMAL)
            for row in reader1:
                if lastNStreams == 0:
                    lastNStreams = row[4]
                if lastNStreams == row[4]:
                    exec_times.append(int(row[2])-int(row[5]))
                else:
                    new_row = [str(int(lastNStreams)-1), sum(exec_times)/len(exec_times)]
                    writer.writerow(new_row)
                    exec_times = []
                    lastNStreams = row[4]

                if lastUpdate != 0:
                    waits.append(int(row[1]) - int(lastUpdate))
                lastUpdate = row[1]

            new_row = [lastNStreams, sum(exec_times)/len(exec_times)]
            writer.writerow(new_row)
            print(str(1000/(sum(waits)/len(waits))) + ' req/s\n')


    return

if __name__ == '__main__':
    main()