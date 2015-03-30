import operator
import sys
import csv

# The file needs to be already sorted by first appearance

def main():
    with open(sys.argv[2], 'r', newline='') as f:
        su_rows = []
        reader1 = csv.reader(f, delimiter=',', quotechar='"')
        sortedcsv = sorted(reader1, key=operator.itemgetter(1), reverse=False)

        with open(sys.argv[3], 'a', newline='') as f2:
            writer = csv.writer(f2, delimiter=',',
                                quotechar='"', quoting=csv.QUOTE_MINIMAL)
            lastUpdate = 0
            first_time = 0;
            waits = []
            for row in sortedcsv:
                if first_time == 0:
                    first_time = row[5]
                if len(su_rows) == 0 or su_rows[-1][0] == row[0]:
                    su_rows.append(row)
                    continue
                if len(su_rows) == int(sys.argv[1]):
                    new_row = [0,0]
                    for su_row in su_rows:
                        i = 5
                        while i + 3 < len(su_row) and su_row[i+3] != '':
                            new_row[0] = int(su_row[i+3]) - int(first_time)
                            new_row[1] = int(su_row[i+3]) - int(su_row[i])
                            writer.writerow(new_row)
                            i = i+3
                    if lastUpdate != 0:
                        waits.append(int(su_row[1]) - int(lastUpdate))
                    lastUpdate = su_row[1]

                su_rows = []
                su_rows.append(row)
            print(str(1000/(sum(waits)/len(waits))) + ' req/s\n')

    return

if __name__ == '__main__':
    main()