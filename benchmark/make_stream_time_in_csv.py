import operator
import sys
import csv
from subprocess import PIPE, Popen

# The file needs to be already sorted by first appearance

def main():
    with open(sys.argv[2], 'r', newline='') as f:
        su_rows = []
        reader1 = csv.reader(f, delimiter=',', quotechar='"')
        sortedcsv = sorted(reader1, key=operator.itemgetter(1), reverse=False)

        with open(sys.argv[3], 'a', newline='') as f2:
            writer = csv.writer(f2, delimiter=',',
                                quotechar='"', quoting=csv.QUOTE_MINIMAL)
            for row in sortedcsv:
                if len(su_rows) == 0 or su_rows[-1][-2] == row[-2]:
                    su_rows.append(row)
                    continue
                proc = Popen('/bin/grep -lir \"'+su_rows[0][6]+'\" ../streams-in/', stdout=PIPE, stderr=PIPE, shell=True)
                l = format(proc.communicate()).split("../streams-in/1-")[1].split("streams.json")[0]
                if int(l) == int(sys.argv[1]):
                    new_row = [0,0]
                    for su_row in su_rows:
                        i = 5
                        while i + 3 < len(su_row) and su_row[i+3] != '':
                            new_row[0] = int(su_row[i+3])
                            new_row[1] = int(su_row[i+3]) - int(su_row[i])
                            writer.writerow(new_row)
                            i = i+3
                su_rows = []
                su_rows.append(row)
            proc = Popen('/bin/grep -lir \"'+su_rows[0][6]+'\" ../streams-in/', stdout=PIPE, stderr=PIPE, shell=True)
            l = format(proc.communicate()).split("../streams-in/1-")[1].split("streams.json")[0]
            if int(l) == int(sys.argv[1]):
                new_row = [0,0]
                for su_row in su_rows:
                    i = 5
                    while i + 3 < len(su_row) and su_row[i+3] != '':
                        new_row[0] = int(su_row[i+3])
                        new_row[1] = int(su_row[i+3]) - int(su_row[i])
                        writer.writerow(new_row)
                        i = i+3

    return

if __name__ == '__main__':
    main()