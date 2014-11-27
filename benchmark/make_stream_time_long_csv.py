import sys
import csv

def main():
    lastUpdate = 0
    waits = []
    with open(sys.argv[2], 'r', newline='') as f:

        reader1 = csv.reader(f, delimiter=',', quotechar='"')

        with open(sys.argv[3], 'a', newline='') as f2:
            writer = csv.writer(f2, delimiter=',',
                                quotechar='"', quoting=csv.QUOTE_MINIMAL)
            for row in reader1:
                if sys.argv[1] != row[4]:
                    continue
                new_row = [0,0]
                i = 5
                while i + 3 < len(row) and row[i+3] != '':
                    new_row[0] = row[i+3]
                    new_row[1] = int(row[i+3]) - int(row[i])
                    writer.writerow(new_row)
                    i = i+3
                if lastUpdate != 0:
                    waits.append(int(row[1]) - int(lastUpdate))
                lastUpdate = row[1]
            print(str(1000/(sum(waits)/len(waits))) + ' req/s\n')

    return

if __name__ == '__main__':
    main()