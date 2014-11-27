import sys
import csv

def main():
    with open(sys.argv[1], 'r', newline='') as f:

        reader1 = csv.reader(f, delimiter=',', quotechar='"')

        with open(sys.argv[2], 'a', newline='') as f2:
            writer = csv.writer(f2, delimiter=',',
                                quotechar='"', quoting=csv.QUOTE_MINIMAL)
            su_rows = []
            for row in reader1:
                if len(su_rows) == 0:
                    su_rows.append(row)
                    continue
                if su_rows[-1][0] != row[0]:
                    # Do mean

                new_row = [0,0]
                i = 5
                while i + 3 < len(row):
                    new_row[0] = row[i+3]
                    new_row[1] = int(row[i+3]) - int(row[i])
                    writer.writerow(new_row)
                    i = i+3

    return

if __name__ == '__main__':
    main()