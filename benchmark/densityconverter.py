import sys
import csv

def main():
    with open(sys.argv[1], 'r', newline='') as f:

        reader = csv.reader(f, delimiter=',', quotechar='"')

        with open('new.'+sys.argv[1], 'a', newline='') as f2:
            writer = csv.writer(f2, delimiter=',',
                                quotechar='"', quoting=csv.QUOTE_MINIMAL)
            first = True
            for row in reader:
                if first:
                    writer.writerow(row)
                    first = False
                    continue
                row[4] = float(row[5])/(float(row[8])*(float(row[2])-float(row[8])))
                writer.writerow(row)

    return


if __name__ == '__main__':
    main()