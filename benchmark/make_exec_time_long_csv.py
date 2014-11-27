import sys
import csv

def main():
    with open(sys.argv[1], 'r', newline='') as f:

        reader1 = csv.reader(f, delimiter=',', quotechar='"')

        with open(sys.argv[2], 'a', newline='') as f2:
            writer = csv.writer(f2, delimiter=',',
                                quotechar='"', quoting=csv.QUOTE_MINIMAL)
            for row in reader1:
                new_row = [row[4], int(row[2])-int(row[5])]
                writer.writerow(new_row)

    return

if __name__ == '__main__':
    main()