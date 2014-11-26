import sys
import csv

def main():
    active_paths = {}
    with open(sys.argv[1], 'r', newline='') as f:

        reader1 = csv.reader(f, delimiter=',', quotechar='"')

        with open(sys.argv[2], 'a', newline='') as f2:
            writer = csv.writer(f2, delimiter=',',
                                quotechar='"', quoting=csv.QUOTE_MINIMAL)
            for row in reader1:
                key = ','.join(row[5:])

                if not (row[3] == 'no-group' or row[3] == 'no-stream'):
                    writer.writerow(row)
                    remove_old_active_path(active_paths, key)
                    continue

                if active_paths.get(key) != None and active_paths.get(key) != row[3]:
                    row[3] = 'sink'
                    writer.writerow(row)
                    active_paths.pop(key, None)
                    continue

                if active_paths.get(key) == row[3]:
                    continue

                remove_old_active_path(active_paths, key)
                active_paths[key] = row[3]

    return

def remove_old_active_path(active_paths, new_key):
    for key in active_paths:
        if key in new_key:
            active_paths.pop(key, None)
            break
    return

if __name__ == '__main__':
    main()