import os
import sys
from benchmark import visualization
from benchmark import setup
import csv

def main():
    for i in range(int(sys.argv[1]), int(sys.argv[2])+1):
        sos = str(i)
        su = setup.Setup('../benchmark.ini', True, sos)
        su.write_initial_streams('../' + str(sos) + 'streams.json')

    return

if __name__ == '__main__':
    main()