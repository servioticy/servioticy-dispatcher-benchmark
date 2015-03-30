import sys
from benchmark import setup

def main():
    for i in range(int(sys.argv[1]), int(sys.argv[2])+1):
        for j in range(int(sys.argv[3]), int(sys.argv[4])+1):
            su = setup.Setup('../benchmark.ini', deploy=True, sos=str(i), isos=str(j))
            su.write_initial_streams('../' + str(i) + '-'+str(j)+'streams.json')
    return

if __name__ == '__main__':
    main()