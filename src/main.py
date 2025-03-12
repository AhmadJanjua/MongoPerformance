from helper import *
import time

if __name__ == '__main__':
    start = time.time_ns()
    genUnStructData(10_000_000)
    end = time.time_ns()
    print((end - start) / 10**9)