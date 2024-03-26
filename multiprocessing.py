#!/usr/bin/env python
import multiprocessing
import time

def do_nothing():
    for i in range(100_000_000):
        i += 1
    print(i)

def main():
    start_time = time.time()
    process1 = multiprocessing.Process(target=do_nothing)
    process2 = multiprocessing.Process(target=do_nothing)
    process1.start()
    process2.start()
    process1.join()
    process2.join()
    end_time = time.time()
    print(f"Elapsed time: {end_time - start_time}")

if __name__ == "__main__":
    main()

