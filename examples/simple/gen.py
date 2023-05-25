"""
generator script for simple pipeline example:
feeds an endless sequence of lists of numbers
"""
import sys
import time

# app:
from pipeline.worker import Worker, run

class Gen(Worker):
    """
    example data source worker
    """

    def main_loop(self, conn, chan):
        n = 0

        while True:
            # create lists of 5 numbers
            # no doubt there's a more pythonic way to do this!
            l = []
            for i in range(0,5):
                l.append(n)
                n += 1
            print(l)

            print("sending...")
            self.send_items(chan, l)

            print("sleeping...")
            sys.stdout.flush()
            time.sleep(1)


if __name__ == '__main__':
    run(Gen, "simple-gen", "generator for simple pipeline")
