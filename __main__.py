from __future__ import print_function
import sys

import sparkpickle


def main():
    with open(sys.argv[1], "rb") as fin:
        i = 0
        t = None
        for obj in sparkpickle.load_gen(fin):
            t = type(obj)
            print(obj)
            i += 1
        print("-" * 80)
        print("Overall: %d objects of type %s" % (i, t))

if __name__ == "__main__":
    sys.exit(main())
