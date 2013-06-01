__author__ = 'Zephyre'

import common
import samsonite

if __name__ == "__main__":
    entries = samsonite.fetch()
    for store in entries:
        print store