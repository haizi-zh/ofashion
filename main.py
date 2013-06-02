import debeers
import common

__author__ = 'Zephyre'


if __name__ == "__main__":
    entries = debeers.fetch()
    # print('%d stores found.'%entries.__len__())
    # for c in entries:
    #     print c