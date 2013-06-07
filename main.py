# coding=utf-8

import common
from robot import viktor_rolf

__author__ = 'Zephyre'


def calc(val):
    a = (0.1, 0.15, 0.4, 0.1, 0.15, 0.1)
    tot = map(lambda x, y: x * y, a, val)
    return sum(tot)


def test():
    common.load_rev_char()
    print common.html2plain('&lt;')

if __name__ == "__main__":
    # entries = donna_karan.fetch('dnky')
    # entries = donna_karan.fetch('donnakaran')
    # entries = debeers.fetch()
    # entries = samsonite.fetch()
    viktor_rolf.fetch()
    # zegna.fetch()
    # ysl.fetch()
    # shanghaitang.fetch()
    # db_test()

    print 'DONE!'