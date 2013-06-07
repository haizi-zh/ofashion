# coding=utf-8

import common
from robot import viktor_rolf
from robot import emiliopucci
from robot import y3
from robot import zenithwatch
import shanghaitang

__author__ = 'Zephyre'


def calc(val):
    a = (0.1, 0.15, 0.4, 0.1, 0.15, 0.1)
    tot = map(lambda x, y: x * y, a, val)
    return sum(tot)


def test():
    common.geo_translate(u'美国')
    common.geo_translate('united states of america')
    common.geo_translate('usa')
    return []


if __name__ == "__main__":
    test_flag = False
    if test_flag:
        test()
    else:
        shanghaitang.fetch()
        print 'DONE!'