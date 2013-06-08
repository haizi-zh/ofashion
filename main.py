# coding=utf-8
import baume

import common
import geosense as gs
import dunhill
import emiliopucci
import shanghaitang
import viktor_rolf
import y3
import zegna
import zenithwatch
import comme_des_garcons

__author__ = 'Zephyre'


def calc(val):
    a = (0.1, 0.15, 0.4, 0.1, 0.15, 0.1)
    tot = map(lambda x, y: x * y, a, val)
    return sum(tot)


def test():
    gs.load_geo()
    gs.test(gs.continent_map, gs.country_map, gs.province_map, gs.city_map)
    return []


if __name__ == "__main__":
    test_flag = True
    if test_flag:
        test()
    else:
        # zenithwatch.fetch()
        # viktor_rolf.fetch()
        # shanghaitang.fetch()
        # emiliopucci.fetch()
        # zegna.fetch()
        # y3.fetch()
        # dunhill.fetch(passwd='07996019')
        # baume.fetch(passwd='07996019')
        comme_des_garcons.fetch(passwd='07996019')
        print 'DONE!'