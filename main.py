# coding=utf-8
import json
import baume
import cerruti
import christofle

import common
import geosense as gs
import dunhill
import emiliopucci
import kenzo
import louboutin
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
    entry = gs.look_up(u'CHICAGO', 3)
    entry = gs.look_up(u'武汉', 3)
    entry = gs.look_up(u'CHICAGO', 3)

    pass
    # gs.load_geo()
    # gs.add_entries(gs.continent_map, gs.country_map, gs.province_map, gs.city_map)
    # new_map={}
    # for c in gs.continent_map:
    #     c1 = gs.continent_map[c]
    #     if common.is_chinese(c):
    #         new_map[c]={common.continent_c:c,common.continent_e:c1}
    #     else:
    #         new_map[c]={common.continent_e:c,common.continent_c:c1}
    # js=json.dumps(new_map,ensure_ascii=False)
    # with open('new_con.dat', 'w') as f:
    #     f.write(js.encode('utf-8'))
    # return new_map
    # gs.add_entries(gs.continent_map, gs.country_map, gs.province_map, gs.city_map)
    # for city in gs.city_map
    # return []


if __name__ == "__main__":
    test_flag = False
    passwd = '07996019'
    if test_flag:
        test()
    else:
        # zenithwatch.fetch()
        viktor_rolf.fetch(passwd=passwd)
        # shanghaitang.fetch()
        # emiliopucci.fetch()
        # zegna.fetch()
        # y3.fetch()
        # dunhill.fetch(passwd='07996019')
        # baume.fetch(passwd='07996019')
        # comme_des_garcons.fetch(passwd='07996019')
        # louboutin.fetch(passwd)
        # cerruti.fetch(passwd)
        # christofle.fetch(passwd=passwd)
        # kenzo.fetch(passwd)
        print 'DONE!'