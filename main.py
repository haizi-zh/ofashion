# coding=utf-8
import json
import baume
import cerruti
import christofle

import common
import donna_karan
import geosense as gs
import dunhill
import emiliopucci
import kenzo
import louboutin
import shanghaitang
import viktor_rolf
import y3
import ysl
import zegna
import zenithwatch
import comme_des_garcons

__author__ = 'Zephyre'


def calc(val):
    a = (0.1, 0.15, 0.4, 0.1, 0.15, 0.1)
    tot = map(lambda x, y: x * y, a, val)
    return sum(tot)


def test():
    for key in gs.province_map['data']:
        prov = gs.province_map['data'][key]
        if prov['code'] != '':
            gs.province_map['lookup'][prov['code']] = key

    gs.commit_maps(2)

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
    passwd = '123456'
    if test_flag:
        test()
    else:
        # donna_karan.fetch(passwd=passwd)
        # ysl.fetch(passwd=passwd)
        zenithwatch.fetch(passwd=passwd)
        # viktor_rolf.fetch(passwd=passwd)
        # shanghaitang.fetch(passwd=passwd)
        # emiliopucci.fetch(passwd=passwd)
        # zegna.fetch(passwd=passwd)
        # y3.fetch(passwd=passwd)
        # dunhill.fetch(passwd=passwd)
        # baume.fetch(passwd='07996019')
        # comme_des_garcons.fetch(passwd=passwd)
        # louboutin.fetch(passwd=passwd)
        # cerruti.fetch(passwd=passwd)
        # christofle.fetch(passwd=passwd)
        # kenzo.fetch(passwd=passwd)
        print 'DONE!'