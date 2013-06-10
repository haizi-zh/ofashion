# coding=utf-8
import json
import re
import time
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
import sergio
import shanghaitang
import triumph
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
    # with open('sample.txt', 'r') as f:
    #     for line in f.readlines():
    #         l = line.strip().decode('utf-8')
    #         m = re.match(ur'^([A-Z]{2})\s+,', l)
    #         if l == '' or m is None:
    #             continue
    #
    #         code = m.group(1)
    #         country = l.split(',')[1].strip().upper()
    #
    #         if country not in gs.country_map['lookup']:
    #             print 'Failed to lookup (%s %s)' % (code, country)
    #             continue
    #         guid = gs.country_map['lookup'][country]
    #         gs.country_map['data'][guid]['code'] = code
    #         gs.country_map['lookup'][code] = guid
    #
    # gs.commit_maps(1)

    pass


if __name__ == "__main__":
    test_flag = False
    passwd = '123456'
    if test_flag:
        test()
    else:
        # donna_karan.fetch(passwd=passwd)
        # ysl.fetch(passwd=passwd)
        # zenithwatch.fetch(passwd=passwd)
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
        # triumph.fetch(passwd=passwd)
        sergio.fetch(passwd=passwd)
        print 'DONE!'