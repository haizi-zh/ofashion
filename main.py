# coding=utf-8
import json
import re
import time
import adidas
import baume
import cerruti
import christofle

import common
import donna_karan
import escada
import esprit
import etro
import fcuk
import folli
import furla
import geosense as gs
import dunhill
import emiliopucci
import gilsander
import hamilton
import hublot
import jimmy_choo
import kenzo
import levis
import louboutin
import lukfook
import marni
import maurice_lacroix
import michael_kors
import mido
import missoni
import ninewest
import oasis
import oris
import paul_joe
import paulshark
import paulsmith
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
    country_info = gs.look_up('GERMANY', 1)

    with open('sample.txt', 'r') as f:
        for line in f.readlines():
            line=line.decode('utf-8')
            terms = line.split(',')
            city = terms[1].strip().upper()
            country = terms[2].strip().upper()

            gs.update_city_map(city, country, 'EUROPE')

    # s = json.dumps(gs.province_map).encode('utf-8')
    # with open('data/province_map.dat', 'w') as f:
    #     f.write(s)
    #
    s = json.dumps(gs.city_map).encode('utf-8')
    with open('data/city_map.dat', 'w') as f:
        f.write(s)


if __name__ == "__main__":
    test_flag = False
    passwd = '123456'
    if test_flag:
        test()
    else:
        # oris.fetch(passwd=passwd)
        # paul_joe.fetch(passwd=passwd)
        # oasis.fetch(passwd=passwd)
        # paulshark.fetch(passwd=passwd)
        # paulsmith.fetch(passwd=passwd)
        # adidas.fetch(passwd=passwd)
        # furla.fetch(passwd=passwd)
        # hamilton.fetch(passwd=passwd)
        # hublot.fetch(passwd=passwd)
        # escada.fetch(passwd=passwd)
        # esprit.fetch(passwd=passwd)
        jimmy_choo.fetch(passwd=passwd)
        print 'DONE!'
