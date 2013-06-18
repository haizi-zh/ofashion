# coding=utf-8
import json
import re
import time
import adidas
import balenciaga
import baume
import benetton
import canali
import cerruti
import christofle

import common
import debeers
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
import juicycouture
import kenzo
import kipling
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
import samsonite
import sergio
import shanghaitang
import shanghaivive
import triumph
import tudor
import unode50
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
    common.extract_tel('HAUPTSTRASSE 197, 3034, ANZBACH/ UNTEROBERNDORF, 0043 2772 52530')


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
        unode50.fetch(passwd=passwd)
        print 'DONE!'
