# coding=utf-8
import json
import random
import re
import time
import adidas
import agnesb
import alexander_mcqueen
import armani
import audemars
import balenciaga
import bally
import baume
import benetton
import bershka
import blancpain
import bottega_veneta
import boucheron
import bulgari
import burberry
import canali
import cartier
import cartier_old
import cerruti
import christofle

import common
import constantin
import debeers
import dkny
import donna_karan
import escada
import esprit
import etro
import fcuk
import ferragamo
import folli
import furla
import geosense as gs
import dunhill
import emiliopucci
import gilsander
import gucci
import hamilton
import hermes
import hm
import hublot
import hugoboss
import issey_miyake
import iwc
import jaeger_lecoultre
import jimmy_choo
import juicycouture
import kenzo
import kipling
import lacoste
import langines
import lanvin
import levis
import liujo
import loewe
import louboutin
import lukfook
import mango
import marc_jacobs
import marni
import maurice_lacroix
import max_co
import maxmara
import michael_kors
import mido
import missoni
import misssixty
import miumiu
import montblanc
import movado
import movado_cn
import mulberry
import nike
import ninewest
import oasis
import omega
import oris
import patek
import paul_joe
import paulshark
import paulsmith
import prada
import rado
import robertocavalli
import rolex
import samsonite
import sergio
import shanghaitang
import shanghaivive
import stella_mccartney
import swatch
import tagheuer
import tiffany
import tod
import tommy
import triumph
import tudor
import unode50
import valentino
import van_cleef
import vera_wang
import versace
import victoriassecret
import viktor_rolf
import vivienne
import y3
import ysl
import zara
import zegna
import zenithwatch
import comme_des_garcons

__author__ = 'Zephyre'


def calc(val):
    a = (0.1, 0.15, 0.4, 0.1, 0.15, 0.1)
    tot = map(lambda x, y: x * y, a, val)
    return sum(tot)


def test():
    brand = '''<ul class="brand main">
<li><a href="#/" title="ALL">ALL</a></li>
<li ><a href="#/IM" title="ISSEY MIYAKE">ISSEY MIYAKE</a></li>
<li><a href="#/ME" title="ISSEY MIYAKE MEN">ISSEY MIYAKE MEN</a></li>
<li class="large"><a href="#/PL" title="PLEATS PLEASE ISSEY MIYAKE">PLEATS PLEASE<br> ISSEY MIYAKE</a></li>
<li><a href="#/HA" title="HaaT">HaaT</a></li>
<li class="large"><a href="#/MI" title="me ISSEY MIYAKE / CAULIFLOWER">me ISSEY MIYAKE /<br> CAULIFLOWER</a></li>
<li><a href="#/BB" title="BAO BAO ISSEY MIYAKE">BAO BAO ISSEY MIYAKE</a></li>
<li><a href="#/1325" title="132 5. ISSEY MIYAKE">132 5. ISSEY MIYAKE</a></li>
<li class="large"><a href="#/HM" title="HIKARU MATSUMURA / THE UNIQUE-BAG">HIKARU MATSUMURA<br> THE UNIQUE-BAG</a></li>
<li><a href="#/IN-EI" title="132 5. ISSEY MIYAKE">IN-EI ISSEY MIYAKE</a></li>
<!--<li><a href="#/24" title="24 ISSEY MIYAKE">24 ISSEY MIYAKE</a></li>
<li><a href="#/MW" title="ISSEY MIYAKE WATCH">ISSEY MIYAKE WATCH</a></li>
<li><a href="#/MP" title="ISSEY MIYAKE PARFUMS">ISSEY MIYAKE PARFUMS</a></li>-->
</ul>'''
    ret = dict((m[0], m[1]) for m in re.findall(ur'<a href="#/([^"]+)" title="([^"]+)">', brand))
    ret = json.dumps(ret)
    return ret




if __name__ == "__main__":
    test_flag = False
    # passwd = 'rose123'
    passwd = '123456'
    if test_flag:
        test()
    else:
        zara.fetch(passwd=passwd)
        # bershka.fetch(passwd=passwd)