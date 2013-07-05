# coding=utf-8
import json
import random
import re
import time
import adidas
import agnesb
import alexander_mcqueen
import alexander_wang
import giorgio_armani
import armani_exchange
import audemars
import balenciaga
import bally
import baume
import benetton
import bershka
import blancpain
import bottega_veneta
import boucheron
import breguet
import breitling
import bulgari
import burberry
import canali
import cartier
import cartier_old
import celine
import cerruti
import chanel
import chaumet
import chloe
import chopard
import christofle
import coach
import columbia

import common
import constantin
import debeers
import diesel
import dior
import dkny
import dolce_gabbana
import donna_karan
import ecco
import escada
import esprit
import etro
import fcuk
import fendi
import ferragamo
import folli
import furla
import geosense as gs
import dunhill
import emiliopucci
import gilsander
import gucci
import hamilton
import hamilton_global
import hermes
import hm
import hublot
import hugoboss
import hushpuppies
import issey_miyake
import iwc
import jaeger_lecoultre
import jimmy_choo
import juicycouture
import kenzo
import kipling
import lacoste
import levis_eu
import levis_us
import longines
import lanvin
import lee
import levis
import liujo
import loewe
import louboutin
import louis_vuitton
import louis_vuitton_3rd
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
import miss_sixty
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
import samsonite_global
import sergio
import shanghaitang
import shanghaivive
import sisley
import stella_mccartney
import swarovski
import swatch
import tagheuer
import tiffany
import tod
import tommy
import tommy_global
import triumph
import trussardi
import tsl
import tudor
import unode50
import us_postal
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
    pass


if __name__ == "__main__":
    test_flag = False
    # passwd = 'rose123'
    passwd = '123456'
    if test_flag:
        test()
    else:
        louis_vuitton.fetch(passwd=passwd)
        # bershka.fetch(passwd=passwd)