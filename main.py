# coding=utf-8
import json
import random
import re
import string
import time
import traceback
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


def dump_geo():
    db = common.StoresDb()
    db.connect_db(passwd='123456')

    print('Clearing data table...')
    db.execute('DELETE FROM country')

    # print('Writing continent records...')
    #
    # print('Writing country records...')
    # for val in gs.country_map['data'].values():
    #     try:
    #         name_e = val['name_e']
    #         name_c = val['name_c']
    #         code = val['code']
    #         iso3_code = val['iso3'] if 'iso3' in val else None
    #
    #         guid = val['continent']
    #         continent = gs.continent_map['data'][guid]['name_e']
    #         ret = db.query_all("SELECT idcontinent FROM continent WHERE name_e='%s'" % continent)
    #         if len(ret) != 1:
    #             print('Error in fetching continent %s' % continent)
    #             continue
    #         idcontinent = string.atoi(ret[0][0])
    #
    #         if iso3_code:
    #             statement = "INSERT INTO country (idcontinent, continent, code, iso3_code, name_e, name_c) VALUES (%d, '%s', '%s', '%s', '%s', '%s')" % (
    #                 idcontinent, continent, code, iso3_code, name_e, name_c)
    #         else:
    #             statement = "INSERT INTO country (idcontinent, continent, code, name_e, name_c) VALUES (%d, '%s', '%s', '%s', '%s')" % (
    #                 idcontinent, continent, code, name_e, name_c)
    #
    #         if code == '':
    #             print('\n%s' % statement)
    #             continue
    #
    #         db.execute(statement)
    #     except KeyError as e:
    #         print traceback.format_exc()
    #         continue

    print('Writing city records...')

    print('Done!')
    db.disconnect_db()


def test():
    dump_geo()


if __name__ == "__main__":
    test_flag = True
    # passwd = 'rose123'
    passwd = '123456'
    if test_flag:
        test()
    else:
        tommy_global.fetch(passwd=passwd)
        # bershka.fetch(passwd=passwd)