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


def sense_cities(lower_bound='a', upper_bound='b'):
    """
    规则化城市字段
    """

    def register_city(geocoded_info):
        for geo_info in geocoded_info:
            admin_info = geo_info['administrative_info']
            if 'country' not in admin_info:
                common.dump(u'Country info does not exist: %s' % admin_info)
                continue

            if 'locality' in admin_info:
                city = admin_info['locality']
            elif 'sublocality' in admin_info:
                city = admin_info['sublocality']
            elif 'administrative_area_level_3' in admin_info:
                city = admin_info['administrative_area_level_3']
            elif 'administrative_area_level_2' in admin_info:
                city = admin_info['administrative_area_level_2']
            else:
                common.dump(u'City info does not exist: %s' % admin_info)
                continue

            # 检验一致性，国家或城市信息必须一致
            ret1 = gs.look_up(country_e, 1)
            ret2 = gs.look_up(admin_info['country'], 1)
            if not ret1 or not ret2:
                common.dump(u'Failed in country test: %s vs %s.' % (country_e, admin_info['country']), log_name)
                continue
            elif ret1['name_e'] != ret2['name_e']:
                ret3 = gs.look_up(city_e, 1)
                ret4 = gs.look_up(city, 1)
                if (ret3['name_e'] if ret3 else city_e) != (ret4['name_e'] if ret4 else city):
                    common.dump(u'Countries or cities does not match.', log_name)
                    continue

            # 登记城市标准化信息
            std_info = {'city_e': city, 'country_e': admin_info['country']}
            if 'administrative_area_level_1' in admin_info:
                std_info['region_e'] = admin_info['administrative_area_level_1']
            else:
                std_info['region_e'] = ''

            # 获得中文信息
            std_info['country_c'] = ''
            std_info['region_c'] = ''
            std_info['city_c'] = ''
            geocoded_info_zh = gs.geocode(addr=geo_info['formatted_address'], lang='zh')
            if geocoded_info_zh:
                admin_info_zh = geocoded_info_zh[0]['administrative_info']
                if 'country' in admin_info_zh:
                    std_info['country_c'] = admin_info_zh['country']
                if 'locality' in admin_info_zh:
                    std_info['city_c'] = admin_info_zh['locality']
                elif 'sublocality' in admin_info_zh:
                    std_info['city_c'] = admin_info_zh['sublocality']
                elif 'administrative_area_level_3' in admin_info_zh:
                    std_info['city_c'] = admin_info_zh['administrative_area_level_3']
                elif 'administrative_area_level_2' in admin_info_zh:
                    std_info['city_c'] = admin_info_zh['administrative_area_level_2']
                if 'administrative_area_level_1' in admin_info_zh:
                    std_info['region_c'] = admin_info_zh['administrative_area_level_1']

            std_sig = u'|'.join((std_info['city_e'], std_info['region_e'], std_info['country_e']))
            city_std[sig] = {'std_sig': std_sig}
            if 'std_sig' not in city_std:
                city_std[std_sig] = {'std_info': std_info, 'geo_info': geo_info}

            # common.dump(u'Standard: %s' % std_info)
            common.dump(u'%s => %s' % (sig, std_sig))
            return True
        return False

    city_std = {}
    log_name = u'sense_cities.log'
    try:
        with open('data/city_std.dat', 'r') as f:
            # {'city|region|country':{'std_info':{'city':...,'region':...,'country':...}, 'geo_result': result}}
            # 城市的标准化映射信息
            city_std = json.loads(f.readlines()[0])
    except IOError:
        common.dump(u'Failed to load data/city_std.dat', log_name)

    db = common.StoresDb()
    db.connect_db(host='localhost', port=3306, user='root', passwd='123456', db='brand_stores')
    tpl_entity = "SELECT DISTINCT city_e, province_e, country_e FROM stores WHERE city_e>'%s' AND city_e<'%s' AND is_geocoded!=4 AND is_geocoded!=5 AND is_geocoded!=6 ORDER BY city_e, province_e, country_e LIMIT 99999"
    tpl_pos = "SELECT lat, lng, addr_e, idstores FROM stores WHERE city_e='%s' AND province_e='%s' AND country_e='%s' LIMIT 99999"
    tpl_geocoded = "UPDATE stores SET is_geocoded=%d WHERE city_e='%s' AND province_e='%s' AND country_e='%s' LIMIT 99999"

    statement = tpl_entity % (lower_bound, upper_bound)
    common.dump(u"Processing cities from '%s' to '%s'..." % (lower_bound, upper_bound), log_name)
    for item in db.query_all(statement):
        try:
            sig = u'|'.join(item[i] for i in xrange(3))
            if sig in city_std:
                common.dump(u'Geo item %s already processed.' % sig, log_name)
                continue
            common.dump(u'Processing %s...' % sig, log_name)

            city_e, province_e, country_e = item
            geo_success = False
            statement = tpl_pos % tuple(tmp.replace("'", r"\'") for tmp in item)
            query_result = db.query_all(statement)
            for lat, lng, addr, idstores in query_result:
                if not lat or not lng or lat == '' or lng == '' or (string.atof(lat) == 0 and string.atof(lng) == 0):
                    continue
                    # 使用经纬度进行查询
                tmp = gs.geocode(latlng='%s,%s' % (lat, lng))
                if not tmp:
                    continue
                geo_success = register_city(tmp)
                if geo_success:
                    break
            if geo_success:
                # 通过经纬度获得
                tmp1 = [4]
                tmp1.extend(tmp.replace("'", r"\'") for tmp in item)
                statement = tpl_geocoded % tuple(tmp1)
                db.execute(statement)
            else:
                for lat, lng, addr, idstores in query_result:
                    # 使用地址进行查询
                    tmp = gs.geocode(u'%s,%s,%s' % (city_e, province_e, country_e))
                    if not tmp:
                        continue
                    geo_success = register_city(tmp)
                    if geo_success:
                        break

                    tmp = gs.geocode(addr)
                    if not tmp:
                        continue
                    geo_success = register_city(tmp)
                    if geo_success:
                        break
                if geo_success:
                    # 通过地址成功获得
                    tmp1 = [5]
                    tmp1.extend(tmp.replace("'", r"\'") for tmp in item)
                    statement = tpl_geocoded % tuple(tmp1)
                    db.execute(statement)
                else:
                    # 未能获得
                    tmp1 = [6]
                    tmp1.extend(tmp.replace("'", r"\'") for tmp in item)
                    statement = tpl_geocoded % tuple(tmp1)
                    db.execute(statement)

            with open(u'data/city_std.dat', 'w') as f:
                f.write(json.dumps(city_std).encode('utf-8'))
        except Exception as e:
            common.dump(traceback.format_exc(), log_name)

    common.dump(u'Done!', log_name)


def geo_translate():
    """
    将国家字段进行中英文翻译，并加入坐标信息
    """
    db = common.StoresDb()
    db.connect_db(passwd='123456')
    for item in db.query_all("SELECT * FROM country WHERE name_c=''"):
        idcountry = string.atoi(item[0])
        name_e, name_c, code = (tmp.upper() for tmp in item[3:6])

        raw = json.loads(
            common.get_data(r'http://maps.googleapis.com/maps/api/geocode/json',
                            data={'address': name_e, 'sensor': 'false'},
                            hdr={'Accept-Language': 'en-us,en;q=0.8,zh-cn;q=0.5,zh;q=0.3'}))
        if raw['status'] != 'OK':
            print('Error in %s, reason: %s' % (name_e, raw['status']))
            continue

        info = raw['results'][0]
        # 确保geocode类型为国家
        if 'country' in info['types']:
            name_e = info['address_components'][0]['long_name']
            code = info['address_components'][0]['short_name']
        new_info = {'name_e': name_e, 'code': code,
                    'lat': info['geometry']['location']['lat'], 'lng': info['geometry']['location']['lng']}
        if 'bounds' in info['geometry']:
            bounds = info['geometry']['bounds']
            new_info['lat_ne'] = bounds['northeast']['lat']
            new_info['lng_ne'] = bounds['northeast']['lng']
            new_info['lat_sw'] = bounds['southwest']['lat']
            new_info['lng_sw'] = bounds['southwest']['lng']
        else:
            for key in ('lat_ne', 'lng_ne', 'lat_sw', 'lng_sw'):
                new_info[key] = None

        raw = json.loads(
            common.get_data(r'http://maps.googleapis.com/maps/api/geocode/json',
                            data={'address': name_e, 'sensor': 'false'},
                            hdr={'Accept-Language': 'zh-cn,zh;q=0.8,en-us;q=0.5,en;q=0.3'}))
        if raw['status'] != 'OK':
            print('Error in %s, reason: %s' % (name_e, raw['status']))
            continue

        info = raw['results'][0]
        # 确保geocode类型为国家
        if 'country' in info['types']:
            name_c = info['address_components'][0]['long_name']
        new_info['name_c'] = name_c

        if new_info['lat_ne']:
            statement = "UPDATE country SET name_e='%s', name_c='%s', code='%s', lat=%f, lng=%f, lat_ne=%f, " \
                        "lng_ne=%f, lat_sw=%f, lng_sw=%f WHERE idcountry=%d" % (
                            new_info['name_e'].upper(), new_info['name_c'].upper(), new_info['code'],
                            new_info['lat'], new_info['lng'], new_info['lat_ne'],
                            new_info['lng_ne'], new_info['lat_sw'], new_info['lng_sw'], idcountry)
        else:
            statement = "UPDATE country SET name_e='%s', name_c='%s', code='%s', lat=%f, lng=%f " \
                        "WHERE idcountry=%d" % (
                            new_info['name_e'].upper(), new_info['name_c'].upper(), new_info['code'],
                            new_info['lat'], new_info['lng'], idcountry)
        print(statement)
        db.execute(statement)


def test():
    sense_cities('a', 'b')
    # geo_translate()
    # dump_geo()


if __name__ == "__main__":
    test_flag = True
    # passwd = 'rose123'
    passwd = '123456'
    if test_flag:
        test()
    else:
        levis_eu.fetch(passwd=passwd)
        # bershka.fetch(passwd=passwd)