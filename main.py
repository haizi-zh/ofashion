# coding=utf-8
import glob
import json
import logging
import pickle
import re
import string
import time
import traceback
import itertools
import _mysql
import bvlgari
import ca
import cartier
import chanel
import common
import escada
import fendi
import geosense as gs
import lacoste
import longchamp
import marc_jacobs
import marni
import maurice_lacroix
import miss_sixty
import paul_joe
import prada
import prada2
import geocode_fetch
import logging.config
import sergio
import swarovski
import viktor_rolf
import viviennetam

__author__ = 'Zephyre'


def dump_geo():
    db = common.StoresDb()
    db.connect_db(passwd='123456')

    print('Clearing data table...')
    db.execute('DELETE FROM country')
    print('Writing city records...')

    print('Done!')
    db.disconnect_db()


def sense_cities(lower_bound='a', upper_bound='b', others=0):
    """
    规则化城市字段
    :param lower_bound:
    :param upper_bound:
    :param others: -1：所有；0：从lower扫描到upper；1：扫描lower和upper以外的特殊字符；2：扫描city_e为空的记录
    """

    def get_unique_latlng(latlng_list, tol_lat=0.5, tol_lng=1):
        """
        从一组经纬度数据点中，去掉距离过远的数据点，取得集中的坐标。
        :param latlng_list:
        :param tol_lat: 纬度的容忍度。
        :param tol_lng: 经度的容忍度。
        """

        def get_avg(l):
            return float(sum(l)) / len(l) if len(l) > 0 else None

        def func(vals, tol):
            vals = list(vals)
            avg = None
            while True:
                avg = get_avg(vals)
                if not avg:
                    break
                max_dist = sorted(tuple({'idx': idx, 'dist': abs(vals[idx] - avg)} for idx in xrange(len(vals))),
                                  key=lambda arg: arg['dist'])[-1]
                if max_dist['dist'] < tol:
                    break
                elif len(vals) == 2:
                    # 如果只有两个数据点，且相互离散，则该方法失效
                    avg = None
                    break
                else:
                    del vals[max_dist['idx']]
            return avg

        lat = func((tmp[0] for tmp in latlng_list), tol_lat)
        lng = func((tmp[1] for tmp in latlng_list), tol_lng)
        # 有时候经纬度会颠倒
        if lat is not None and lng is not None and abs(lat) > 90:
            lat, lng = lng, lat
        return (lat, lng)


    def register_city(geocoded_info):
        candidate_geo = None
        for geo_info in geocoded_info:
            admin_info = geo_info['administrative_info']
            if 'country' not in admin_info:
                common.dump(u'Country info does not exist: %s' % admin_info, log_name)
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
                common.dump(u'City info does not exist: %s' % admin_info, log_name)
                continue

            tmp_geo = {'city_e': city, 'country_e': admin_info['country']}
            if 'administrative_area_level_1' in admin_info:
                tmp_geo['region_e'] = admin_info['administrative_area_level_1']
            else:
                tmp_geo['region_e'] = ''
            tmp_geo['formatted_address'] = geo_info['formatted_address']

            if not candidate_geo:
                candidate_geo = tmp_geo
                # 检验一致性，国家或城市信息必须一致
            ret1 = gs.look_up(country_e, 1)
            ret2 = gs.look_up(admin_info['country'], 1)
            if (ret1['name_e'] if ret1 else country_e) != (ret2['name_e'] if ret2 else admin_info['country']):
                common.dump(u'Countries does not match.', log_name)
                ret3 = gs.look_up(city_e, 1)
                ret4 = gs.look_up(city, 1)
                if (ret3['name_e'] if ret3 else city_e) != (ret4['name_e'] if ret4 else city):
                    common.dump(u'Cities does not match.', log_name)
                    continue

            # 如果走到这一步，说明geo_info通过了上述检验，可以使用
            candidate_geo = tmp_geo
            break

        # candidate_geo是正确的地理信息
        if not candidate_geo:
            return False

        # 登记城市标准化信息
        std_info = candidate_geo

        # 获得中文信息
        std_info['country_c'] = ''
        std_info['region_c'] = ''
        std_info['city_c'] = ''
        geocoded_info_zh = gs.geocode(addr=candidate_geo['formatted_address'], lang='zh')
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
        if others == 2:
            city_std[idstores] = {'std_sig': std_sig}
        else:
            city_std[sig] = {'std_sig': std_sig}
        if 'std_sig' not in city_std:
            city_std[std_sig] = {'std_info': std_info, 'geo_info': geo_info}
        common.dump(u'%s => %s' % (sig, std_sig), log_name)
        return True

    city_std = {}
    if others == -1:
        log_name = u'sense_cities_all.log'
        file_name = u'data/city_std_all.dat'
    elif others == 1:
        log_name = u'sense_cities_others.log'
        file_name = u'data/city_std_others.dat'
    elif others == 2:
        log_name = u'sense_cities_null.log'
        file_name = u'data/city_std_null.dat'
    else:
        log_name = u'sense_cities_%s_%s.log' % (lower_bound, upper_bound)
        file_name = u'data/city_std_%s_%s.dat' % (lower_bound, upper_bound)
    try:
        with open(file_name, 'r') as f:
            # {'city|region|country':{'std_info':{'city':...,'region':...,'country':...}, 'geo_result': result}}
            # 城市的标准化映射信息
            city_std = json.load(f, 'utf-8')
    except IOError:
        common.dump(u'Failed to load data/city_std.dat', log_name)

    db = common.StoresDb()
    db.connect_db(host='localhost', port=3306, user='root', passwd='123456', db='brand_stores')
    if others == -1:
        tpl_entity = "SELECT DISTINCT city_e, province_e, country_e FROM stores WHERE city_e!='' AND (is_geocoded<4 OR is_geocoded>7) AND is_geocoded!=100 ORDER BY city_e, province_e, country_e LIMIT 99999"
        tpl_entity = "SELECT DISTINCT city_e, province_e, country_e FROM stores WHERE city_e!='' AND is_geocoded=0 ORDER BY city_e, province_e, country_e LIMIT 99999"
    elif others == 1:
        tpl_entity = "SELECT DISTINCT city_e, province_e, country_e FROM stores WHERE (city_e<'a' OR city_e>'}') AND city_e!='' AND (is_geocoded<4 OR is_geocoded>7) AND is_geocoded!=100 ORDER BY city_e, province_e, country_e LIMIT 99999"
        # tpl_entity = "SELECT DISTINCT city_e, province_e, country_e FROM stores WHERE (city_e<'a' OR city_e>'}') AND city_e!='' AND is_geocoded=6 ORDER BY city_e, province_e, country_e LIMIT 99999"
    elif others == 2:
        tpl_entity = "SELECT city_e, province_e, country_e, idstores FROM stores WHERE city_e='' AND (is_geocoded<4 OR is_geocoded>7) AND is_geocoded!=100 ORDER BY city_e, province_e, country_e LIMIT 99999"
        # tpl_entity = "SELECT city_e, province_e, country_e, idstores FROM stores WHERE city_e='' ORDER BY city_e, province_e, country_e LIMIT 99999"
    else:
        tpl_entity = "SELECT DISTINCT city_e, province_e, country_e FROM stores WHERE city_e>'%s' AND city_e<'%s' AND (is_geocoded<4 OR is_geocoded>7) ORDER BY city_e, province_e, country_e LIMIT 99999"
    tpl_pos = "SELECT lat, lng, addr_e, idstores FROM stores WHERE city_e='%s' AND province_e='%s' AND country_e='%s' LIMIT 99999"
    tpl_geocoded = "UPDATE stores SET is_geocoded=%d WHERE city_e='%s' AND province_e='%s' AND country_e='%s'"

    if others == 0:
        statement = tpl_entity if others else tpl_entity % (lower_bound, upper_bound)
    else:
        statement = tpl_entity
    common.dump(u"Processing cities from '%s' to '%s'..." % (lower_bound, upper_bound), log_name)
    for item in db.query_all(statement):
        try:
            sig = u'|'.join(item[i].upper() for i in xrange(3))
            if others != 2:
                if sig in city_std:
                    common.dump(u'Geo item %s already processed.' % sig, log_name)
                    tmp1 = [7]
                    tmp1.extend(tmp.replace("'", r"\'") for tmp in (item[i] for i in xrange(3)))
                    statement = tpl_geocoded % tuple(tmp1)
                    db.execute(statement)
                    continue
            common.dump(u'Processing %s...' % sig, log_name)

            city_e, province_e, country_e = item[:3]
            geo_success = False
            if others == 2:
                idstores = item[3]
                statement = "SELECT lat, lng, addr_e, idstores FROM stores WHERE idstores=%s LIMIT 99999" % item[3]
            else:
                statement = tpl_pos % tuple(tmp.replace("'", r"\'") for tmp in item)
            query_result = db.query_all(statement)
            # 使用经纬度进行查询
            latlng_list = []
            for lat, lng, addr, tmp in query_result:
                if not lat or not lng or lat == '' or lng == '':
                    continue
                latlng_list.append(tuple(map(string.atof, (lat, lng))))

            lat, lng = get_unique_latlng(latlng_list)
            if lat and lng:
                tmp = gs.geocode(latlng='%f,%f' % (lat, lng))
                if tmp:
                    geo_success = register_city(tmp)
            if geo_success:
                # 通过经纬度获得
                tmp1 = [4]
                tmp1.extend(tmp.replace("'", r"\'") for tmp in item)
                if others == 2:
                    statement = "UPDATE stores SET is_geocoded=4 WHERE idstores=%s" % idstores
                else:
                    statement = tpl_geocoded % tuple(tmp1)
                db.execute(statement)
            else:
                for lat, lng, addr, idstores in query_result:
                    if city_e == '' and province_e == '' and country_e == '':
                        continue
                        # 使用地址进行查询
                    tmp = gs.geocode(u'%s,%s,%s,%s' % (addr, city_e, province_e, country_e))
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
                    if others == 2:
                        statement = "UPDATE stores SET is_geocoded=5 WHERE idstores=%s" % idstores
                    else:
                        statement = tpl_geocoded % tuple(tmp1)
                    db.execute(statement)
                else:
                    # 未能获得
                    tmp1 = [6]
                    tmp1.extend(tmp.replace("'", r"\'") for tmp in item)
                    if others == 2:
                        statement = "UPDATE stores SET is_geocoded=6 WHERE idstores=%s" % idstores
                    else:
                        statement = tpl_geocoded % tuple(tmp1)
                    db.execute(statement)

            # 如果city_e为空，更新
            if others == 2 and geo_success:
                std_info = city_std[city_std[idstores]['std_sig']]['std_info']
                ret = db.query_all("SELECT idcontinent FROM country WHERE name_e='%s'" % std_info['country_e'])
                if len(ret) == 0:
                    common.dump(u'Failed to lookup continent for %s' % std_info['country_e'])
                else:
                    ret = db.query_all("SELECT name_e, name_c FROM continent WHERE idcontinent='%s'" % ret[0][0])
                    continent_e = ret[0][0]
                    continent_c = ret[0][1]
                    statement = 'UPDATE stores SET continent_e="%s", continent_c="%s", country_e="%s", country_c="%s", province_e="%s", province_c="%s", city_e="%s", city_c="%s" where idstores=%s' % (
                        continent_e, continent_c, std_info['country_e'], std_info['country_c'],
                        std_info['region_e'], std_info['region_c'],
                        std_info['city_e'], std_info['city_c'], idstores)
                    db.execute(statement)

        except Exception:
            common.dump(traceback.format_exc(), log_name)

    with open(file_name, 'w') as f:
        json.dump(city_std, f)

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


def merge_city_std(dump=False):
    """
    合并城市标准化信息
    """
    city_std = {}
    for file_name in glob.iglob(u'data/city_std*.dat'):
        if u'city_std_all.dat' in file_name:
            continue

        print(u'Processing %s...' % file_name)
        with open(file_name, 'r') as f:
            raw = json.load(f, u'utf-8')
        for key in raw.keys():
            if key not in city_std:
                city_std[key] = raw[key]

    if dump:
        print(u'Dumping the whole info...')
        with open(u'data/city_std_all.dat', 'w') as f:
            json.dump(city_std, f, ensure_ascii=True)

    print(u'Done!')


def decode_city():
    """
    解析Unicode escape
    """
    log_name = u'decode_city.log'
    db = common.StoresDb()
    db.connect_db(host='localhost', port=3306, user='root', passwd='123456', db='brand_stores')

    ret = db.query_all('SELECT idstores, city_e FROM stores WHERE locate("u00", city_e)')
    pat = re.compile(ur'u00.{2}', re.I)
    for idstores, city_e in ret:
        term_list = re.findall(pat, city_e)
        term_c = re.split(pat, city_e)
        new_term_list = [(u'\\' + tmp).lower().decode('unicode_escape').upper() for tmp in term_list]
        new_term_list.append('')
        tmp_list = []
        for i in xrange(len(new_term_list)):
            tmp_list.append(term_c[i])
            tmp_list.append(new_term_list[i])
        new_city_e = ''.join(tmp_list)
        common.dump(u'%s => %s' % (city_e, new_city_e), log_name)
        db.execute('UPDATE stores SET city_e="%s" WHERE idstores=%s' % (new_city_e, idstores))

    ret = db.query_all('SELECT idstores, country_e FROM stores WHERE locate("u00", country_e)')
    for idstores, country_e in ret:
        term_list = re.findall(pat, country_e)
        term_c = re.split(pat, country_e)
        new_term_list = [(u'\\' + tmp).lower().decode('unicode_escape').upper() for tmp in term_list]
        new_term_list.append('')
        tmp_list = []
        for i in xrange(len(new_term_list)):
            tmp_list.append(term_c[i])
            tmp_list.append(new_term_list[i])
        new_country_e = ''.join(tmp_list)
        common.dump(u'%s => %s' % (country_e, new_country_e), log_name)
        db.execute('UPDATE stores SET country_e="%s" WHERE idstores=%s' % (new_country_e, idstores))

    pass


def proc_city_std():
    """
    更新数据库的city, region等字段
    """
    with open(u'data/city_std_all.dat', 'r') as f:
        city_std = json.load(f, encoding='utf-8')

    db = common.StoresDb()
    db.connect_db(host='localhost', port=3306, user='root', passwd='123456', db='brand_stores')

    log_name = u'proc_city.log'

    # 已经更新的字段，形式为region|country|或者
    processed_country = set([])
    # 检查country字段
    for key in itertools.ifilter(lambda key: u'std_info' in city_std[key], city_std):
        common.dump(u'Processing %s' % key, log_name)
        city, region, country = (city_std[key]['std_info'][tmp_key] for tmp_key in ('city_e', 'region_e', 'country_e'))
        if country in processed_country:
            continue
        ret = db.query_all('SELECT * FROM country WHERE name_e="%s"' % country)
        if len(ret) == 1:
            common.dump(u'Country hit: %s' % country, log_name)
            processed_country.add(country)
        else:
            if len(db.query_all('SELECT * FROM country WHERE locate("%s", alias)' % country)) > 0:
                common.dump(u'Country hit: %s' % country, log_name)
                continue

            # 将新国家添加到数据库中
            try:
                ret = gs.geocode(addr='%s' % country)[0]
                admin_info = ret['administrative_info']
                admin_info['country_code'] = ''
                for component in ret['address_components']:
                    if 'country' in component['types']:
                        admin_info['country_code'] = component['short_name']
                        break
                geo_info = ret['geometry']
                ret = gs.geocode(addr='%s' % country, lang='zh')[0]
                admin_info_zh = ret['administrative_info']

                # 如果已存在，则添加到alias字段中
                ret = db.query_all('SELECT idcountry, alias FROM country WHERE code="%s"' % admin_info['country_code'])
                if len(ret) > 0:
                    idcountry, alias = ret[0]
                    common.dump(u'Adding %s to the alias: %s' % (country, alias), log_name)
                    alias = u'%s|%s' % (alias, country) if alias != '' else country
                    db.execute('UPDATE country SET alias="%s" WHERE idcountry=%s' % (alias, idcountry))
                    continue

                common.dump(u'Failed to look up country: %s, now adding to the database...' % country, log_name)
                statement = ('INSERT INTO country (idcontinent, continent, name_e, name_c, code, lat, lng, '
                             'lat_ne, lng_ne, lat_sw, lng_sw) VALUES (%d, "%s", "%s", "%s", "%s", %f, %f, %f, %f, '
                             '%f, %f)') % (0, 'UNKNOWN', admin_info['country'], admin_info_zh['country'],
                                           admin_info['country_code'], geo_info['location']['lat'],
                                           geo_info['location']['lng'],
                                           geo_info['bounds']['northeast']['lat'],
                                           geo_info['bounds']['northeast']['lng'],
                                           geo_info['bounds']['southwest']['lat'],
                                           geo_info['bounds']['southwest']['lng'])
                common.dump(statement, log_name)
                db.execute(statement)
                processed_country.add(country)
            except (IndexError, TypeError):
                common.dump(traceback.format_exc(), log_name)

    # 更新数据库的region和city字段
    for key in itertools.ifilter(lambda key: u'std_info' in city_std[key], city_std):
        try:
            common.dump(u'Processing %s...' % key, log_name)
            pos_info = city_std[key]['geo_info']['geometry']
            std_info = city_std[key]['std_info']

            # 省/州信息
            ret = db.query_all(u'SELECT * FROM region WHERE country="%s" AND name_e="%s"' % (
                std_info['country_e'], std_info['region_e']))
            if len(ret) == 0:
                common.dump(u'Adding new region in %s: %s(%s)' % tuple(std_info[tmp] for tmp in
                                                                       ('country_e', 'region_e', 'region_c')), log_name)
                country_hit = False
                ret1 = db.query_all(u'SELECT idcountry FROM country WHERE name_e="%s"' % std_info['country_e'])
                if len(ret1) == 0:
                    ret1 = db.query_all(
                        u'SELECT idcountry FROM country WHERE locate("%s", alias)' % std_info['country_e'])
                    if len(ret1) == 0:
                        common.dump(u'Failed to lookup the country %s' % std_info['country_e'], log_name)
                    else:
                        country_hit = True
                else:
                    country_hit = True

                if country_hit:
                    statement = u'INSERT INTO region (idcountry, country, name_e, name_c) VALUES ' \
                                u'("%s", "%s", "%s", "%s")' % (ret1[0][0], std_info['country_e'],
                                                               std_info['region_e'], std_info['region_c'])
                    common.dump(statement, log_name)
                    db.execute(statement)

            # 城市信息
            ret = db.query_all(u'SELECT * FROM city WHERE country="%s" AND region="%s" AND name_e="%s"' % (
                std_info['country_e'], std_info['region_e'], std_info['city_e']))
            if len(ret) == 0:
                common.dump(u'Adding new city in %s: %s(%s)' % tuple(std_info[tmp] for tmp in
                                                                     ('country_e', 'city_e', 'city_c')), log_name)
                country_hit = False
                ret1 = db.query_all(u'SELECT idcountry FROM country WHERE name_e="%s"' % std_info['country_e'])
                if len(ret1) == 0:
                    ret1 = db.query_all(
                        u'SELECT idcountry FROM country WHERE locate("%s", alias)' % std_info['country_e'])
                    if len(ret1) == 0:
                        common.dump(u'Failed to lookup the country %s' % std_info['country_e'], log_name)
                    else:
                        country_hit = True
                else:
                    country_hit = True

                if country_hit:
                    if 'bounds' in pos_info:
                        bounds = pos_info['bounds']
                    elif 'viewpoint' in pos_info:
                        bounds = pos_info['viewpoint']
                    else:
                        bounds = None
                    if bounds:
                        statement = u'INSERT INTO city (idcountry, country, region, name_e, name_c, lat, lng, ' \
                                    u'lat_ne, lng_ne, lat_sw, lng_sw) VALUES ("%s", "%s", "%s", "%s", "%s", ' \
                                    u'%f, %f, %f, %f, %f, %f)' % (
                                        ret1[0][0], std_info['country_e'], std_info['region_e'], std_info['city_e'],
                                        std_info['city_c'], pos_info['location']['lat'], pos_info['location']['lng'],
                                        pos_info['bounds']['northeast']['lat'], pos_info['bounds']['northeast']['lng'],
                                        pos_info['bounds']['southwest']['lat'], pos_info['bounds']['southwest']['lng'])
                    else:
                        statement = u'INSERT INTO city (idcountry, country, region, name_e, name_c, lat, lng) ' \
                                    u'VALUES ("%s", "%s", "%s", "%s", "%s", %f, %f)' % \
                                    (ret1[0][0], std_info['country_e'], std_info['region_e'], std_info['city_e'],
                                     std_info['city_c'], pos_info['location']['lat'], pos_info['location']['lng'])
                    common.dump(statement, log_name)
                    db.execute(statement)
        except (KeyError, IndexError, TypeError):
            common.dump(traceback.format_exc(), log_name)


def update_city_std(delta_lat=2, delta_lng=4, lower=None, upper=None, null_city=False):
    """
    将stores记录中的地理信息字段标准化
    :param delta_lat: 如果原始记录和标准记录之间的经纬度差异超过该数值，说明原始记录有误。
    :param delta_lng:
    """
    with open(u'data/city_std_all.dat', 'r') as f:
        city_std = json.load(f, encoding='utf-8')

    db = common.StoresDb()
    db.connect_db(host='localhost', port=3306, user='root', passwd='123456', db='brand_stores')
    if null_city:
        log_name = u'update_city_std_null.log'
    else:
        log_name = u'update_city_std_%s_%s.log' % (lower, upper) if lower and upper else u'update_city_std.log'

    if null_city:
        # 处理city_e为空的数据
        query_results = filter(lambda key: re.search(ur'^\d+$', key), city_std.keys())
        tot_cnt = len(query_results)
        cnt = 0
        for idstores in query_results:
            cnt += 1
            common.dump(
                u'%d/%d(%.1f%%) Processing idstores=%s...' % (cnt, tot_cnt, float(cnt) / tot_cnt * 100, idstores),
                log_name)
            try:
                info = city_std[city_std[idstores]['std_sig']]
                while True:
                    if 'geo_info' in info:
                        break
                    elif 'std_sig' in info:
                        info = city_std[info['std_sig']]
                    else:
                        break
                loc = info['geo_info']['geometry']['location']
                admin = info['std_info']
                # 原始数据的经纬度信息
                try:
                    lat_o, lng_o = map(string.atof,
                                       db.query_all('SELECT lat, lng FROM stores WHERE idstores=%s' % idstores)[0])
                    if abs(lat_o) > 90:
                        lat_o, lng_o = lng_o, lat_o
                    if abs(lat_o - loc['lat']) > delta_lat or abs(lng_o - loc['lng']) > delta_lng:
                        common.dump(
                            u'Lat-lng mismatch. Original: (%f, %f), std: (%f, %f)' % (
                                lat_o, lng_o, loc['lat'], loc['lng']),
                            log_name)
                        lat, lng = loc['lat'], loc['lng']
                    else:
                        lat, lng = lat_o, lng_o
                except (ValueError, TypeError):
                    lat, lng = loc['lat'], loc['lng']
                param = map(lambda key: admin[key].replace(u'"', u'\\"'),
                            ('country_e', 'country_c', 'region_e', 'region_c', 'city_e', 'city_c'))
                param.extend((lat, lng, 100, idstores))
                statement = 'UPDATE stores SET country_e="%s", country_c="%s", province_e="%s", province_c="%s", ' \
                            'city_e="%s", city_c="%s", lat=%f, lng=%f, is_geocoded=%d WHERE idstores=%s' % tuple(param)
                common.dump(statement, log_name)
                db.execute(statement)
            except (KeyError, IndexError, TypeError):
                common.dump(traceback.format_exc(), log_name)
    else:
        # 处理city_e非空的数据
        if lower and upper:
            query_results = db.query_all(
                'SELECT DISTINCT city_e,province_e,country_e FROM stores WHERE city_e!="" AND city_e>"%s" '
                'AND city_e<"%s" AND is_geocoded!=6 '
                'AND is_geocoded!=100 LIMIT 999999' % (lower, upper))
        else:
            query_results = db.query_all(
                'SELECT DISTINCT city_e,province_e,country_e FROM stores WHERE city_e!="" AND '
                'is_geocoded!=6 AND is_geocoded!=100 LIMIT 999999')
        tot_cnt = len(query_results)
        cnt = 0
        # 计时
        ts_start = time.time()
        ts_list = []
        max_sample = 30
        for city, region, country in query_results:
            cnt += 1
            sig = u'|'.join((city.upper(), region.upper(), country.upper()))
            if len(ts_list) == max_sample:
                del ts_list[0]
            ts_list.append(time.time())
            # 剩余时间估计
            est_time = (tot_cnt - cnt) * ((ts_list[-1] - ts_list[0]) / (len(ts_list) - 1)) if len(ts_list) > 1 else None

            common.dump(u'%d/%d(%.1f%%) Processing %s... Elapsed time: %s, Estimated time left: %s' % (
                cnt, tot_cnt, float(cnt) / tot_cnt * 100, sig, common.get_time_str(time.time() - ts_start),
                common.get_time_str(est_time) if est_time else u'N/A',), log_name)
            try:
                info = city_std[sig]
                while True:
                    if 'geo_info' in info:
                        break
                    elif 'std_sig' in info:
                        info = city_std[info['std_sig']]
                    else:
                        break

                loc = info['geo_info']['geometry']['location']
                admin = info['std_info']
                for store in db.query_all('SELECT idstores, lat, lng FROM stores WHERE city_e="%s" AND '
                                          'province_e="%s" AND country_e="%s" AND is_geocoded!=6 AND '
                                          'is_geocoded!=100' % (
                    city.replace(u'"', u'\\"'), region.replace(u'"', u'\\"'), country.replace(u'"', u'\\"'))):
                    idstores = store[0]
                    # 原始数据的经纬度信息
                    try:
                        lat_o, lng_o = map(string.atof, store[1:])
                        if abs(lat_o) > 90:
                            lat_o, lng_o = lng_o, lat_o
                        if abs(lat_o - loc['lat']) > delta_lat or abs(lng_o - loc['lng']) > delta_lng:
                            common.dump(
                                u'Lat-lng mismatch. Original: (%f, %f), std: (%f, %f)' % (
                                    lat_o, lng_o, loc['lat'], loc['lng']),
                                log_name)
                            lat, lng = loc['lat'], loc['lng']
                        else:
                            lat, lng = lat_o, lng_o
                    except (ValueError, TypeError):
                        lat, lng = loc['lat'], loc['lng']

                    param = map(lambda key: admin[key].replace(u'"', u'\\"'),
                                ('country_e', 'country_c', 'region_e', 'region_c', 'city_e', 'city_c'))
                    param.extend((lat, lng, idstores))
                    statement = 'UPDATE stores SET country_e="%s", country_c="%s", province_e="%s", province_c="%s", ' \
                                'city_e="%s", city_c="%s", lat=%f, lng=%f, is_geocoded=100 WHERE idstores=%s' % tuple(
                        param)
                    common.dump(statement, log_name)
                    db.execute(statement)
            except (KeyError, IndexError, TypeError) as e:
                if isinstance(e, KeyError):
                    statement = 'UPDATE stores SET is_geocoded=0 WHERE city_e="%s" AND province_e="%s" AND country_e="%s"' % (
                        city.replace(u'"', u'\\"'), region.replace(u'"', u'\\"'), country.replace(u'"', u'\\"'))
                    common.dump(u'%s not exist. Reset is_geocoded to 0: %s' % (sig, statement), log_name)
                    db.execute(statement)
                else:
                    common.dump(traceback.format_exc(), log_name)
    common.dump(u'Done!', log_name)


def set_city_id():
    """
    设置idcity
    """
    db = common.StoresDb()
    db.connect_db(host='localhost', port=3306, user='rose', passwd='rose123', db='rose')
    log_name = u'set_city_id.log'
    query_results = db.query_all('SELECT idcity, name_e, region, country FROM city')
    tot_cnt = len(query_results)
    cnt = 0
    for idcity, city, region, country in query_results:
        cnt += 1
        common.dump(
            u'%d/%d(%.1f%%) Updating: %s|%s|%s' % (cnt, tot_cnt, float(cnt) / tot_cnt * 100, city, region, country),
            log_name)
        city = city.replace(u'"', u'\\"')
        region = region.replace(u'"', u'\\"')
        country = country.replace(u'"', u'\\"')
        db.execute('UPDATE stores SET idcity=%s WHERE city_e="%s" AND province_e="%s" AND country_e="%s"' % (
            idcity, city, region, country))

    common.dump(u'Done', log_name)


def fetch_stores(db, data, func_chain, level=0, logger=None):
    """
    :param data:
    :param func_chain:
    :param level: 0：国家；1：城市；2：商店列表
    """
    return [
        {
            'func': (lambda v: fetch_stores(db, v, func_chain, level + 1, logger)) if level < len(
                func_chain) - 1 else None,
            'data': s, 'level': level + 1} for s in func_chain[level](db, data, logger)]


def fetch_stores2(db, data, func_chain, level=0, node_tracker=None, node_dump=None, max_dep=2, logger=None):
    """
    :param data:
    :param func_chain:
    :param level: 0：国家；1：城市；2：商店列表
    """
    if node_tracker is not None and level <= max_dep:
        if unicode(level) not in node_tracker:
            node_tracker[unicode(level)] = {}

        # 该节点已经全部完成
        if data['node_id'] in node_tracker[unicode(level)] and node_tracker[unicode(level)][data['node_id']]:
            return
        node_tracker[unicode(level)][data['node_id']] = False
        if node_dump:
            with open(node_dump, 'w') as fpickle:
                json.dump(node_tracker, fpickle)

    ret = func_chain[level](db, data, logger)
    for s in ret:
        fetch_stores2(db, s, func_chain, level + 1, node_tracker, node_dump=node_dump, logger=logger)

    if node_tracker is not None and level <= max_dep:
        node_tracker[unicode(level)][data['node_id']] = True
        if node_dump:
            with open(node_dump, 'w') as fpickle:
                json.dump(node_tracker, fpickle)


def merge(db, brand_id, columns, logger=None):
    """
    将update_stores中的数据和stores中的数据合并
    :param db:
    :param columns: 需要更新的字段
    :param logger:
    """
    # db.query(str.format('SELECT * FROM update_stores WHERE brand_id={0}',brand_id))
    # record_set=db.store_result()

    pass


if __name__ == "__main__":
    test_flag = False

    if test_flag:
        print(geocode_fetch.calc_distance((-70.3, 35.2), (-4.1, 44.2)))
    else:
        db = _mysql.connect(db='spider_stores', user='root', passwd='123456')
        db.query("SET NAMES 'utf8'")
        module = chanel
        logger = module.get_logger()
        logger.info(unicode.format(u'{0} STARTED', module.__name__.upper()))
        logger.info(u'================')
        data = module.get_data()
        data['update'] = True
        data['table'] = 'spider_stores.stores'
        data['update_table'] = 'spider_stores.update_stores'
        module.init(db, data, logger)

        node_tracker = {}
        pickle_name = str.format('../log/{0}.p', module.__name__)
        try:
            with open(pickle_name, 'r') as fpickle:
                node_tracker = json.load(fpickle)
        except IOError as e:
            pass

        fetch_stores2(db, data, module.get_func_chain(), logger=logger, node_tracker=node_tracker,
                      node_dump=pickle_name)


        # results = common.walk_tree({'func': lambda v: fetch_stores(
        #     db, v, module.get_func_chain(), logger=logger), 'data': data})

        # #         如果为UPDATE模式，则还需要做merge操作
        # if data['update']:
        #     module.merge(db, data, logger)

        # fetch_stores(db, module.get_data(), module.get_func_chain(), logger)
        db.close()
        logger.info(u'================')
        logger.info(unicode.format(u'{0} STOPPED', module.__name__.upper()))