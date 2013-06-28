# coding=utf-8
import hashlib
import json
import re
import sys
import time
import urllib
import common as cm

__author__ = 'Zephyre'

continent_map = None
country_map = None
city_map = None
province_map = None

import random


def deref_guid(entry):
    """
    以递归的方式，替换掉guid部分
    :param entry:
    :return:
    """
    entry = dict(entry)
    if 'province' in entry:
        guid = entry['province']
        if guid != '':
            entry['province'] = deref_guid(province_map['data'][guid])

    if 'country' in entry:
        guid = entry['country']
        if guid != '':
            entry['country'] = deref_guid(country_map['data'][guid])

    if 'continent' in entry:
        guid = entry['continent']
        if guid != '':
            entry['continent'] = deref_guid(continent_map['data'][guid])

    return entry


def look_up(term, level=0):
    """
    查询一个地理词条的信息
    :param term:
    :param level: 查询级别：0：洲；1：国家；2：州/省；3：城市
    :return:
    """
    if term is None:
        return None

    term = term.strip().upper()
    g_map = [continent_map, country_map, province_map, city_map]
    if term in g_map[level]['lookup']:
        guid = g_map[level]['lookup'][term]
        if isinstance(guid, list):
            guid = guid[0]
        entry = deref_guid(g_map[level]['data'][guid])
        return entry
    else:
        return None


def commit_maps(opt=3):
    if opt == 3:
        with open('data/city_map_new.dat', 'w') as f:
            f.write(json.dumps(city_map, ensure_ascii=False).encode('utf-8'))
    elif opt == 2:
        with open('data/province_map_new.dat', 'w') as f:
            f.write(json.dumps(province_map, ensure_ascii=False).encode('utf-8'))
    elif opt == 1:
        with open('data/country_map_new.dat', 'w') as f:
            f.write(json.dumps(country_map, ensure_ascii=False).encode('utf-8'))
    elif opt == 0:
        with open('data/continent_map_new.dat', 'w') as f:
            f.write(json.dumps(continent_map, ensure_ascii=False).encode('utf-8'))


def update_city_map(city_name=None, country_name=None, continent_name=None, province_name=None):
    """
    添加新的地理条目。
    :param city_name:
    :param country_name:
    :param continent_name:
    :param province_name:
    :return:
    """
    sha = hashlib.sha1()

    if continent_name is not None:
        continent_name = continent_name.strip().upper()
    if country_name is not None:
        country_name = country_name.strip().upper()
    if province_name is not None:
        province_name = province_name.strip().upper()

    city_list = city_name.split('/')
    for i in xrange(len(city_list)):
        city_list[i] = city_list[i].strip().upper()
    city_name = city_list[0]

    if country_name is not None and look_up(country_name, 1) is None:
        if continent_name is None:
            return
        continent_guid = continent_map['lookup'][look_up(continent_name, 0)['name_e']]
        item = {'continent': continent_guid, 'name_e': country_name, 'name_c': ''}
        while True:
            sha.update(str(random.randint(0, sys.maxint)))
            guid = ''.join(['%x' % ord(v) for v in sha.digest()])
            if guid not in country_map['data']:
                country_map['data'][guid] = item
                country_map['lookup'][country_name] = guid
                print 'Country added: %s in %s' % (item['name_e'], continent_name)
                break

    if province_name is not None and look_up(province_name, 2) is None:
        item = {'country': '', 'code': '', 'name_e': province_name, 'name_c': ''}
        ret = look_up(country_name, 1)
        if ret is not None:
            item['country'] = country_map['lookup'][ret['name_e']]
        while True:
            sha.update(str(random.randint(0, sys.maxint)))
            guid = ''.join(['%x' % ord(v) for v in sha.digest()])
            if guid not in province_map['data']:
                province_map['data'][guid] = item
                province_map['lookup'][province_name] = guid
                print 'Province added: %s in %s' % (item['name_e'], country_name)
                break

    if city_name is not None and look_up(city_name, 3) is None:
        item = {'country': '', 'province': '', 'name_e': city_name, 'name_c': ''}

        ret = look_up(country_name, 1)
        if ret is not None:
            item['country'] = country_map['lookup'][ret['name_e']]
        ret = look_up(province_name, 2)
        if ret is not None:
            item['province'] = province_map['lookup'][ret['name_e']]
        while True:
            sha.update(str(random.randint(0, sys.maxint)))
            guid = ''.join(['%x' % ord(v) for v in sha.digest()])
            if guid not in city_map['data']:
                city_map['data'][guid] = item
                for alias in city_list:
                    city_map['lookup'][alias] = [guid]
                    if province_name is not None:
                        print 'City added: %s in %s, %s' % (alias, province_name, country_name)
                    else:
                        print 'City added: %s in %s' % (alias, country_name)

                break


# def geocode(city, retried):
#     metro_list = []
#     js = cm.get_data('http://maps.googleapis.com/maps/api/geocode/json',
#                      {'address': city.encode('utf-8'), 'sensor': 'false'})
#     entry = json.loads(js)
#     if entry['status'] == 'OVER_QUERY_LIMIT':
#         if retried:
#             print 'Failed to geocode due to query limit: %s' % city
#         else:
#             print 'Cooling down...'
#             time.sleep(5)
#             geocode(city, True)
#             return
#
#     if entry['status'] != u'OK' or len(entry['results']) < 1:
#         print 'Failed to geocode: %s, reason: %s' % (city, entry['status'])
#         return
#
#     addr = entry['results'][0]['address_components']
#     geo = entry['results'][0]['geometry']
#
#     item = {'name_e': addr[0]['long_name'], 'country': '',
#             'lat': geo['location']['lat'], 'lng': geo['location']['lng']}
#     if len(addr) > 1:
#         item['country'] = addr[-1]['long_name']
#
#     print 'Added: %s' % item
#     metro_list.append(item)


def load_geo():
    global continent_map
    global country_map
    global city_map
    global province_map

    with open('data/continent_map.dat', 'r') as f:
        continent_map = json.load(f)
    with open('data/country_map.dat', 'r') as f:
        country_map = json.load(f)
    with open('data/province_map.dat', 'r') as f:
        province_map = json.load(f)
    with open('data/city_map.dat', 'r') as f:
        city_map = json.load(f)


load_geo()


def addr_sense(addr, country=None, province=None, city=None):
    # 从地址信息中找出国家和城市
    terms = addr.split(',')

    if country is None:
        for i in xrange(-1, -len(terms) - 1, -1):
            tmp = look_up(terms[i].strip().upper(), 1)
            if tmp is not None:
                country = tmp['name_e']
                break

    if province is None:
        for i in xrange(-1, -len(terms) - 1, -1):
            tmp = look_up(re.sub(ur'\d+', '', terms[i]).strip().upper(), 2)
            if tmp is not None:
                if country is not None and country != tmp['country']['name_e']:
                    continue
                else:
                    province = tmp['name_e']
                    if country is None and tmp['country'] != '':
                        country = tmp['country']['name_e']
                    break

    for i in xrange(-1, -len(terms) - 1, -1):
        tmp = look_up(re.sub(ur'\d+', '', terms[i]).strip().upper(), 3)
        if tmp is not None:
            if country is not None and country != tmp['country']['name_e'] and tmp['name_e'] != 'HONG KONG' \
                and tmp['name_e'] != 'MACAU':
                continue
            else:
                city = tmp['name_e']
                if country is None and tmp['country'] != '':
                    country = tmp['country']['name_e']
                if province is None and tmp['province'] != '':
                    province = tmp['province']['name_e']
                break

    return country, province, city


def geocode(addr=None, latlng=None, retry=3, cooling_time=2, log_name=None):
    # http://maps.googleapis.com/maps/api/geocode/json?address=1600+Amphitheatre+Parkway,+Mountain+View,+CA&sensor=true_or_false
    if addr is not None:
        if isinstance(addr, unicode):
            addr = addr.encode('utf-8')
        else:
            addr = str(addr)

    if latlng is not None:
        if isinstance(latlng, unicode):
            latlng = latlng.encode('utf-8')
        else:
            latlng = str(latlng)

    url = 'http://maps.googleapis.com/maps/api/geocode/json?'
    param = {'sensor': 'false'}
    if addr is not None:
        param['address'] = addr
    if latlng is not None:
        param['latlng'] = latlng

    url += urllib.urlencode(param)

    cnt = 0
    while True:
        cool = True
        try:
            ret = json.loads(cm.get_data(url))
            if ret['status'] == 'OK':
                return ret['results']
            elif ret['ZERO_RESULTS']:
                return None
            else:
                cool = False
                raise Exception()
        except Exception, e:
            cnt += 1
            if cnt < retry:
                if cool:
                    time.sleep(cooling_time)
                continue
            else:
                if log_name is None:
                    cm.dump('Error in geocoding: %s, status: %s' % (url, ret['status']))
                else:
                    cm.dump('Error in geocoding: %s, status: %s' % (url, ret['status']), log_name)
                return None
    return None


def field_sense(entry):
    # Geo
    country = entry[cm.country_e]
    city = entry[cm.city_e]
    ret = look_up(city, 3)
    ret1 = look_up(country, 1)
    if ret1 is not None:
        country = ret1['name_e']
    if ret is not None and ret['country']['name_e'] == country:
        entry[cm.city_e] = ret['name_e']
        entry[cm.city_c] = ret['name_c']

        prov = ret['province']
        if prov != '':
            ret1 = look_up(prov['name_e'], 2)
            if ret1 is not None:
                entry[cm.province_e] = ret1['name_e']
                entry[cm.province_c] = ret1['name_c']

    province = entry[cm.province_e]
    ret = look_up(province, 2)
    if ret is not None:
        entry[cm.province_e] = ret['name_e']
        entry[cm.province_c] = ret['name_c']

    ret = look_up(country, 1)
    if ret is not None:
        cm.update_entry(entry, {cm.country_e: ret['name_e'], cm.country_c: ret['name_c']})
        ret1 = look_up(ret['continent']['name_e'], 0)
        cm.update_entry(entry, {cm.continent_e: ret1['name_e'], cm.continent_c: ret1['name_c']})

        if entry[cm.zip_code] == '':
            m = None
            if ret['name_e'] == look_up(u'CHINA', 1)['name_e']:
                # 中国邮编
                m = re.match(ur'.*\b(\d{6})\b', entry[cm.addr_e])
            elif ret['name_e'] == look_up(u'UNITED STATES', 1)['name_e']:
                # 美国邮编
                m = re.match(ur'.*\b(\d{5})\b', entry[cm.addr_e])
            elif ret['name_e'] == look_up(u'JAPAN', 1)['name_e']:
                # 日本邮编
                m = re.match(ur'.*\b(\d{3}\-\d{4})\b', entry[cm.addr_e])
            if m is not None:
                entry[cm.zip_code] = m.group(1)

    cm.chn_check(entry)

    if entry[cm.zip_code] == '':
        # 数字和城市，州一起，可能为邮编
        m = re.match(ur'.*\s+(\d{5,})\b', entry[cm.addr_e])
        if m is not None:
            tmp = entry[cm.addr_e][m.end() + 1:]
            terms = re.findall(ur'\b(\S+?)\b', tmp)
            if len(terms) > 0:
                if look_up(terms[0], 2) is not None or look_up(terms[0], 3) is not None:
                    entry[cm.zip_code] = m.group(1)
            else:
                tmp = entry[cm.addr_e][m.end() - len(m.group(1)) - 1::-1]
                terms = re.findall(ur'\b(\S+?)\b', tmp)
                if len(terms) > 0:
                    if look_up(terms[0][::-1], 2) is not None or look_up(terms[0][::-1], 3) is not None:
                        entry[cm.zip_code] = m.group(1)


