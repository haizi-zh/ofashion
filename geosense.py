# coding=utf-8
import hashlib
import json
import re
import sys
import time
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

    country_name = country_name.strip().upper()

    if look_up(country_name, 1) is None:
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

    if city_name == None:
        return

    country_name = look_up(country_name, 1)['name_e']
    country_guid = country_map['lookup'][country_name]
    city_name = city_name.strip().upper()
    if look_up(city_name, 3) is None:
        item = {'country': country_guid, 'province': '', 'name_e': city_name, 'name_c': ''}
        if province_name is not None:
            # province_name = province_name.strip().upper()
            ret = look_up(province_name, 2)
            if ret is not None:
                item['province'] = province_map['lookup'][ret['name_e']]
        while True:
            sha.update(str(random.randint(0, sys.maxint)))
            guid = ''.join(['%x' % ord(v) for v in sha.digest()])
            if guid not in city_map['data']:
                city_map['data'][guid] = item
                city_map['lookup'][city_name] = [guid]
                print 'City added: %s in %s' % (item['name_e'], country_name)
                break


def geocode(city, retried):
    metro_list=[]
    js = cm.get_data('http://maps.googleapis.com/maps/api/geocode/json',
                             {'address': city.encode('utf-8'), 'sensor': 'false'})
    entry = json.loads(js)
    if entry['status']=='OVER_QUERY_LIMIT':
        if retried:
            print 'Failed to geocode due to query limit: %s'%city
        else:
            print 'Cooling down...'
            time.sleep(5)
            geocode(city, True)
            return

    if entry['status'] != u'OK' or len(entry['results']) < 1:
        print 'Failed to geocode: %s, reason: %s' % (city, entry['status'])
        return

    addr = entry['results'][0]['address_components']
    geo = entry['results'][0]['geometry']

    item = {'name_e': addr[0]['long_name'], 'country':'',
            'lat': geo['location']['lat'], 'lng': geo['location']['lng']}
    if len(addr) > 1:
        item['country'] = addr[-1]['long_name']

    print 'Added: %s' % item
    metro_list.append(item)

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





def field_sense(entry):
    # Geo
    country = entry[cm.country_e]
    city = entry[cm.city_e]
    ret = look_up(city, 3)
    if ret is not None:
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
        entry[cm.province_e]=ret['name_e']
        entry[cm.province_c]=ret['name_c']

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
    else:
        print 'Error in looking up %s' % country
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


