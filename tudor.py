# coding=utf-8
import json
import string
import re
import common as cm
import geosense as gs

__author__ = 'Zephyre'

db = None


def fetch_countries(data):
    url = data['location_url']
    try:
        body = cm.get_data(url, {'lang': 'en'})
    except Exception:
        cm.dump('Error in fetching countries: %s' % url, 'tudor_log.txt')
        dump_data = {'level': 0, 'time': cm.format_time(), 'data': {'url': url}, 'brand_id': data['brand_id']}
        cm.dump(dump_data)
        return []

    results = []
    for m in re.findall(ur'<country code="([A-Z]{2})" id="(\d+)" name=".+?" seo-name="(.+?)"/>', body):
        d = data.copy()
        d['country_code'] = m[0]
        d['country_id'] = string.atoi(m[1])
        d['country'] = m[2].strip()
        results.append(d)
    return results


def fetch_regions(data):
    url = data['location_url']
    try:
        body = cm.get_data(url, {'lang': 'en', 'country': data['country']})
    except Exception:
        cm.dump('Error in fetching regions: %s, %s' % (url, data['country']), 'tudor_log.txt')
        dump_data = {'level': 0, 'time': cm.format_time(), 'data': {'url': url}, 'brand_id': data['brand_id']}
        cm.dump(dump_data)
        return []

    results = []
    for m in re.findall(ur'<region id="(\d+)" name="(.+?)" seo-name="(.+?)"', body):
        d = data.copy()
        d['region_id'] = string.atoi(m[0])
        d['region_name'] = m[1]
        d['region'] = m[2].strip()
        results.append(d)
    return results


def fetch_cities(data):
    url = data['location_url']
    try:
        body = cm.get_data(url, {'lang': 'en', 'country': data['country'], 'region': data['region']})
    except Exception:
        cm.dump('Error in fetching cities: %s, %s' % (url, data['region']), 'tudor_log.txt')
        dump_data = {'level': 0, 'time': cm.format_time(), 'data': {'url': url}, 'brand_id': data['brand_id']}
        cm.dump(dump_data)
        return []

    results = []
    for m in re.findall(ur'<city.*?id="(\d+)" name="(.+?)" seo-name="(.+?)"', body):
        d = data.copy()
        d['city_id'] = string.atoi(m[0])
        d['city_name'] = m[1]
        d['city'] = m[2].strip()
        results.append(d)
    return results


def fetch_stores(data):
    url = data['data_url']
    param = {'lang': 'en', 'country': data['country_id'], 'region': data['region_id'],
             'city': data['city_id']}
    try:
        body = cm.get_data(url, param)
    except Exception:
        cm.dump('Error in fetching stores: %s, %s' % (url, param), 'tudor_log.txt')
        dump_data = {'level': 0, 'time': cm.format_time(), 'data': {'url': url}, 'brand_id': data['brand_id']}
        cm.dump(dump_data)
        return []

    store_list = []
    for m in re.findall(ur'<dealer[^<>]+?>(.+?)</dealer>', body, re.S):
        entry = cm.init_store_entry(data['brand_id'], data['brandname_e'], data['brandname_c'])
        entry[cm.country_e] = data['country_code']
        entry[cm.province_e] = data['region_name'].upper().replace('PROVINCE', '').strip()
        entry[cm.city_e] = data['city_name'].strip().upper()

        m1 = re.search(ur'<name><!\[CDATA\[(.+?)\]\]></name>', m, re.S)
        if m1 is not None:
            entry[cm.name_e] = cm.reformat_addr(m1.group(1))

        m1 = re.search(ur'<address><!\[CDATA\[(.+?)\]\]></address>', m, re.S)
        if m1 is not None:
            entry[cm.addr_e] = cm.reformat_addr(m1.group(1))

        m1 = re.search(ur'<phone1>(.+?)</phone1>', m, re.S)
        if m1 is not None:
            entry[cm.tel] = m1.group(1).strip()

        m1 = re.search(ur'<fax1>(.+?)</fax1>', m, re.S)
        if m1 is not None:
            entry[cm.fax] = m1.group(1).strip()

        m1 = re.search(ur'<latitude>(.+?)</latitude>', m, re.S)
        if m1 is not None:
            entry[cm.lat] = string.atof(m1.group(1))

        m1 = re.search(ur'<longitude>(.+?)</longitude>', m, re.S)
        if m1 is not None:
            entry[cm.lng] = string.atof(m1.group(1))

        gs.field_sense(entry)
        cm.dump('(%s / %d) Found store: %s, %s (%s, %s)' % (data['brandname_e'], data['brand_id'],
                                                            entry[cm.name_e], entry[cm.addr_e], entry[cm.country_e],
                                                            entry[cm.continent_e]), 'tudor_log.txt')
        db.insert_record(entry, 'stores')
        store_list.append(entry)

    return store_list


def fetch(level=1, data=None, user='root', passwd=''):
    def func(data, level):
        """
        :param data:
        :param level: 0：国家；1：城市；2：商店列表
        """
        if level == 0:
            # 国家列表
            return [{'func': lambda data: func(data, 1), 'data': s} for s in fetch_countries(data)]
        if level == 1:
            # 州列表
            return [{'func': lambda data: func(data, level + 1), 'data': s} for s in fetch_regions(data)]
        if level == 2:
            # 城市列表
            return [{'func': lambda data: func(data, level + 1), 'data': s} for s in fetch_cities(data)]
        if level == 3:
            # 商店列表
            return [{'func': None, 'data': s} for s in fetch_stores(data)]
        else:
            return []

    # Walk from the root node, where level == 1.
    if data is None:
        data = {'location_url': 'http://www.tudorwatch.com/core/dealers/locations',
                'data_url': 'http://www.tudorwatch.com/core/dealers/list',
                'brand_id': 10362, 'brandname_e': u'Tudor', 'brandname_c': u'帝舵'}

    global db
    db = cm.StoresDb()
    db.connect_db(user=user, passwd=passwd)
    db.execute(u'DELETE FROM %s WHERE brand_id=%d' % ('stores', data['brand_id']))

    results = cm.walk_tree({'func': lambda data: func(data, 0), 'data': data})
    db.disconnect_db()

    return results