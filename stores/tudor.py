# coding=utf-8
import string
import re
import traceback

from pyquery import PyQuery as pq

import common as cm
from stores import geosense as gs


__author__ = 'Zephyre'

db = None
log_name = u'tudor_log.txt'
id_set = {}


def fetch_countries(data):
    url = data['location_url']
    try:
        body = cm.get_data(url, {'lang': data['lang']})
    except Exception:
        cm.dump('Error in fetching countries: %s' % url, 'tudor_log.txt')
        dump_data = {'level': 0, 'time': cm.format_time(), 'data': {'url': url}, 'brand_id': data['brand_id']}
        cm.dump(dump_data)
        return []

    results = []
    for item in pq(body.encode('utf-8'))('country[id!=""]'):
        d = data.copy()
        d['country_code'] = item.attrib['code']
        d['country_id'] = string.atoi(item.attrib['id'])
        results.append(d)
    return results


def fetch_regions(data):
    url = data['location_url']
    try:
        body = cm.get_data(url, {'lang': data['lang'], 'country': data['country_id']})
    except Exception:
        cm.dump('Error in fetching regions: %s, %s' % (url, data['country']), 'tudor_log.txt')
        dump_data = {'level': 0, 'time': cm.format_time(), 'data': {'url': url}, 'brand_id': data['brand_id']}
        cm.dump(dump_data)
        return []

    results = []
    for item in pq(body.encode('utf-8'))('region[id!=""]'):
        d = data.copy()
        d['region_id'] = string.atoi(item.attrib['id'])
        tmp = cm.html2plain(item.attrib['name']).strip().upper()
        d['region_name'] = re.sub(ur'市$', '', re.sub(ur'省$', '', tmp).strip()).strip()
        results.append(d)
    return results


def fetch_cities(data):
    url = data['location_url']
    try:
        body = cm.get_data(url, {'lang': data['lang'], 'country': data['country_id'], 'region': data['region_id']})
    except Exception:
        cm.dump('Error in fetching cities: %s, %s' % (url, data['region']), 'tudor_log.txt')
        dump_data = {'level': 0, 'time': cm.format_time(), 'data': {'url': url}, 'brand_id': data['brand_id']}
        cm.dump(dump_data)
        return []

    results = []
    for item in pq(body.encode('utf-8'))('city[id!=""]'):
        d = data.copy()
        d['city_id'] = string.atoi(item.attrib['id'])
        tmp = item.attrib['name']
        d['city_name'] = cm.extract_city(re.sub(ur'市$', '', re.sub(ur'省$', '', tmp).strip()).strip())[0]
        results.append(d)
    return results


def fetch_stores(data):
    url = data['data_url']
    param = {'lang': data['lang'], 'country': data['country_id'], 'region': data['region_id'],
             'city': data['city_id']}
    try:
        body = cm.get_data(url, param)
    except Exception:
        cm.dump('Error in fetching stores: %s, %s' % (url, param), 'tudor_log.txt')
        dump_data = {'level': 0, 'time': cm.format_time(), 'data': {'url': url}, 'brand_id': data['brand_id']}
        cm.dump(dump_data)
        return []

    store_list = []
    for store in (pq(tmp) for tmp in pq(body.encode('utf-8'))('dealer')):
        try:
            entry = cm.init_store_entry(data['brand_id'], data['brandname_e'], data['brandname_c'])
            entry[cm.country_e] = data['country_code']
            entry[cm.province_e] = data['region_name'].replace('PROVINCE', '').strip()
            entry[cm.city_e] = data['city_name']

            store_id = store[0].attrib['id']
            if store_id in id_set:
                if data['country_code'] == 'CN':
                    entry = id_set[store_id]

                    entry[cm.name_c] = cm.reformat_addr(store('name')[0].text).strip()
                    tmp = store('address')
                    entry[cm.addr_c] = cm.reformat_addr(tmp[0].text) if len(tmp) > 0 and tmp[0].text else ''
                    entry[cm.province_c] = data['region_name']
                    entry[cm.city_c] = data['city_name']

                    db.execute(u'DELETE FROM stores WHERE brand_id=%d AND native_id="%s"' % (
                        data['brand_id'], entry[cm.native_id]))
                    db.insert_record(entry, 'stores')
            else:
                entry[cm.native_id] = store_id

                entry[cm.name_e] = cm.reformat_addr(store('name')[0].text).strip()
                tmp = store('address')
                entry[cm.addr_e] = cm.reformat_addr(tmp[0].text) if len(tmp) > 0 and tmp[0].text else ''
                tmp = store('phone1')
                entry[cm.tel] = tmp[0].text.strip() if len(tmp) > 0 and tmp[0].text else ''

                tmp = store('fax1')
                entry[cm.fax] = tmp[0].text.strip() if len(tmp) > 0 and tmp[0].text else ''

                tmp = store('latitude')
                try:
                    entry[cm.lat] = string.atof(tmp[0].text) if len(tmp) > 0 and tmp[0].text else ''
                except (ValueError, KeyError, TypeError) as e:
                    cm.dump('Error in fetching lat: %s' % str(e), log_name)
                tmp = store('longitude')
                try:
                    entry[cm.lng] = string.atof(tmp[0].text) if len(tmp) > 0 and tmp[0].text else ''
                except (ValueError, KeyError, TypeError) as e:
                    cm.dump('Error in fetching lng: %s' % str(e), log_name)

                gs.field_sense(entry)
                ret = gs.addr_sense(entry[cm.addr_e])
                if ret[0] is not None and entry[cm.country_e] == '':
                    entry[cm.country_e] = ret[0]
                if ret[1] is not None and entry[cm.province_e] == '':
                    entry[cm.province_e] = ret[1]
                if ret[2] is not None and entry[cm.city_e] == '':
                    entry[cm.city_e] = ret[2]
                gs.field_sense(entry)
                cm.dump('(%s / %d) Found store: %s, %s (%s, %s)' % (data['brandname_e'], data['brand_id'],
                                                                    entry[cm.name_e], entry[cm.addr_e],
                                                                    entry[cm.country_e],
                                                                    entry[cm.continent_e]), 'tudor_log.txt')
                db.insert_record(entry, 'stores')
                id_set[store_id] = entry
                store_list.append(entry)
        except (IndexError, TypeError) as e:
            print traceback.format_exc()
            continue

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
                'data_url': 'http://www.tudorwatch.com/core/dealers/list', 'lang': 'en',
                'brand_id': 10362, 'brandname_e': u'Tudor', 'brandname_c': u'帝舵'}

    global db
    db = cm.StoresDb()
    db.connect_db(user=user, passwd=passwd)
    db.execute(u'DELETE FROM %s WHERE brand_id=%d' % ('stores', data['brand_id']))

    cm.walk_tree({'func': lambda data: func(data, 0), 'data': data})

    data['country_code'] = 'CN'
    data['country_id'] = 115
    data['lang'] = 'zh-Hans'
    cm.walk_tree({'func': lambda data: func(data, 1), 'data': data})

    db.disconnect_db()

    return id_set.values()