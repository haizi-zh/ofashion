# coding=utf-8
import string
import re

from pyquery import PyQuery as pq

import common as cm
from stores import geosense as gs


__author__ = 'Zephyre'

db = None
log_name = 'alexander_wang_log.txt'


def fetch_countries(data):
    url = data['url']
    try:
        body = cm.get_data(url)
    except Exception, e:
        cm.dump('Error in fetching countries: %s' % url, log_name)
        return ()

    results = []
    for item in pq(body)('select[class="country-dd"] option[value!=""]'):
        d = data.copy()
        d['country_code'] = item.attrib['value']
        d['country'] = cm.html2plain(item.text).strip().upper() if item.text else ''
        results.append(d)
    return tuple(results)


def fetch_stores(data):
    url = data['data_url']
    param = {'country': data['country_code'], 'city': data['city_id']}
    if data['state_id']:
        param['region'] = data['state_id']
    try:
        body = cm.get_data(url, param)
    except Exception, e:
        cm.dump('Error in fetching stores: %s, %s' % (url, param), log_name)
        return ()

    store_map = {}
    for item in pq(body)('ul[class="store-type-list clearfix"] li[class!="store-type all"] a'):
        store_url = item.attrib['href']
        if item.text:
            type_name = item.text.strip()
        else:
            continue

        try:
            store_url = data['host'] + store_url
            body = cm.get_data(store_url)
        except Exception, e:
            cm.dump('Error in fetching store for type: %s, %s' % (type_name, url), log_name)
            continue

        for store in (pq(tmp) for tmp in pq(body)('li.store')):
            try:
                tmp = cm.reformat_addr(unicode(pq(store('div.store-name')[0])))
                name = re.sub(ur'\d+\.\s*,\s*', '', tmp)
                addr = cm.reformat_addr(unicode(pq(store('div.store-address')[0])))
                uid = u'%s|%s' % (name, addr)
                if uid in store_map:
                    entry = store_map[uid]
                    entry[cm.store_type] = '%s, %s' % (entry[cm.store_type], type_name)
                else:
                    entry = cm.init_store_entry(data['brand_id'], data['brandname_e'], data['brandname_c'])
                    entry[cm.country_e] = data['country']
                    entry[cm.city_e] = data['city']
                    entry[cm.province_e] = data['state'] if data['state'] else ''
                    entry[cm.name_e] = name
                    entry[cm.store_type] = type_name
                    # entry[cm.url] = url
                    entry[cm.addr_e] = addr
                    tmp = store('div.store-phone')
                    if len(tmp) > 0 and tmp[0].text:
                        entry[cm.tel] = tmp[0].text.strip()
                    # entry[cm.native_id] = u'%s|%s|%s' % (uid, entry[cm.city_e], entry[cm.country_e])
                    store_map[uid] = entry
            except (IndexError, TypeError) as e:
                cm.dump(u'Error in parsing: %s' % store_url, log_name)
                continue

    for entry in store_map.values():
        gs.field_sense(entry)
        ret = gs.addr_sense(entry[cm.addr_e], entry[cm.country_e])
        if ret[1] is not None and entry[cm.province_e] == '':
            entry[cm.province_e] = ret[1]
        if ret[2] is not None and entry[cm.city_e] == '':
            entry[cm.city_e] = ret[2]
        gs.field_sense(entry)

        cm.dump('(%s / %d) Found store: %s, %s (%s, %s, %s)' % (data['brandname_e'], data['brand_id'],
                                                                entry[cm.name_e], entry[cm.addr_e], entry[cm.city_e],
                                                                entry[cm.country_e], entry[cm.continent_e]), log_name)
        db.insert_record(entry, 'stores')

    return tuple(store_map.values())


def fetch_states(data):
    url = data['data_url']
    param = {'country': data['country_code']}
    if param['country'] != 'US':
        d = data.copy()
        d['state_id'] = None
        d['state'] = ''
        return (d,)

    try:
        body = cm.get_data(url, param)
    except Exception, e:
        cm.dump('Error in fetching stores: %s, %s' % (url, param), log_name)
        return ()

    results = []
    for item in pq(body)('select[class="dd-region"] option[value!=""]'):
        d = data.copy()
        d['state_id'] = string.atoi(item.attrib['value'])
        d['state'] = cm.html2plain(item.text).strip().upper() if item.text else ''
        results.append(d)
    return tuple(results)


def fetch_cities(data):
    url = data['data_url']
    param = {'country': data['country_code']}
    if data['state_id']:
        param['region'] = data['state_id']
    try:
        body = cm.get_data(url, param)
    except Exception, e:
        cm.dump('Error in fetching stores: %s, %s' % (url, param), log_name)
        return ()

    results = []
    for item in pq(body)('select[class="dd-city"] option[value!=""]'):
        d = data.copy()
        d['city_id'] = item.attrib['value']
        d['city'] = cm.html2plain(item.text).strip().upper() if item.text else ''
        results.append(d)
    return tuple(results)


def fetch(level=1, data=None, user='root', passwd=''):
    def func(data, level):
        """
        :param data:
        :param level: 0：国家；1：城市；2：商店列表
        """
        if level == 0:
            # 国家列表
            return [{'func': lambda data: func(data, level + 1), 'data': s} for s in fetch_countries(data)]
        if level == 1:
            # 州列表
            return [{'func': lambda data: func(data, level + 1), 'data': s} for s in fetch_states(data)]
        if level == 2:
            # 城市列表
            return [{'func': lambda data: func(data, level + 1), 'data': s} for s in fetch_cities(data)]
        if level == 3:
            # 商店
            return [{'func': None, 'data': s} for s in fetch_stores(data)]
        else:
            return ()

    # Walk from the root node, where level == 1.
    if data is None:
        data = {'data_url': 'http://www.alexanderwang.com/stores/StoreList',
                'url': 'http://www.alexanderwang.com/stores/list?country=AT&region=&city=',
                'host': 'http://www.alexanderwang.com',
                'brand_id': 10009, 'brandname_e': u'Alexander Wang', 'brandname_c': u'亚历山大·王'}

    global db
    db = cm.StoresDb()
    db.connect_db(user=user, passwd=passwd)
    db.execute(u'DELETE FROM %s WHERE brand_id=%d' % ('stores', data['brand_id']))

    results = cm.walk_tree({'func': lambda data: func(data, 0), 'data': data})
    db.disconnect_db()
    cm.dump('Done!', log_name)

    return results


