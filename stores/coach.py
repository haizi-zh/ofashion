# coding=utf-8
import logging
import logging.config
import string
import re

from pyquery import PyQuery as pq

import common as cm
from stores import geosense as gs


__author__ = 'Zephyre'


def get_logger():
    logging.config.fileConfig('stores/coach.cfg')
    return logging.getLogger('firenzeLogger')


def fetch_countries(data):
    logger = logging.getLogger('firenzeLogger')
    url = data['url']
    try:
        body = cm.get_data(url)['body']
    except Exception, e:
        logger.error('Error in fetching countries: %s' % url)
        return ()

    states_set = set([])
    for m in re.findall(ur'<a href="[^"]+" onclick="setStateCode\(\'([A-Z]{2})\'\)', body, re.S):
        states_set.add(m)

    m = re.search(ur'<select id="countryCode"[^<>]*>(.+?)</select>', body, re.S)
    if m is None:
        cm.dump('Error in fetching countries: %s' % url, log_name)
        return ()
    sub = m.group(1)
    results = []
    country_set = set([])
    for m in re.findall(ur'<option\s+value="([A-Z]{2})"[^<>]*>([^<>]+)', sub):
        d = data.copy()
        code = m[0]
        if code in country_set:
            continue
        else:
            country_set.add(code)
        d['country_code'] = code
        d['states_set'] = states_set if code == 'US' else None
        d['country'] = cm.html2plain(m[1]).strip().upper()
        # if code in ('CN','CA','US'):
        results.append(d)
    return tuple(results)


def fetch_stores(data):
    url = data['data_url']
    logger = logging.getLogger('firenzeLogger')
    param = {'storeId': 10551, 'catalogId': 10051, 'countryTab': 'in', 'countryCode': data['country_code']}
    if data['country_code'] == 'US':
        param['radius'] = 20
        param['state'] = data['state_code']
    try:
        body = cm.get_data(url, param)['body']
    except Exception as e:
        logger.error('Error in fetching stores: %s, %s' % (url, param))
        return ()

    store_list = []
    for item in (pq(tmp) for tmp in pq(body)('div.vcard')):
        entry = cm.init_store_entry(data['brand_id'], data['brandname_e'], data['brandname_c'])
        entry[cm.country_e] = data['country_code']
        tmp = item('div.resultsHeader a b')
        if len(tmp) > 0 and tmp[0].text:
            entry[cm.name_e] = cm.html2plain(tmp[0].text)
        tmp = item('div.adr')
        if len(tmp) > 0:
            tmp = pq(tmp[0])
            entry[cm.addr_e] = cm.reformat_addr(unicode(tmp))
            tmp1 = tmp('.locality')
            if len(tmp1) > 0 and tmp1[0].text:
                entry[cm.city_e] = cm.extract_city(tmp1[0].text)[0]
            tmp1 = tmp('.region')
            if len(tmp1) > 0 and tmp1[0].text:
                entry[cm.province_e] = cm.html2plain(tmp1[0].text).strip().upper()
            tmp1 = tmp('.postal-code')
            if len(tmp1) > 0 and tmp1[0].text:
                entry[cm.zip_code] = tmp1[0].text

        tmp = item('div.tel')
        if len(tmp) > 0:
            entry[cm.tel] = tmp[0].text if tmp[0].text else ''

        tmp = item('div.store_hours')
        if len(tmp) > 0:
            entry[cm.hours] = cm.reformat_addr(unicode(pq(tmp[0])))

        tmp = item('#map')
        if len(tmp) > 0:
            m = re.search(ur'Lat=(-?\d+\.\d+)', unicode(pq(tmp[0])))
            if m:
                entry[cm.lat] = string.atof(m.group(1))
            m = re.search(ur'Lng=(-?\d+\.\d+)', unicode(pq(tmp[0])))
            if m:
                entry[cm.lng] = string.atof(m.group(1))

        gs.field_sense(entry)
        ret = gs.addr_sense(entry[cm.addr_e], entry[cm.country_e])
        if ret[1] is not None and entry[cm.province_e] == '':
            entry[cm.province_e] = ret[1]
        if ret[2] is not None and entry[cm.city_e] == '':
            entry[cm.city_e] = ret[2]
        gs.field_sense(entry)
        # cm.dump('(%s / %d) Found store: %s, %s (%s, %s)' % (data['brandname_e'], data['brand_id'],
        #                                                     entry[cm.name_e], entry[cm.addr_e], entry[cm.country_e],
        #
        #                                                     entry[cm.continent_e]), log_name)
        # cm.insert_record(data['database'], entry, 'stores')
        store_list.append(entry)

    return tuple(store_list)


def fetch_states(data):
    if data['country_code'] != 'US':
        d = data.copy()
        d['state_code'] = None
        return [d]

    results = []
    for state in data['states_set']:
        d = data.copy()
        d['state_code'] = state
        results.append(d)
    return tuple(results)


def fetch(db, data=None):
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
            # 商店
            return [{'func': None, 'data': s} for s in fetch_stores(data)]
        else:
            return ()

    # Walk from the root node, where level == 1.
    if data is None:
        data = {'data_url': 'http://www.coach.com/online/handbags/COAStoreLocatorWSCmd',
                'url': 'http://www.coach.com/online/handbags/StoreLocatorGMView?storeId=10551&catalogId=10051&LOC=BN',
                'brand_id': 10093, 'brandname_e': u'Coach', 'brandname_c': u'蔻驰', 'database': db}

    # db.query(u'DELETE FROM %s WHERE brand_id=%d' % ('stores', data['brand_id']))

    results = cm.walk_tree({'func': lambda data: func(data, 0), 'data': data})

    return results


