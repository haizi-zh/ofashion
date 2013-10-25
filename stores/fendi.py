# coding=utf-8
import json
import logging
import logging.config
import re
from stores import geosense as gs

__author__ = 'Zephyre'

db = None
log_name = 'fendi_log.txt'
store_set = set([])


def gen_city_map():
    with open('city_lite.dat', 'r') as f:
        sub = f.readlines()
    return json.loads(sub[0])


def fetch_countries(data, logger):
    url = data['host']
    try:
        body = cm.get_data(url)
    except Exception, e:
        cm.dump('Error in fetching countries: %s' % url, log_name)
        return ()
    m = re.search(ur'<select[^<>]+id="country"[^<>]+>(.+?)</select>', body, re.S)
    sub = m.group(1)
    results = []
    for m in re.findall(ur'<option value="([a-z]{2})"', sub):
        d = data.copy()
        d['country_code'] = m
        results.append(d)
    return tuple(results)


def fetch_cities(data, logger):
    ret = gs.look_up(data['country_code'].upper(), 1)
    if ret is None:
        return ()

    country = ret['name_e']
    city_map = data['city_map']
    results = []
    if country in city_map:
        for city in city_map[country]:
            d = data.copy()
            d['country'] = country
            d['city'] = city
            d['city_lat'] = city_map[country][city]['lat']
            d['city_lng'] = city_map[country][city]['lng']
            results.append(d)
    return tuple(results)


def fetch_store_details(db, data, logger):
    url = data['url']
    try:
        body = cm.get_data(url, hdr={'X-Requested-With': ''})
    except Exception, e:
        cm.dump('Error in fetching store details: %s' % url, log_name)
        return ()

    start = body.find(ur'<div class="store-contact-info">')
    if start == -1:
        cm.dump('Error in fetching store details: %s' % url, log_name)
        return ()
    sub = cm.extract_closure(body[start:], ur'<div\b', ur'</div>')[0]
    entry = cm.init_store_entry(data['brand_id'], data['brandname_e'], data['brandname_c'])
    entry[cm.country_e] = data['country']
    entry[cm.city_e] = data['city']

    m = re.search(ur'<h2 class="store-name">([^<>]+)</h2>', sub, re.S)
    entry[cm.name_e] = cm.reformat_addr(m.group(1)) if m else ''

    m = re.search(ur'<address>(.+?)</address>', sub, re.S)
    ret = re.split(re.compile(ur'<!--\s*PHONE\s*/\s*FAX\s*-->', re.S | re.I), m.group(1))
    entry[cm.addr_e] = cm.reformat_addr(ret[0])
    if len(ret) > 1:
        tel_sub = cm.reformat_addr(ret[1])
        entry[cm.tel] = re.sub(re.compile(ur'tel\s*[\.:]?', re.I), '', tel_sub).strip()

    m = re.search(ur'<div class="wrap_products"', body)
    if m:
        type_sub = cm.extract_closure(body[m.start():], ur'<div\b', ur'</div>')[0]
        entry[cm.store_type] = ', '.join(re.findall(ur'<li[^<>]*>([^<>]+)', type_sub))

    m = re.search(ur'<div class="opening-hours-container"', body)
    if m:
        hours_sub = cm.extract_closure(body[m.start():], ur'<div\b', ur'</div>')[0]
        hours_list = []
        for item in re.findall(ur'<li>(.+?)</li>', hours_sub, re.S):
            m1 = re.search(ur'<span class="day">([^<>]+)', item)
            m2 = re.search(ur'<span class="time">(.+?)</span>', item, re.S)
            if m1 and m2:
                hours_list.append('%s: %s' % tuple(map(cm.reformat_addr, (m1.group(1), m2.group(1)))))
        entry[cm.hours] = ', '.join(hours_list)

    gs.field_sense(entry)
    ret = gs.addr_sense(entry[cm.addr_e], entry[cm.country_e])
    if ret[1] is not None and entry[cm.province_e] == '':
        entry[cm.province_e] = ret[1]
    if ret[2] is not None and entry[cm.city_e] == '':
        entry[cm.city_e] = ret[2]
    gs.field_sense(entry)

    cm.dump('(%s / %d) Found store: %s, %s (%s, %s)' % (data['brandname_e'], data['brand_id'],
                                                        entry[cm.name_e], entry[cm.addr_e], entry[cm.country_e],
                                                        entry[cm.continent_e]), log_name)
    # db.insert_record(entry, 'stores')
    return (entry, )


def fetch_store_list(data, logger):
    url = data['data_url']
    param = {'country': data['country_code'], 'query': data['city']}
    try:
        body = cm.get_data(url, param)
    except Exception, e:
        cm.dump('Error in fetching store list: %s, %s' % (url, param), log_name)
        return ()

    results = []
    for m in re.finditer(ur'<div class="pos-list-info"', body):
        sub = cm.extract_closure(body[m.start():], ur'<div\b', ur'</div>')[0]
        m = re.search(ur'<a class="[^"]*see-details[^"]*"\s+href="([^"]+)"', sub)
        if m:
            d = data.copy()
            d['url'] = data['host'] + m.group(1)
            if d['url'] not in store_set:
                store_set.add(d['url'])
                results.append(d)
    return tuple(results)


def fetch(db, data=None, user='root', passwd=''):
    logging.config.fileConfig('fendi.cfg')
    logger = logging.getLogger('firenzeLogger')
    logger.info(u'fendi STARTED')

    def func(data, level):
        """
        :param data:
        :param level: 0：国家；1：城市；2：商店列表
        """
        if level == 0:
            # 国家列表
            return [{'func': lambda data: func(data, level + 1), 'data': s} for s in fetch_countries(data, logger)]
        if level == 1:
            # 城市列表
            return [{'func': lambda data: func(data, level + 1), 'data': s} for s in fetch_cities(data, logger)]
        if level == 2:
            # 商店列表
            return [{'func': lambda data: func(data, level + 1), 'data': s} for s in fetch_store_list(data, logger)]
        if level == 3:
            # 商店
            return [{'func': None, 'data': s} for s in fetch_store_details(db, data, logger)]
        else:
            return ()

    # Walk from the root node, where level == 1.
    if data is None:
        data = {'data_url': 'http://store-en.fendi.com/search',
                'host': 'http://store-en.fendi.com',
                'brand_id': 10135, 'brandname_e': u'Fendi', 'brandname_c': u'芬迪',
                'city_map': gen_city_map()}

    # db.query(u'DELETE FROM %s WHERE brand_id=%d' % ('spider_stores', data['brand_id']))
    results = cm.walk_tree({'func': lambda data: func(data, 0), 'data': data})
    logger.info(u'DONE')

    return results


