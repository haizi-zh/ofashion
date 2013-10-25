# coding=utf-8
import json
import logging
import logging.config
import re
from stores import geosense as gs

__author__ = 'Zephyre'


def fetch_continents(data, logger):
    url = data['store_url']
    try:
        html = cm.get_data(url)
    except Exception:
        print 'Error occured: %s' % url
        dump_data = {'level': 0, 'time': cm.format_time(), 'data': {'url': url}, 'brand_id': data['brand_id']}
        cm.dump(dump_data)
        return []

    start = html.find(u'<select id="continent" name="continent"')
    if start == -1:
        return []
    sub, start, end = cm.extract_closure(html[start:], ur'<select\b', ur'</select')

    continent_list = []
    for m in re.findall(ur'<option value="(.+?)">.+?</option>', sub):
        d = data.copy()
        d['continent'] = m
        continent_list.append(d)
    return continent_list


def fetch_countries(data, logger):
    url = data['sel_url']
    try:
        body = cm.post_data(url, {'continent': data['continent'],
                                  'country': '', 'city': '', 'page': 0})
    except Exception:
        print 'Error occured: %s' % url
        dump_data = {'level': 0, 'time': cm.format_time(), 'data': {'url': url}, 'brand_id': data['brand_id']}
        cm.dump(dump_data)
        return []

    raw = json.loads(body)
    country_list = []
    for c in raw['country']:
        d = data.copy()
        d['country'] = c
        country_list.append(d)
    return country_list


def fetch_cities(data, logger):
    url = data['sel_url']
    try:
        body = cm.post_data(url, {'continent': data['continent'],
                                  'country': data['country'], 'city': '', 'page': 0})
    except Exception:
        print 'Error occured: %s' % url
        dump_data = {'level': 0, 'time': cm.format_time(), 'data': {'url': url}, 'brand_id': data['brand_id']}
        cm.dump(dump_data)
        return []

    raw = json.loads(body)
    city_list = []
    for c in raw['city']:
        d = data.copy()
        d['city'] = c
        city_list.append(d)
    return city_list


def fetch_stores(db, data, logger):
    url = data['store_url']
    try:
        body = cm.post_data(url, {'continent': data['continent'],
                                  'country': data['country'], 'city': data['city'],
                                  'send': 1, 'page': 0})
    except Exception:
        print 'Error occured: %s' % url
        dump_data = {'level': 0, 'time': cm.format_time(), 'data': {'url': url}, 'brand_id': data['brand_id']}
        cm.dump(dump_data)
        return []

    store_list = []
    for m in re.finditer(ur'<div class="shop">', body):
        entry = cm.init_store_entry(data['brand_id'], data['brandname_e'], data['brandname_c'])

        sub, start, end = cm.extract_closure(body[m.end():], ur'<div\b', ur'</div>')
        if end == 0:
            continue
        m1 = re.search(ur'<h3>\s*(.+?)\s*</h3>', sub, re.S)
        if m1 is not None:
            entry[cm.name_e] = m1.group(1)

        m1 = re.search(ur'<p[^>]*>(.+?)</p>', sub, re.S)
        if m1 is not None:
            entry[cm.store_type] = re.sub(re.compile(ur'\s*\+\s*', re.S), ', ', m1.group(1).strip())

        addr_sub, start, end = cm.extract_closure(sub, ur'<ul\b', ur'</ul>')
        if end != 0:
            tmp = re.findall(ur'<li>\s*(.+?)\s*</li>', addr_sub)
            addr_list = []

            if len(tmp) >= 3:
                entry[cm.tel] = tmp[-1].strip()
                del tmp[-1]

            for term in tmp:
                term = cm.html2plain(term).strip()
                if term != '':
                    addr_list.append(term)
            entry[cm.addr_e] = ', '.join(addr_list)

        start = sub.lower().find(ur'opening hours')
        if start != -1:
            opening_sub, start, end = cm.extract_closure(sub[start:], ur'<ul\b', ur'</ul>')
            tmp = re.findall(ur'<li>\s*(.+?)\s*</li>', opening_sub)
            opening_list = []
            for term in tmp:
                term = cm.html2plain(term).strip()
                if term != '':
                    opening_list.append(term)
            entry[cm.hours] = ', '.join(opening_list)

        cm.update_entry(entry, {cm.continent_e: data['continent'].strip().upper(),
                                cm.country_e: data['country'].strip().upper()})
        entry[cm.city_e] = cm.extract_city(data['city'])[0]

        gs.field_sense(entry)
        ret = gs.addr_sense(entry[cm.addr_e], entry[cm.country_e])
        if ret[1] is not None and entry[cm.province_e] == '':
            entry[cm.province_e] = ret[1]
        if ret[2] is not None and entry[cm.city_e] == '':
            entry[cm.city_e] = ret[2]
        gs.field_sense(entry)

        print '(%s / %d) Found store: %s, %s (%s, %s)' % (data['brandname_e'], data['brand_id'],
                                                          entry[cm.name_e], entry[cm.addr_e], entry[cm.country_e],
                                                          entry[cm.continent_e])
        store_list.append(entry)
        cm.insert_record(db, entry, 'spider_stores.stores')
    return store_list


def fetch(db, data=None, user='root', passwd=''):
    logging.config.fileConfig('escada.cfg')
    logger = logging.getLogger('firenzeLogger')
    logger.info(u'escada STARTED')

    def func(data, level):
        """
        :param data:
        :param level: 0：国家；1：城市；2：商店列表
        """
        if level == 0:
            # 洲列表
            return [{'func': lambda data: func(data, 1), 'data': s} for s in fetch_continents(data, logger)]
        if level == 1:
            # 国家列表
            return [{'func': lambda data: func(data, 2), 'data': s} for s in fetch_countries(data, logger)]
        if level == 2:
            # 城市列表
            return [{'func': lambda data: func(data, 3), 'data': s} for s in fetch_cities(data, logger)]
        if level == 3:
            # 商店列表
            return [{'func': None, 'data': s} for s in fetch_stores(db, data, logger)]
        else:
            return []

    # Walk from the root node, where level == 1.
    if data is None:
        data = {'sel_url': 'http://de.escada.com/en/ajax/updateStorefinderForm/',
                'store_url': 'http://de.escada.com/en/dynamiccontent/storefinder/',
                'brand_id': 10122, 'brandname_e': u'Escada', 'brandname_c': u'爱斯卡达'}

    db.query(u'DELETE FROM %s WHERE brand_id=%d' % ('spider_stores', data['brand_id']))

    results = cm.walk_tree({'func': lambda data: func(data, 0), 'data': data})
    logger.info(u'DONE')

    return results