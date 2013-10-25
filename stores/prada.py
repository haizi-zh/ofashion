# coding=utf-8
import json
import logging
import logging.config
import string
import re

from pyquery import PyQuery as pq

import common as cm
from stores import geosense as gs


__author__ = 'Zephyre'


def fetch_store_list(data, logger):
    url = data['url']
    try:
        raw_data = cm.get_data(url)
    except Exception:
        logger.error('Error in fetching store lists: %s' % url)
        return []

    body = pq(raw_data)('div.store-country')

    country, city = None, None
    results = []

    def func(tag):
        for item in body(tag)('li a'):
            data_type = item.attrib['data-type']
            if data_type == 'country':
                country = item.text.upper().strip()
            elif data_type == 'city':
                city = item.text.upper().strip()
            elif data_type == 'store':
                d = data.copy()
                d['country'] = country
                d['city'] = city
                d['store_name'] = item.text.upper().strip()
                d['store_url'] = d['host'] + item.attrib['href']
                d['native_id'] = int(item.attrib['data-id'])
                results.append(d)

    map(func, ('#shop-list', '#outlet-list'))
    return results

    # start = raw_data.find(ur"<div class='store-country'>")
    # if start == -1:
    #     logger.error('Error in fetching store lists: %s' % url)
    #     return []
    # raw_data = cm.extract_closure(raw_data[start:], ur'<div\b', ur'</div>')[0]
    #
    # start_stores = raw_data.find(ur'<h3><a href="/store-locator/index">Stores</a></h3>')
    # start_outlets = raw_data.find(ur"<h3 class='outlets'>")
    # store_sub = raw_data[start_stores:start_outlets]
    # outlet_sub = raw_data[start_outlets:]
    #
    # results = []
    # for m1 in re.finditer(ur'<a [^<>]*data-id="([^"]+)"[^<>]*data-type="country">([^<>]+)</a>', store_sub):
    #     country_id = string.atoi(m1.group(1))
    #     country = m1.group(2).strip()
    #     sub1 = cm.extract_closure(store_sub[m1.end():], ur'<ul>', ur'</ul>')[0]
    #     for m2 in re.finditer(ur'<a [^<>]*data-id="([^"]+)"[^<>]*data-type="city">([^<>]+)</a>', sub1):
    #         city_id = string.atoi(m2.group(1))
    #         city = m2.group(2).strip()
    #         sub2 = cm.extract_closure(sub1[m2.end():], ur'<ul>', ur'</ul>')[0]
    #         for m3 in re.finditer(ur'<a href="([^"]+)"[^<>]*data-id="([^"]+)"[^<>]*data-type="store">([^<>]+)</a>',
    #                               sub2):
    #             d = data.copy()
    #             d['country_id'] = country_id
    #             d['country'] = country
    #             d['city_id'] = city_id
    #             d['city'] = city
    #             d['url'] = m3.group(1).strip()
    #             d['store_id'] = string.atoi(m3.group(2))
    #             d['store'] = cm.html2plain(m3.group(3).strip())
    #             # d['store_type'] = 'store'
    #             results.append(d)
    #
    # for m1 in re.finditer(ur'<a [^<>]*data-id="([^"]+)"[^<>]*data-type="country">([^<>]+)</a>', outlet_sub):
    #     country_id = string.atoi(m1.group(1))
    #     country = m1.group(2).strip()
    #     sub1 = cm.extract_closure(outlet_sub[m1.end():], ur'<ul>', ur'</ul>')[0]
    #     for m2 in re.finditer(ur'<a [^<>]*data-id="([^"]+)"[^<>]*data-type="city">([^<>]+)</a>', sub1):
    #         city_id = string.atoi(m2.group(1))
    #         city = m2.group(2).strip()
    #         sub2 = cm.extract_closure(sub1[m2.end():], ur'<ul>', ur'</ul>')[0]
    #         for m3 in re.finditer(ur'<a href="([^"]+)"[^<>]*data-id="([^"]+)"[^<>]*data-type="store">([^<>]+)</a>',
    #                               sub2):
    #             d = data.copy()
    #             d['country_id'] = country_id
    #             d['country'] = country
    #             d['city_id'] = city_id
    #             d['city'] = city
    #             d['url'] = m3.group(1).strip()
    #             d['store_id'] = string.atoi(m3.group(2))
    #             d['store'] = m3.group(3).strip()
    #             d['store_type'] = 'outlet'
    #             results.append(d)
    #
    # return results


def fetch_store_details(db, data, logger):
    url = data['store_url']
    try:
        body = pq(cm.get_data(url))
    except Exception:
        logger.error('Error in fetching store details: %s' % url)
        return []

    entry = cm.init_store_entry(data['brand_id'], data['brandname_e'], data['brandname_c'])

    entry[cm.addr_e] = cm.reformat_addr(unicode(body('p.address')))
    entry[cm.store_type] = ', '.join(temp.text.strip() for temp in body('li.availability li'))

    raw = json.loads(body('div.gmap_info_box')[0].attrib['data-shop'])['table']

    # start = body.find(ur'<h3>available in store</h3>')
    # if start != -1:
    #     type_sub = cm.extract_closure(body[start:], ur'<ul\b', ur'</ul>')[0]
    #     entry[cm.store_type] = ', '.join(
    #         cm.html2plain(tmp).strip() for tmp in re.findall(ur'<li[^<>]*>(.+?)</li>', type_sub, re.S))
    #
    # start = body.find(ur"<div class='gmap_info_box'")
    # if start == -1:
    #     logger.error('Error in fetching store details: %s' % url)
    #     return []
    # body = cm.extract_closure(body[start:], ur'<div\b', ur'</div>')[0]

    # raw = json.loads(cm.extract_closure(body, ur'\{', ur'\}')[0])['table']
    entry[cm.name_e] = cm.html2plain(raw['name'])
    entry[cm.city_e] = data['city'].strip().upper()
    entry[cm.country_e] = data['country'].strip().upper()
    # entry[cm.store_type] = data['store_type']
    entry[cm.addr_e] = cm.reformat_addr(raw['address'])
    m = re.search(re.compile(ur'phone:(.*?)fax:(.*?)', re.I | re.S), raw['phone'])
    if m is not None:
        entry[cm.tel] = m.group(1).strip()
        entry[cm.fax] = m.group(2).strip()
    else:
        m = re.search(re.compile(ur'phone:(.*?)', re.I | re.S), raw['phone'])
        if m is not None:
            entry[cm.tel] = m.group(1).strip()
        m = re.search(re.compile(ur'fax:(.*?)', re.I | re.S), raw['phone'])
        if m is not None:
            entry[cm.fax] = m.group(1).strip()
    entry[cm.hours] = raw['hours']
    if raw['lat'] is not None and raw['lat'] != '':
        entry[cm.lat] = string.atof(raw['lat'])
    if raw['lng'] is not None and raw['lng'] != '':
        entry[cm.lng] = string.atof(raw['lng'])
    gs.field_sense(entry)
    ret = gs.addr_sense(entry[cm.addr_e], entry[cm.country_e])
    if ret[1] is not None:
        entry[cm.province_e] = ret[1]
        gs.field_sense(entry)

    logger.info('(%s / %d) Found store: %s, %s (%s, %s)' % (data['brandname_e'], data['brand_id'],
                                                            entry[cm.name_e], entry[cm.addr_e], entry[cm.country_e],
                                                            entry[cm.continent_e]))
    cm.insert_record(db, entry, 'spider_stores.stores')
    return [entry]


def fetch(db, data=None, user='root', passwd=''):
    logging.config.fileConfig('prada.cfg')
    logger = logging.getLogger('firenzeLogger')

    def func(data, level):
        """
        :param data:
        :param level: 0：国家；1：城市；2：商店列表
        """
        if level == 0:
            return [{'func': lambda data: func(data, level + 1), 'data': s} for s in fetch_store_list(data, logger)]
        if level == 1:
            return [{'func': None, 'data': s} for s in fetch_store_details(db, data, logger)]
        else:
            return []

    # Walk from the root node, where level == 1.
    if data is None:
        data = {'url': 'http://www.prada.com/en/store-locator?cc=CN',
                'host': 'http://www.prada.com',
                'brand_id': 10300, 'brandname_e': u'Prada', 'brandname_c': u'普拉达'}

    db.query(str.format('DELETE FROM spider_stores.stores WHERE brand_id={0}', data['brand_id']))
    results = cm.walk_tree({'func': lambda data: func(data, 0), 'data': data})
    logger.info(u'Done')

    return results