# coding=utf-8
import json
import logging
import string
import re
import traceback
import common as cm
import geosense as gs
from pyquery import PyQuery as pq

__author__ = 'Zephyre'

db = None
log_name = 'chanel_log.txt'


def gen_city_map():
    with open('city_lite.dat', 'r') as f:
        sub = f.readlines()
    return json.loads(sub[0])


def fetch_countries(data, logging=None):
    url = data['url']
    cm.get_data(url=url, data='')
    body = pq(url=url)
    body = body('#lstCountry option')
    results=[]
    for item1 in body:
        code=item1.attrib['value'].decode('utf-8')
        country=item1.text.decode('utf-8')
        ret=gs.lookup(code,1)
    return body



    results = []
    for m1 in re.finditer(ur'<ul class="countries">', body):
        sub = cm.extract_closure(body[m1.start():], ur'<ul\b', ur'</ul>')[0]
        for m2 in re.finditer(ur'<li>\s*<div class="value">', sub, re.S):
            country_sub = cm.extract_closure(sub[m2.start():], ur'<li\b', ur'</li>')[0]
            m3 = re.search(ur'<div class="value">\s*<a href="([^"]+)"[^<>]*>([^<>]+)', country_sub, re.S)
            if not m3:
                continue
            country = cm.html2plain(m3.group(2)).strip().upper()
            for m3 in re.findall(ur'<li><a href="([^"]+)"[^<>]*>([^<>]+)', country_sub):
                city = cm.html2plain(m3[1]).strip().upper()
                d = data.copy()
                d['country'], d['city'], d['url'] = country, city, m3[0]
                results.append(d)
    return tuple(results)


def fetch_stores(data):
    url = data['url']
    try:
        body = cm.get_data(url)
    except Exception, e:
        cm.dump('Error in fetching stores: %s' % url, log_name)
        return ()

    start = body.find(ur'<div id="boutiques">')
    if start == -1:
        cm.dump('Error in fetching stores: %s' % url, log_name)
        return ()
    body = cm.extract_closure(body[start:], ur'<div\b', ur'</div>')[0]

    store_list = []
    for m1 in re.finditer(ur'<div class="type"\s*>([^<>]+)', body):
        store_class = m1.group(1)
        type_sub = cm.extract_closure(body[m1.end():], ur'<ul>', ur'</ul>')[0]
        for m2 in re.findall(ur'<li>(.+?)</li>', type_sub, re.S):
            entry = cm.init_store_entry(data['brand_id'], data['brandname_e'], data['brandname_c'])
            entry[cm.country_e] = data['country']
            entry[cm.city_e] = cm.extract_city(data['city'])[0]
            entry[cm.store_class] = store_class

            m3 = re.search(ur'<div class="name">([^<>]+)', m2)
            entry[cm.name_e] = cm.html2plain(m3.group(1)).strip() if m3 else ''
            m3 = re.search(ur'<div class="state">([^<>]+)', m2)
            entry[cm.province_e] = cm.html2plain(m3.group(1)).strip().upper() if m3 else ''

            addr_list = []
            m3 = re.search(ur'<div class="mallhotel">([^<>]+)', m2)
            val = cm.html2plain(m3.group(1)).strip() if m3 else ''
            if val != '':
                addr_list.append(val)
            m3 = re.search(ur'<div class="address1">([^<>]+)', m2)
            val = cm.html2plain(m3.group(1)).strip() if m3 else ''
            if val != '':
                addr_list.append(val)
            m3 = re.search(ur'<div class="address2">([^<>]+)', m2)
            val = cm.html2plain(m3.group(1)).strip() if m3 else ''
            if val != '':
                addr_list.append(val)
            entry[cm.addr_e] = ', '.join(addr_list)

            m3 = re.search(ur'<div class="zipcode">([^<>]+)', m2)
            entry[cm.zip_code] = m3.group(1).strip() if m3 else ''

            try:
                m3 = re.search(ur'<div class="latitude">([^<>]+)', m2)
                entry[cm.lat] = string.atof(m3.group(1)) if m3 else ''
            except (ValueError, KeyError, TypeError) as e:
                cm.dump('Error in fetching lat: %s' % str(e), log_name)
            try:
                m3 = re.search(ur'<div class="longitude">([^<>]+)', m2)
                entry[cm.lng] = string.atof(m3.group(1)) if m3 else ''
            except (ValueError, KeyError, TypeError) as e:
                cm.dump('Error in fetching lng: %s' % str(e), log_name)

            m3 = re.search(ur'<a href="([^"]+)"\s*>DETAILS', m2)
            if m3:
                d = data.copy()
                d['url'] = m3.group(1)
                entry = fetch_store_details(d, entry)

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
            store_list.append(entry)

    return tuple(store_list)


def fetch_store_details(data, entry):
    entry = entry.copy()
    url = data['url']
    try:
        body = cm.get_data(url)
    except Exception, e:
        cm.dump('Error in fetching stores: %s' % url, log_name)
        return entry

    start = body.find(ur'<div id="boutique">')
    if start == -1:
        cm.dump('Error in fetching stores: %s' % url, log_name)
        return entry
    body = cm.extract_closure(body[start:], ur'<div\b', ur'</div>')[0]

    addr_list = []
    m3 = re.search(ur'<div id="floor">([^<>]+)', body)
    val = cm.html2plain(m3.group(1)).strip() if m3 else ''
    if val != '':
        addr_list.append(val)
    m3 = re.search(ur'<div id="mallhotel">([^<>]+)', body)
    val = cm.html2plain(m3.group(1)).strip() if m3 else ''
    if val != '':
        addr_list.append(val)
    m3 = re.search(ur'<div id="address1">([^<>]+)', body)
    val = cm.html2plain(m3.group(1)).strip() if m3 else ''
    if val != '':
        addr_list.append(val)
    m3 = re.search(ur'<div id="address2">([^<>]+)', body)
    val = cm.html2plain(m3.group(1)).strip() if m3 else ''
    if val != '':
        addr_list.append(val)
    entry[cm.addr_e] = ', '.join(addr_list)

    m = re.search(ur'<div id="phone">([^<>]+)</div>', body)
    entry[cm.tel] = cm.extract_tel(m.group(1)) if m else ''
    m = re.search(ur'<div id="fax">([^<>]+)</div>', body)
    entry[cm.fax] = cm.extract_tel(m.group(1)) if m else ''
    m = re.search(ur'<div id="email">([^<>]+)</div>', body)
    entry[cm.email] = m.group(1).strip() if m else ''

    m = re.search(ur'<div id="opening">', body)
    if m:
        hours_list = []
        for m in re.findall(ur'<li>([^<>]+)', cm.extract_closure(body[m.start():], ur'<div\b', ur'</div>')[0]):
            if m.strip() != '':
                hours_list.append(m.strip())
        entry[cm.hours] = ', '.join(hours_list)

    m = re.search(ur'<div id="products">', body)
    if m:
        type_list = []
        for m in (cm.html2plain(tmp).strip() for tmp in \
                  re.findall(ur'<li>([^<>]+)', cm.extract_closure(body[m.start():], ur'<div\b', ur'</div>')[0])):
            if len(m) > 1:
                type_list.append(m)
        entry[cm.store_type] = ', '.join(type_list)

    return entry


def fetch_countries_beauty(data):
    url = data['url']
    try:
        body = cm.get_data(url)
    except Exception, e:
        cm.dump('Error in fetching countries: %s' % url, log_name)
        return ()

    results = []
    for item in pq(body)('#lstCountry option[value]'):
        d = data.copy()
        d['country_code'] = item.attrib['value']
        results.append(d)
    return tuple(results)


def fetch_cities_beauty(data):
    url = '%s/%s' % (data['city_url'], data['country_code'])
    try:
        body = cm.get_data(url)
    except Exception, e:
        cm.dump('Error in fetching countries: %s' % url, log_name)
        return ()

    results = []
    html = pq(body)
    if len(html('select#lstCity')) > 0:
        for item in html('select#lstCity option'):
            if 'value' in item.attrib:
                continue
            d = data.copy()
            d['city_selected'] = True
            d['city'] = item.text
            results.append(d)
        return tuple(results)
    else:
        ret = gs.look_up(data['country_code'], 1)
        if ret is None:
            return ()

        country = ret['name_e']
        city_map = data['city_map']
        results = []
        if country in city_map:
            for city in city_map[country]:
                d = data.copy()
                d['city_selected'] = False
                d['city'] = city
                d['city_lat'] = city_map[country][city]['lat']
                d['city_lng'] = city_map[country][city]['lng']
                results.append(d)
        return tuple(results)


def fetch_stores_beauty(data):
    url = data['lst_url']
    param = {'chkCat[0]': 'FRG', 'chkCat[1]': 'MKP', 'chkCat[2]': 'PRE', 'chkCat[3]': 'EXC', 'div': 'fnb',
             'lstCountry': data['country_code'], 'lstCity': data['city']}
    try:
        body = cm.post_data(url, param)
    except Exception, e:
        cm.dump('Error in fetching countries: %s' % url, log_name)
        return ()

    store_list = []
    for store in (pq(tmp) for tmp in pq(body)('div[class="formfields searchresults"] div.storeitem')):
        try:
            entry = cm.init_store_entry(data['brand_id'], data['brandname_e'], data['brandname_c'])
            entry[cm.country_e] = data['country_code']
            entry[cm.city_e] = data['city']

            tmp = store('.storename')
            entry[cm.name_e] = cm.html2plain(tmp[0].text).strip() if len(tmp) > 0 and tmp[0].text else ''
            tmp = store('.storeaddr')
            entry[cm.addr_e] = cm.reformat_addr(tmp[0].text) if len(tmp) > 0 and tmp[0].text else ''
            tmp = store('.communicationmode')
            if len(tmp) > 0:
                val = (tmp.strip() for tmp in cm.reformat_addr(unicode(pq(tmp[0]))).split(','))
                for item in val:
                    pat_tel = re.compile(ur'(tel|telephone|phone)\s*[:\.]?\s*(.+)', re.I)
                    pat_fax = re.compile(ur'fax\s*[:\.]?\s*(.+)', re.I)
                    m = re.search(pat_tel, item)
                    if m:
                        entry[cm.tel] = m.group(2).strip()
                    else:
                        m = re.search(pat_fax, item)
                        entry[cm.fax] = m.group(1).strip() if m else ''

            tmp = store('.products')
            entry[cm.store_type] = cm.reformat_addr(unicode(pq(tmp[0]))) if len(tmp) > 0 else ''

            if 'city_lat' in data:
                entry[cm.lat] = data['city_lat']
            if 'city_lng' in data:
                entry[cm.lng] = data['city_lng']

            gs.field_sense(entry)
            ret = gs.addr_sense(entry[cm.addr_e], entry[cm.country_e])
            if ret[1] is not None and entry[cm.province_e] == '':
                entry[cm.province_e] = ret[1]
            if ret[2] is not None and entry[cm.city_e] == '':
                entry[cm.city_e] = ret[2]
            gs.field_sense(entry)
            entry[cm.comments] = 'BEAUTY'

            cm.dump('(%s / %d) Found store: %s, %s (%s, %s)' % (data['brandname_e'], data['brand_id'],
                                                                entry[cm.name_e], entry[cm.addr_e], entry[cm.country_e],
                                                                entry[cm.continent_e]), log_name)
            # db.insert_record(entry, 'stores')
            store_list.append(entry)
        except Exception, e:
            print traceback.format_exc()
            continue

    return tuple(store_list)


def fetch(level=1, data=None, user='root', passwd='', logger=None):
    def func(data, level, logging=None):
        """
        :param data:
        :param level: 0：国家；1：城市；2：商店列表
        """
        if level == 0:
            # 国家列表
            return [{'func': lambda data: func(data, level + 1, logging), 'data': s} for s in
                    fetch_countries(data, logging)]
        if level == 1:
            # 商店
            return [{'func': None, 'data': s} for s in fetch_stores(data)]
        else:
            return ()


    def func_beauty(data, level):
        if level == 0:
            # 国家列表
            return [{'func': lambda data: func_beauty(data, level + 1), 'data': s} for s in
                    fetch_countries_beauty(data)]
        if level == 1:
            # 城市列表
            return [{'func': lambda data: func_beauty(data, level + 1), 'data': s} for s in fetch_cities_beauty(data)]
        if level == 2:
            # 商店
            return [{'func': None, 'data': s} for s in fetch_stores_beauty(data)]
        else:
            return ()

    # Walk from the root node, where level == 1.
    if data is None:
        data = {'data_url': 'xxxxxxxxxx',
                'url': 'http://www-cn.chanel.com/fashion/storelocator/11-1',
                'brand_id': 10074, 'brandname_e': u'Chanel', 'brandname_c': u'香奈儿'}

    logger = logger if logger else logging.getLogger()

    global db
    results = []
    db = cm.StoresDb()
    db.connect_db(user=user, passwd=passwd)
    # db.execute(u'DELETE FROM %s WHERE brand_id=%d' % ('stores', data['brand_id']))

    # results = cm.walk_tree({'func': lambda data: func(data, 0), 'data': data})

    data = {'city_url': 'http://www-cn.chanel.com/store-finder/en_GB/store-locator/searchaddressform/fnb',
            'lst_url': 'http://www-cn.chanel.com/en_GB/store-locator/searchaddress/fnb/',
            'url': 'http://www-cn.chanel.com/store-finder/en_GB/store-locator/countrylist/GB/fnb',
            'brand_id': 10074, 'brandname_e': u'Chanel', 'brandname_c': u'香奈儿',
            'city_map': gen_city_map()}

    results.extend(cm.walk_tree({'func': lambda data: func(data, 0), 'data': data}))
    db.disconnect_db()
    cm.dump('Done!', log_name)

    return results


