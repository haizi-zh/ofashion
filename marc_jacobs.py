# coding=utf-8
import json
import string
import re
import common as cm
import geosense as gs

__author__ = 'Zephyre'

db = None
log_name = 'marc_jacobs_log.txt'


def fetch_countries(data):
    url = data['url']
    try:
        body = cm.get_data(url)
    except Exception:
        cm.dump('Error in fetching countries: %s' % url, log_name)
        return []

    m = re.search(ur'<select class="country-select"', body)
    if m is None:
        cm.dump('Error in fetching countries: %s' % url, log_name)
        return []
    body = cm.extract_closure(body[m.start():], ur'<select\b', ur'</select>')[0]
    results = []
    for m in re.findall(ur'<option value="([A-Z]{2})">', body):
        d = data.copy()
        d['country_code'] = m
        results.append(d)
    return results


def fetch_cities(data):
    url = data['host'] + data['city_url']
    param = {'cc': data['country_code']}
    try:
        body = cm.get_data(url, param)
    except Exception:
        cm.dump('Error in fetching cities: %s, %s' % (url, param), log_name)
        return []

    if data['country_code'] == 'LU':
        d = data.copy()
        d['city'] = ''
        return [d]

    m = re.search(ur'<select class="city-select"', body)
    if m is None:
        cm.dump('Error in fetching cities: %s, %s' % (url, param), log_name)
        return []
    body = cm.extract_closure(body[m.start():], ur'<select\b', ur'</select>')[0]
    results = []
    for m in re.findall(ur'<option value="([^"]+)">', body):
        d = data.copy()
        d['city'] = m
        results.append(d)
    return results


def fetch_stores(data):
    url = data['host'] + data['store_url']
    param = {'CC': data['country_code'], 'City': data['city']}
    try:
        body = cm.get_data(url, param)
    except Exception:
        cm.dump('Error in fetching stores: %s, %s' % (url, param), log_name)
        return []

    store_list = []
    # pat_tel = re.compile(ur'tel:\s*', re.I)
    # pat_fax = re.compile(ur'fax:\s*', re.I)
    # pat_email = re.compile(ur'email:\s*', re.I)

    pat_tel = re.compile(ur'tel:\s*(.+?)(?=(?:tel|fax|email|$))', re.I | re.S)
    pat_fax = re.compile(ur'fax:\s*(.+?)(?=(?:tel|fax|email|$))', re.I | re.S)
    pat_email = re.compile(ur'email:\s*(.+?)(?=(?:tel|fax|email|$))', re.I | re.S)

    for m in re.finditer(ur'<div class="store-info">', body):
        entry = cm.init_store_entry(data['brand_id'], data['brandname_e'], data['brandname_c'])
        entry[cm.country_e] = data['country_code']
        entry[cm.city_e] = data['city'].strip().upper()

        sub = cm.extract_closure(body[m.start():], ur'<div\b', ur'</div>')[0]
        m1 = re.search(ur'<h2 class="store-name[^"]*">(.+?)</h2>', sub)
        if m1 is not None:
            entry[cm.name_e] = cm.reformat_addr(m1.group(1))
            entry[cm.store_class] = entry[cm.name_e]
        m1 = re.search(ur'<dt class="address"', sub)
        if m1 is not None:
            tmp = cm.reformat_addr(cm.extract_closure(sub[m1.end():], ur'<dd>', ur'</dd>')[0])
            entry[cm.addr_e] = tmp
            if len(tmp) > 1:
                m1 = re.search(ur'[\d\-]{4,}', tmp.split(',')[-2])
                if m1 is not None and len(re.findall(ur'\d', m1.group())) >= 4:
                    entry[cm.zip_code] = m1.group().strip()
        for m1 in re.findall(ur'<a href="#" class="phone-num">([^<>]+)</a>', sub):
            m2 = re.search(pat_tel, m1)
            if m2 is not None:
                entry[cm.tel] = m2.group(1).strip()
            m2 = re.search(pat_fax, m1)
            if m2 is not None:
                entry[cm.fax] = m2.group(1).strip()
            m2 = re.search(pat_email, m1)
            if m2 is not None:
                entry[cm.email] = m2.group(1).strip()

        m1 = re.search(ur'<dt>Store Hours</dt>', sub)
        if m1 is not None:
            start = sub.find(ur'<dt>')
            m2 = re.search(ur'<dd>(.+?)</dd>', sub[m1.end():start], re.S)
            if m2 is not None:
                entry[cm.hours] = m2.strip()
        m1 = re.search(ur'<dt>Store Carries</dt>', sub)
        if m1 is not None:
            entry[cm.store_type] = cm.reformat_addr(cm.extract_closure(sub[m1.end():], ur'<dd>', ur'</dd>')[0])
        m1 = re.search(ur'<ul class="store-links">', sub)
        if m1 is not None:
            m2 = re.search(ur'<a href="([^"]+)"', sub[m1.end():])
            if m2 is not None:
                entry[cm.url] = m2.group(1)

        gs.field_sense(entry)
        ret = gs.addr_sense(entry[cm.addr_e], entry[cm.country_e])
        if ret[1] is not None and entry[cm.province_e] == '':
            entry[cm.province_e] = ret[1]
        gs.field_sense(entry)
        cm.dump('(%s / %d) Found store: %s, %s (%s, %s)' % (data['brandname_e'], data['brand_id'],
                                                            entry[cm.name_e], entry[cm.addr_e], entry[cm.country_e],
                                                            entry[cm.continent_e]), log_name)
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
            return [{'func': lambda data: func(data, level + 1), 'data': s} for s in fetch_countries(data)]
        if level == 1:
            # 国家列表
            return [{'func': lambda data: func(data, level + 1), 'data': s} for s in fetch_cities(data)]
        if level == 2:
            # 商店
            return [{'func': None, 'data': s} for s in fetch_stores(data)]
        else:
            return []

    # Walk from the root node, where level == 1.
    if data is None:
        data = {'city_url': '/store/locationfilterpartial', 'store_url': '/store/list',
                'host': 'http://www.marcjacobs.com',
                'url': 'http://www.marcjacobs.com/store',
                'brand_id': 10239, 'brandname_e': u'Marc Jacobs', 'brandname_c': u'马克雅克布'}

    global db
    db = cm.StoresDb()
    db.connect_db(user=user, passwd=passwd)
    db.execute(u'DELETE FROM %s WHERE brand_id=%d' % ('stores', data['brand_id']))

    results = cm.walk_tree({'func': lambda data: func(data, 0), 'data': data})
    db.disconnect_db()

    return results