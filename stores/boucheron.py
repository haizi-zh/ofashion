# coding=utf-8
import json
import string
import re
from stores import geosense as gs

__author__ = 'Zephyre'

db = None
log_name = 'boucheron_log.txt'


def fetch_store_list(data):
    url = data['url']
    try:
        body = cm.get_data(url)
    except Exception, e:
        cm.dump('Error in fetching countries: %s' % url, log_name)
        return ()

    m = re.search(ur'var\s+storesDatas\s*=\s*', body)
    raw = json.loads(re.search(ur'(\{.+?\})\s*;', body[m.end():]).group(1))
    results = []
    for k1 in raw.keys():
        continent = raw[k1]['label']
        for k2 in raw[k1]['children'].keys():
            country = raw[k1]['children'][k2]['label']
            country_code = k2
            for item in raw[k1]['children'][k2]['children']:
                city = item['label']
                for store in item['children'].items():
                    d = data.copy()
                    d['country'] = country
                    d['country_code'] = country_code
                    d['city'] = city
                    d['store_id'] = store[0]
                    d['store_name'] = cm.html2plain(store[1]['label'])
                    d['content'] = body
                    try:
                        d['lat'] = string.atof(store[1]['lat'])
                    except (ValueError, KeyError, TypeError) as e:
                        d['lat'] = None
                        cm.dump('Error in fetching lat-lng: %s' % str(e), log_name)
                    try:
                        d['lng'] = string.atof(store[1]['lng'])
                    except (ValueError, KeyError, TypeError) as e:
                        d['lng'] = None
                        cm.dump('Error in fetching lat-lng: %s' % str(e), log_name)
                    results.append(d)

    return tuple(results)


def fetch_details(data, detail_url, entry):
    entry = entry.copy()
    try:
        body = cm.get_data(detail_url)
    except Exception, e:
        cm.dump('Error in fetching countries: %s' % detail_url, log_name)
        return entry

    m = re.search(ur'<div class="top">(.+?)</div>', body, re.S)
    pat_tel = re.compile(ur'(tel|telephone|phone)\s*[\.:]\s*(.+?)<', re.I)
    pat_fax = re.compile(ur'fax\s*[\.:]\s*(.+?)<', re.I)
    if m:
        sub = m.group(1)
        m1 = re.search(pat_tel, sub)
        entry[cm.tel] = m1.group(1).strip() if m1 else ''
        m1 = re.search(pat_fax, sub)
        entry[cm.fax] = m1.group(1).strip() if m1 else ''
        m1 = re.search(ur'class="type-produits-list"\s*>([^<>]+)</p>', sub)
        entry[cm.store_type] = m1.group(1).strip() if m1 else ''
        for m1 in re.findall(ur'<li[^<>]*>(.+?)</li>', sub):
            if 'monday' in m1.lower():
                entry[cm.hours] = m1.strip()
                break

    return entry


def fetch_stores(data):
    entry = cm.init_store_entry(data['brand_id'], data['brandname_e'], data['brandname_c'])
    code = data['country_code']
    if gs.look_up(code, 1) is None:
        entry[cm.country_e] = cm.html2plain(data['country']).strip().upper()
    else:
        entry[cm.country_e] = code
    entry[cm.name_e] = data['store_name']
    entry[cm.city_e] = cm.extract_city(data['city'])[0]
    entry[cm.lat] = data['lat'] if data['lat'] is not None else ''
    entry[cm.lng] = data['lng'] if data['lng'] is not None else ''

    m = re.search(ur'data-boutique\s*=\s*"%s"' % data['store_id'], data['content'])
    sub = data['content'][m.end():]

    m1 = re.search(ur'<li class="isDistributeur[^<>]+>(.+?)</li>', sub)
    if m1 is not None:
        entry[cm.store_class] = cm.reformat_addr(m1.group(1))

    m1 = re.search(ur'<li class="place-title[^<>]+>(.+?)</li>', sub, re.S)
    if m1 is not None:
        entry[cm.addr_e] = cm.reformat_addr(m1.group(1))

    m1 = re.search(ur'<li class="contacts[^<>]+>(.+?)</li>', sub, re.S)
    if m1 is not None:
        m2 = re.search(ur'<a class="popupLaunch" href="([^"]+)"', m1.group(1))
        if m2:
            entry = fetch_details(data, m2.group(1), entry)

        m2 = re.search(ur'<p>(.+?)</p>', m1.group(1), re.S)
        if m2:
            ct_list = tuple(tmp.strip() for tmp in cm.reformat_addr(m2.group(1)).split(','))
            entry[cm.tel] = cm.extract_tel(ct_list[0])
            if len(ct_list) > 1:
                entry[cm.email] = ct_list[1].strip()

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
    db.insert_record(entry, 'stores')

    return tuple(entry)


def fetch(level=1, data=None, user='root', passwd=''):
    def func(data, level):
        """
        :param data:
        :param level: 0：国家；1：城市；2：商店列表
        """
        if level == 0:
            # 商店列表
            return [{'func': lambda data: func(data, level + 1), 'data': s} for s in fetch_store_list(data)]
        if level == 1:
            # 商店
            return [{'func': None, 'data': s} for s in fetch_stores(data)]
        else:
            return ()

    # Walk from the root node, where level == 1.
    if data is None:
        data = {'url': 'http://cn.boucheron.com/en_cn/stores.html',
                'brand_id': 10050, 'brandname_e': u'Boucheron', 'brandname_c': u'宝诗龙'}

    global db
    db = cm.StoresDb()
    db.connect_db(user=user, passwd=passwd)
    db.execute(u'DELETE FROM %s WHERE brand_id=%d' % ('stores', data['brand_id']))

    results = cm.walk_tree({'func': lambda data: func(data, 0), 'data': data})
    db.disconnect_db()
    cm.dump('Done!', log_name)

    return results


