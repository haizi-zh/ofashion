# coding=utf-8
import json
import string
import re
import common as cm
from stores import geosense as gs

__author__ = 'Zephyre'

db = None
log_name = 'stella_mccartney_log.txt'


def fetch_countries(data):
    url = data['url']
    try:
        body = cm.get_data(url)
    except Exception, e:
        cm.dump('Error in fetching countries: %s' % url, log_name)
        return []

    start = body.find(ur'<select id="storelocator-select-country"')
    if start == -1:
        cm.dump('Error in fetching countries: %s' % url, log_name)
        return []
    body = cm.extract_closure(body[start:], ur'<select\b', ur'</select>')[0]
    results = []
    for m in re.findall(ur'<option value="(\d+)"\s*>([^<>]+)</option>', body):
        id = string.atoi(m[0])
        if id == 0:
            continue
        d = data.copy()
        d['country_id'] = id
        d['country'] = cm.html2plain(m[1]).strip().upper()
        results.append(d)
    return results


def fetch_cities(data):
    url = data['data_url']
    param = {'action': 'yoox_storelocator_change_country', 'country_id': data['country_id'], 'dataType': 'JSON'}
    try:
        body = cm.post_data(url, param)
    except Exception, e:
        cm.dump('Error in fetching cities: %s, %s' % (url, param), log_name)
        return []

    sub = json.loads(body)['html']['select']
    results = []
    for m in re.findall(ur'<option value="(\d+)"\s*>([^<>]+)</option>', sub):
        id = string.atoi(m[0])
        if id == 0:
            continue
        d = data.copy()
        d['city_id'] = id
        d['city'] = cm.html2plain(m[1]).strip().upper()
        results.append(d)
    return results


def fetch_stores(data):
    url = data['data_url']
    param = {'action': 'yoox_storelocator_change_city', 'city_id': data['city_id'], 'dataType': 'JSON'}
    try:
        body = cm.post_data(url, param)
    except Exception, e:
        cm.dump('Error in fetching stores: %s, %s' % (url, param), log_name)
        return []

    sub = json.loads(body)['html']['boxes']

    store_list = []
    for m in re.finditer(ur'<li[^<>]+rel="(\d+)">', sub):
        entry = cm.init_store_entry(data['brand_id'], data['brandname_e'], data['brandname_c'])
        entry[cm.country_e] = data['country']
        entry[cm.city_e] = data['city']

        store_id = string.atoi(m.group(1))
        for s in json.loads(body)['data']['stores']:
            if s['id'] == store_id:
                if s['latlong']['lat'] != '':
                    entry[cm.lat] = string.atof(s['latlong']['lat'])
                    entry[cm.lng] = string.atof(s['latlong']['lng'])
                break

        store_sub = cm.extract_closure(sub[m.start():], ur'<li\b', ur'</li>')[0]

        m1 = re.search(ur'<div class="storelocator-item-title">([^<>]+)</div>', store_sub)
        if m1 is not None:
            entry[cm.name_e] = cm.html2plain(m1.group(1)).strip()

        m1 = re.search(ur'<div class="storelocator-item-address">([^<>]+)</div>', store_sub)
        if m1 is not None:
            entry[cm.addr_e] = cm.reformat_addr(m1.group(1)).strip()

        m1 = re.search(ur'<div class="storelocator-item-phone">([^<>]+)</div>', store_sub)
        if m1 is not None:
            entry[cm.tel] = cm.extract_tel(m1.group(1))

        m1 = re.search(ur'<div class="storelocator-item-fax">([^<>]+)</div>', store_sub)
        if m1 is not None:
            entry[cm.fax] = cm.extract_tel(m1.group(1))

        m1 = re.search(ur'<div class="storelocator-item-email">([^<>]+)</div>', store_sub)
        if m1 is not None:
            entry[cm.email] = cm.extract_email(m1.group(1))

        m1 = re.search(ur'<div class="storelocator-item-hours">([^<>]+)</div>', store_sub)
        if m1 is not None:
            entry[cm.hours] = m1.group(1).strip()

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
            # 城市列表
            return [{'func': lambda data: func(data, level + 1), 'data': s} for s in fetch_cities(data)]
        if level == 2:
            # 商店
            return [{'func': None, 'data': s} for s in fetch_stores(data)]
        else:
            return []

    # Walk from the root node, where level == 1.
    if data is None:
        data = {'url': 'http://www.stellamccartney.com/experience/en/stellas-world/store-locator/',
                'data_url': 'http://www.stellamccartney.com/experience/en/wpapi/ajax-service/',
                'brand_id': 10333, 'brandname_e': u'Stella McCartney', 'brandname_c': u'斯特拉·麦卡特尼'}

    global db
    db = cm.StoresDb()
    db.connect_db(user=user, passwd=passwd)
    db.execute(u'DELETE FROM %s WHERE brand_id=%d' % ('stores', data['brand_id']))

    results = cm.walk_tree({'func': lambda data: func(data, 0), 'data': data})
    db.disconnect_db()

    return results


