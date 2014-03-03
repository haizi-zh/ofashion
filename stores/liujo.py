# coding=utf-8
import json
import string
import re
from stores import geosense as gs

__author__ = 'Zephyre'

db = None
log_name = 'liujo_log.txt'


def fetch_countries(data):
    url = data['home_url']
    try:
        body = cm.get_data(url)
    except Exception:
        cm.dump('Error in fetching countries: %s' % url, log_name)
        return []

    m = re.search(ur'<select\s+id\s*=\s*"store-locator-country"', body)
    if m is None:
        cm.dump('Error in fetching countries: %s' % url, log_name)
        return []
    body = cm.extract_closure(body[m.start():], ur'<select\b', ur'</select>')[0]
    results = []
    for m in re.findall(ur'<option value="([A-Z]{2})">', body):
        d = data.copy()
        d['country_code'] = m
        # if m.upper() == 'GR':
        results.append(d)
    return results


def fetch_cities(data):
    param = {'action': 'getRegionsFromAjax', 'country': data['country_code']}
    url = data['url']
    try:
        body = cm.post_data(url, param)
    except Exception:
        cm.dump('Error in fetching cities: %s, %s' % (url, param), log_name)
        return []

    results = []
    for city in json.loads(body):
        d = data.copy()
        d['city'] = city
        results.append(d)
    return results


#action=getStoresFromAjax&country=AT&region=AUSTRIA&collection=


def get_detail(data):
    param = {'action': 'loadStoreFromAjax', 'id': data['store_id']}
    url = data['url']
    try:
        body = cm.post_data(url, param)
    except Exception:
        cm.dump('Error in fetching stores: %s, %s' % (url, param), log_name)
        return ()

    m = re.search(ur'<div class="lines">(.+?)</div>', body, re.S)
    if m is None:
        return ()
    return tuple(term.strip() for term in re.findall(ur'<li>(.+?)</li>', m.group(1), re.S))


def fetch_stores(data):
    param = {'action': 'getStoresFromAjax', 'country': data['country_code'],
             'region': data['city'], 'collection': ''}
    url = data['url']
    try:
        body = cm.post_data(url, param)
    except Exception:
        cm.dump('Error in fetching stores: %s, %s' % (url, param), log_name)
        return []

    store_list = []
    for m1 in re.finditer(ur'<div class="shop-type-container">', body):
        sub = cm.extract_closure(body[m1.start():], ur'<div\b', ur'</div>')[0]
        store_class = ''
        m2 = re.search(ur'<div class="shop-type-title">(.+?)</div>', sub, re.S)
        if m2 is not None:
            store_class = cm.reformat_addr(m2.group(1))

        for m2 in re.finditer(ur'<div class="shop"', sub):
            store_sub = cm.extract_closure(sub[m2.start():], ur'<div\b', ur'</div>')[0]
            entry = cm.init_store_entry(data['brand_id'], data['brandname_e'], data['brandname_c'])
            entry[cm.store_class] = store_class
            entry[cm.country_e] = data['country_code']
            entry[cm.city_e] = cm.extract_city(data['city'])[0]

            m3 = re.search(ur'loadStore\((\d+)\s*,\s*(-?\d+\.\d+)\s*,\s*(-?\d+\.\d+)\)', store_sub)
            if m3 is not None:
                data['store_id'] = string.atoi(m3.group(1))
                entry[cm.lat] = string.atof(m3.group(2))
                entry[cm.lng] = string.atof(m3.group(3))
                entry[cm.store_type] = ', '.join(get_detail(data))

            m3 = re.search(ur'<div class="shop-name shop-details shop-main-name">([^<>]+)</div>', store_sub)
            if m3 is not None:
                entry[cm.name_e] = m3.group(1).strip()
            addr_list = []
            m3 = re.search(ur'<div class="shop-street shop-details">([^<>]+)</div>', store_sub)
            if m3 is not None:
                addr_list.append(cm.reformat_addr(m3.group(1)))
            m3 = re.search(ur'<div class="shop-city shop-details">([^<>]+)</div>', store_sub)
            if m3 is not None:
                tmp = cm.reformat_addr(m3.group(1))
                m3 = re.search(ur'(\d{4,})', tmp)
                if m3 is not None:
                    entry[cm.zip_code] = m3.group(1).strip()
                addr_list.append(tmp)
            entry[cm.addr_e] = ', '.join(addr_list)

            ret = gs.addr_sense(entry[cm.addr_e], entry[cm.country_e])
            if ret[1] is not None and entry[cm.province_e] == '':
                entry[cm.province_e] = ret[1]
            gs.field_sense(entry)
            cm.dump('(%s / %d) Found store: %s, %s (%s, %s, %s)' % (data['brandname_e'], data['brand_id'],
                                                                    entry[cm.name_e], entry[cm.addr_e],
                                                                    entry[cm.city_e],
                                                                    entry[cm.country_e],
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
        data = {'url': 'http://www.liujo.com/corporate/wp-admin/admin-ajax.php',
                'home_url': 'http://www.liujo.com/int/corporate/storelocator/',
                'brand_id': 10218, 'brandname_e': u'Liu Jo', 'brandname_c': u'Liu Jo'}

    global db
    db = cm.StoresDb()
    db.connect_db(user=user, passwd=passwd)
    db.execute(u'DELETE FROM %s WHERE brand_id=%d' % ('stores', data['brand_id']))

    results = cm.walk_tree({'func': lambda data: func(data, 0), 'data': data})
    db.disconnect_db()

    return results