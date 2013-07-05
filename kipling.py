# coding=utf-8
import json
import string
import re
import common as cm
import geosense as gs

__author__ = 'Zephyre'

db = None


def fetch_countries(data):
    url = data['home_url']
    try:
        body = cm.get_data(url)
    except Exception:
        print 'Error occured: %s' % url
        dump_data = {'level': 0, 'time': cm.format_time(), 'data': {'url': url}, 'brand_id': data['brand_id']}
        cm.dump(dump_data)
        return []

    start = body.find(u'<select id="countries"')
    if start == -1:
        print 'Error in fetching countries.'
        return []
    body = cm.extract_closure(body[start:], ur'<select\b', ur'</select>')[0]

    results = []
    for m in re.findall(ur'<option value="(.+?)">.+?</option>', body):
        d = data.copy()
        d['country'] = m
        results.append(d)
    return results


def fetch_cities(data):
    url = data['city_url']
    try:
        body = cm.get_data(url, {'country': data['country']})
    except Exception:
        print 'Error occured: %s, %s' % (url, data['country'])
        dump_data = {'level': 0, 'time': cm.format_time(), 'data': {'url': url}, 'brand_id': data['brand_id']}
        cm.dump(dump_data)
        return []

    raw = json.loads(body)
    results = []
    for item in raw['items']:
        d = data.copy()
        d['city'] = item['city']
        results.append(d)
    return results


def fetch_stores(data):
    url = data['store_url']
    try:
        body = cm.get_data(url, {'country': data['country'], 'city': data['city']})
    except Exception:
        print 'Error occured: %s' % url
        dump_data = {'level': 0, 'time': cm.format_time(), 'data': {'url': url}, 'brand_id': data['brand_id']}
        cm.dump(dump_data)
        return []

    raw = json.loads(body)
    store_list = []

    for item in raw['items']:
        entry = cm.init_store_entry(data['brand_id'], data['brandname_e'], data['brandname_c'])
        entry[cm.country_e] = data['country'].strip().upper()
        tmp = cm.extract_city(data['city'])[0]
        if entry[cm.country_e] == 'USA':
            entry[cm.province_e] = tmp
        else:
            entry[cm.city_e] = tmp
        gs.field_sense(entry)

        addr = cm.reformat_addr(item['address'].replace(u'\\', ''))
        addr_list = [tmp.strip() for tmp in addr.split(',')]
        tel = cm.extract_tel(addr_list[-1])
        if tel !='':
            entry[cm.tel]=tel
            del addr_list[-1]
        entry[cm.addr_e]=', '.join(addr_list)
        entry[cm.store_type] = item['shop_type']

        gs.field_sense(entry)
        ret = gs.addr_sense(entry[cm.addr_e])
        if ret[0] is not None and entry[cm.country_e] == '':
            entry[cm.country_e] = ret[0]
        if ret[1] is not None and entry[cm.province_e] == '':
            entry[cm.province_e] = ret[1]
        if ret[2] is not None and entry[cm.city_e] == '':
            entry[cm.city_e] = ret[2]
        gs.field_sense(entry)

        print '(%s / %d) Found store: %s, %s (%s, %s)' % (data['brandname_e'], data['brand_id'],
                                                          entry[cm.name_e], entry[cm.addr_e], entry[cm.country_e],
                                                          entry[cm.continent_e])
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
            return [{'func': lambda data: func(data, 1), 'data': s} for s in fetch_countries(data)]
        if level == 1:
            # 城市列表
            return [{'func': lambda data: func(data, 2), 'data': s} for s in fetch_cities(data)]
        if level == 2:
            # 商店详情
            return [{'func': None, 'data': s} for s in fetch_stores(data)]
        else:
            return []

    # Walk from the root node, where level == 1.
    if data is None:
        data = {'home_url': 'http://www.kipling.com/int/store-locator/',
                'city_url': 'http://www.kipling.com/int/ustorelocator/ajax/getCitiesByCountry',
                'store_url': 'http://www.kipling.com/int/ustorelocator/ajax/getStores',
                'brand_id': 10196, 'brandname_e': u'Kipling', 'brandname_c': u'吉普林'}

    global db
    db = cm.StoresDb()
    db.connect_db(user=user, passwd=passwd)
    db.execute(u'DELETE FROM %s WHERE brand_id=%d' % ('stores', data['brand_id']))

    results = cm.walk_tree({'func': lambda data: func(data, 0), 'data': data})
    db.disconnect_db()

    return results