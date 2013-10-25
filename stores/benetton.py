# coding=utf-8
import string
import re
from stores import geosense as gs

__author__ = 'Zephyre'

db = None


def fetch_countries(data):
    url = data['home_url']
    try:
        body = cm.get_data(url)
    except Exception:
        cm.dump('Error occured in fetching country list: %s' % url, 'benetton_log.txt', False)
        return []

    start = body.find(u'<td><strong>Select Country</strong></td>')
    if start == -1:
        cm.dump('Error occured in fetching country list: %s' % url, 'benetton_log.txt', False)
    body = cm.extract_closure(body[start:], ur'<select\b', ur'</select>')[0]

    results = []
    for m in re.findall(ur'<option value="(.+?)">.+?</option>', body):
        d = data.copy()
        d['country'] = m
        results.append(d)
    return results


def fetch_cities(data):
    url = data['data_url']
    try:
        body = cm.get_data(url, {'country_code': data['country'], 'toget': 'citylist'})
    except Exception:
        cm.dump('Error in fetching cities: %s, %s' % (url, data['country']), 'benetton_log.txt', False)
        dump_data = {'level': 0, 'time': cm.format_time(), 'data': {'url': url}, 'brand_id': data['brand_id']}
        cm.dump(dump_data)
        return []

    results = []
    for m in re.findall(ur'<option value=\\"(.+?)\\">', body):
        d = data.copy()
        d['city'] = m.strip().upper()
        results.append(d)
    return results


def fetch_stores(data):
    # country=Greece&city=ATHENS&adutl=+01&kids=+02&undercolor=+06&togetmap=mapdata
    url = data['data_url']
    param = {'country': data['country'], 'city': data['city'], 'adutl': ' 01', 'kids': ' 02',
             'undercolor': ' 06', 'togetmap': 'mapdata'}
    try:
        body = cm.get_data(url, param)
    except Exception:
        cm.dump('Error in fetching stores: %s, %s' % (url, param), 'benetton_log.txt', False)
        dump_data = {'level': 0, 'time': cm.format_time(), 'data': {'url': url}, 'brand_id': data['brand_id']}
        cm.dump(dump_data)
        return []

    store_list = []
    for m in re.findall(ur'<marker (.+?)>', body):
        entry = cm.init_store_entry(data['brand_id'], data['brandname_e'], data['brandname_c'])
        m1 = re.search(ur'name=\\"(.+?)\\"', m)
        if m1 is not None:
            entry[cm.name_e] = cm.html2plain(m1.group(1).strip().replace(u'\\', ''))
        m1 = re.search(ur'address=\\"(.+?)\\"', m)
        if m1 is not None:
            addr = cm.reformat_addr(cm.html2plain(m1.group(1)).replace(u'\\', ''))
            tel = cm.extract_tel(addr)
            if tel != '':
                entry[cm.tel] = tel
                addr = addr.replace(tel, '')
            entry[cm.addr_e] = cm.reformat_addr(addr)

        m1 = re.search(ur'lat=\\"(.+?)\\"', m)
        if m1 is not None:
            entry[cm.lat] = string.atof(m1.group(1))

        m1 = re.search(ur'lng=\\"(.+?)\\"', m)
        if m1 is not None:
            entry[cm.lng] = string.atof(m1.group(1))

        entry[cm.country_e] = data['country'].strip().upper()
        entry[cm.city_e] = cm.extract_city(data['city'])[0]
        gs.field_sense(entry)
        cm.dump('(%s / %d) Found store: %s, %s (%s, %s)' % (data['brandname_e'], data['brand_id'],
                                                            entry[cm.name_e], entry[cm.addr_e], entry[cm.country_e],
                                                            entry[cm.continent_e]), 'benetton_log.txt', False)
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
        data = {'home_url': 'http://www.benetton.com/store-locator/',
                'data_url': 'http://www.benetton.com/storelocator/map_ajax.php',
                'brand_id': 10038, 'brandname_e': u'Benetton', 'brandname_c': u'贝纳通'}

    global db
    db = cm.StoresDb()
    db.connect_db(user=user, passwd=passwd)
    db.execute(u'DELETE FROM %s WHERE brand_id=%d' % ('stores', data['brand_id']))

    results = cm.walk_tree({'func': lambda data: func(data, 0), 'data': data})
    db.disconnect_db()

    return results