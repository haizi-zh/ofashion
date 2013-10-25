# coding=utf-8
import string
import re
from stores import geosense as gs

__author__ = 'Zephyre'

db = None
log_name = 'tod_log.txt'


def fetch_countries(data):
    url = data['url']
    try:
        body = cm.get_data(url)
    except Exception, e:
        cm.dump('Error in fetching countries: %s' % url, log_name)
        return []

    m = re.search(ur'<div id="countryselect">(.+?)</div>', body)
    if m is None:
        cm.dump('Error in fetching countries: %s' % url, log_name)
        return []
    results = []
    for m1 in re.findall(ur'<option value="(\d+)"\s*>([^<>]+)', m.group(1)):
        d = data.copy()
        d['country_id'] = string.atoi(m1[0])
        d['country'] = cm.html2plain(m1[1]).strip().upper()
        results.append(d)
    return results


def fetch_stores(data):
    url = data['data_url']
    param = {'countryId': data['country_id']}
    try:
        body = cm.post_data(url, param)
    except Exception, e:
        cm.dump('Error in fetching stores: %s, %s' % (url, param), log_name)
        return []

    store_list = []
    for s in re.findall(ur'<store>(.+?)</store>', body):
        entry = cm.init_store_entry(data['brand_id'], data['brandname_e'], data['brandname_c'])
        entry[cm.country_e] = data['country']

        m = re.search(ur'<name>(.+?)</name>', s)
        if m is not None:
            entry[cm.name_e] = cm.html2plain(m.group(1)).strip()

        m = re.search(ur'<brands>(.+?)</brands>', s)
        if m is not None:
            brand_list = []
            for m1 in re.findall(ur'<brand>(.+?)</brand>', m.group(1)):
                brand_list.append(m1)
            entry[cm.store_type] = ', '.join(brand_list)

        m = re.search(ur'<city>(.+?)</city>', s)
        if m is not None:
            entry[cm.city_e] = cm.html2plain(m.group(1)).strip().upper()

        m = re.search(ur'<address>(.+?)</address>', s)
        if m is not None:
            entry[cm.addr_e] = cm.reformat_addr(m.group(1)).strip()

        m = re.search(ur'<phone>(.+?)</phone>', s)
        if m is not None:
            entry[cm.tel] = m.group(1).strip()

        m = re.search(ur'<(?:latitude|latitiude)>(.+?)</(?:latitude|latitiude)>', s)
        if m is not None:
            entry[cm.lat] = string.atof(m.group(1))
        m = re.search(ur'<longitude>(.+?)</longitude>', s)
        if m is not None:
            entry[cm.lng] = string.atof(m.group(1))

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
            # 商店
            return [{'func': None, 'data': s} for s in fetch_stores(data)]
        else:
            return []

    # Walk from the root node, where level == 1.
    if data is None:
        data = {'data_url': 'http://www.tods.com/en/boutiques/index/getStoresByCountry',
                'url': 'http://www.tods.com/en/boutiques/',
                'brand_id': 10354, 'brandname_e': u"Tod's", 'brandname_c': u'托德斯'}

    global db
    db = cm.StoresDb()
    db.connect_db(user=user, passwd=passwd)
    db.execute(u'DELETE FROM %s WHERE brand_id=%d' % ('stores', data['brand_id']))

    results = cm.walk_tree({'func': lambda data: func(data, 0), 'data': data})
    db.disconnect_db()

    return results


