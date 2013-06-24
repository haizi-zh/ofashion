# coding=utf-8
import json
import string
import re
import common as cm
import geosense as gs

__author__ = 'Zephyre'

db = None
log_name = 'loewe_log.txt'


def fetch_countries(data):
    url = data['home_url']
    try:
        body = cm.get_data(url)
    except Exception:
        cm.dump('Error in fetching countries: %s' % url, log_name)
        return []

    m = re.search(ur'<select\s+name\s*=\s*"country"\s+id\s*=\s*"storelocator_country">', body)
    if m is None:
        cm.dump('Error in fetching countries: %s' % url, log_name)
        return []
    body = cm.extract_closure(body[m.start():], ur'<select\b', ur'</select>')[0]
    results = []
    for m in re.findall(ur'<option value="([A-Z]{2})"', body):
        d = data.copy()
        d['country_code'] = m
        results.append(d)
    return results


def fetch_cities(data):
    url = data['data_url']
    param = {'country': data['country_code']}
    try:
        body = cm.post_data(url, param)
    except Exception:
        cm.dump('Error in fetching cities: %s, %s' % (url, param), log_name)
        return []

    results = []
    for m in re.findall(ur'<option value="([^"]+)">([^<>]+)', body):
        d = data.copy()
        d['city'] = m[1].strip().upper()
        d['city_code'] = m[0]
        results.append(d)
    return results


def fetch_stores(data):
    url = data['store_url']
    param = {'store_country': data['country_code'], 'store_city': data['city_code']}
    try:
        body = cm.post_data(url, param)
    except Exception:
        cm.dump('Error in fetching stores: %s, %s' % (url, param), log_name)
        return []

    store_list = []
    for s in re.findall(ur'<marker\b([^<>]+)/\s*>', body):
        entry = cm.init_store_entry(data['brand_id'], data['brandname_e'], data['brandname_c'])
        m = re.search(ur'store_name="([^"]+)"', s)
        if m is not None:
            entry[cm.name_e] = cm.reformat_addr(m.group(1))
        entry[cm.country_e] = data['country_code']
        entry[cm.city_e] = data['city']
        addr_list = []
        for key in ['store_mall_name', 'store_address', 'store_zip_code']:
            m = re.search(ur'%s="([^"]+)"' % key, s)
            if m is not None:
                tmp = cm.reformat_addr(m.group(1))
                if tmp != '':
                    addr_list.append(tmp)
        entry[cm.addr_e] = ', '.join(addr_list)
        m = re.search(ur'store_zip_code="([^"]+)"', s)
        if m is not None:
            entry[cm.zip_code] = m.group(1).strip()
        m = re.search(ur'store_telephone="([^"]+)"', s)
        if m is not None:
            entry[cm.tel] = m.group(1).strip()
        m = re.search(ur'store_fax="([^"]+)"', s)
        if m is not None:
            entry[cm.fax] = m.group(1).strip()
        m = re.search(ur'store_email="([^"]+)"', s)
        if m is not None:
            entry[cm.email] = m.group(1).strip()
        m = re.search(ur'store_latitude="([^"]+)"', s)
        if m is not None:
            entry[cm.lat] = string.atof(m.group(1).strip())
        m = re.search(ur'store_longitude="([^"]+)"', s)
        if m is not None:
            entry[cm.lng] = string.atof(m.group(1).strip())

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
        data = {'home_url': 'http://www.loewe.com/cn_en/storelocator/location/searchindex',
                'data_url': 'http://www.loewe.com/cn_en/storelocator/location/ajax',
                'store_url': 'http://www.loewe.com/cn_en/storelocator/location/search',
                'brand_id': 10220, 'brandname_e': u'Loewe', 'brandname_c': u'罗意威'}

        global db
        db = cm.StoresDb()
        db.connect_db(user=user, passwd=passwd)
        db.execute(u'DELETE FROM %s WHERE brand_id=%d' % ('stores', data['brand_id']))

        results = cm.walk_tree({'func': lambda data: func(data, 0), 'data': data})
        db.disconnect_db()

        return results