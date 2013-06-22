# coding=utf-8
import json
import string
import re
import common as cm
import geosense as gs

__author__ = 'Zephyre'

db = None
log_name = 'armani_log.txt'


def fetch_countries(data):
    country_list = ['AF', 'AR', 'AM', 'AU', 'AT', 'AZ', 'BH', 'BY', 'BE', 'BR', 'CA', 'CL', 'CN', 'HR', 'CZ',
                    'DK', 'EG', 'EN', 'EE', 'FI', 'FR', 'FX', 'DE', 'GR', 'GU', 'HU', 'IN', 'ID', 'IL', 'IT',
                    'JP', 'JO', 'KZ', 'KR', 'KW', 'LV', 'LB', 'LT', 'LU', 'MY', 'MX', 'MN', 'MA', 'NL', 'NZ',
                    'PA', 'PY', 'PH', 'PL', 'PT', 'QA', 'RU', 'SA', 'RS', 'SG', 'SI', 'ES', 'CH', 'TW', 'TH',
                    'TR', 'UA', 'AE', 'UK', 'US', 'VE', 'VN', 'VG']
    results = []
    for c in country_list:
        d = data.copy()
        d['country_code'] = c
        results.append(d)
    return results


def fetch_stores(data):
    url = data['url']
    param = {'country_id': data['country_code'], 'city': '', 'label_id': '', 'lang': 'en'}
    try:
        body = cm.get_data(url, param)
    except Exception:
        cm.dump('Error in fetching stores: %s, %s' % (url, param), log_name)
        return []

    start = body.find(ur'<stores>')
    if start == -1:
        cm.dump('Error in fetching stores: %s, %s' % (url, param), log_name)
        return []
    body = cm.extract_closure(body[start:], ur'<stores>', ur'</stores>')[0]

    store_list=[]
    for m in re.findall(ur'<store\b[^<>]+>(.+?)</store>', body):
        entry = cm.init_store_entry(data['brand_id'], data['brandname_e'], data['brandname_c'])
        entry[cm.country_e] = data['country_code']
        m1 = re.search(ur'<name>(.+?)</name>', m)
        if m1 is not None:
            entry[cm.name_e] = cm.reformat_addr(m1.group(1).strip())
        m1 = re.search(ur'<address>(.+?)</address>', m)
        if m1 is not None:
            entry[cm.addr_e] = cm.reformat_addr(m1.group(1).strip())
        m1 = re.search(ur'<city>(.+)</city>', m)
        if m1 is not None:
            entry[cm.city_e] = cm.html2plain(m1.group(1).strip().upper())
        m1 = re.search(ur'<zip>(.+?)</zip>', m)
        if m1 is not None:
            entry[cm.zip_code] = m1.group(1).strip()
        m1 = re.search(ur'<tel>(.+?)</tel>', m)
        if m1 is not None:
            entry[cm.tel] = m1.group(1).strip()
        m1 = re.search(ur'<fax>(.+?)</fax>', m)
        if m1 is not None:
            entry[cm.fax] = m1.group(1).strip()
        m1 = re.search(ur'<email>(.+?)</email>', m)
        if m1 is not None:
            entry[cm.email] = m1.group(1).strip()
        m1 = re.search(ur'<link>(.+?)</link>', m)
        if m1 is not None:
            entry[cm.url] = m1.group(1).strip()
        gs.field_sense(entry)
        ret = gs.addr_sense(entry[cm.addr_e], entry[cm.country_e])
        if ret[1] is not None:
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
            # 商店
            return [{'func': None, 'data': s} for s in fetch_stores(data)]
        else:
            return []

    # Walk from the root node, where level == 1.
    if data is None:
        data = {'url': 'http://www.armanistores.com/storefinder/storesXML.jsp',
                'brand_id': 10149, 'brandname_e': u'Giorgio Armani', 'brandname_c': u'乔治阿玛尼'}

    global db
    db = cm.StoresDb()
    db.connect_db(user=user, passwd=passwd)
    db.execute(u'DELETE FROM %s WHERE brand_id=%d' % ('stores', data['brand_id']))

    results = cm.walk_tree({'func': lambda data: func(data, 0), 'data': data})
    db.disconnect_db()

    return results