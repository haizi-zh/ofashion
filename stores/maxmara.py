# coding=utf-8
import string
import re
import urllib
from stores import geosense as gs

__author__ = 'Zephyre'

db = None
log_name = 'gucci_log.txt'


def fetch_countries(data):
    url = data['url']
    try:
        body = cm.get_data(url)
    except Exception:
        cm.dump('Error in fetching countries: %s' % url, log_name)
        return []

    start = body.find(ur'<fieldset class="countryDialog">')
    body = cm.extract_closure(body[start:], ur'<select\b', ur'</select')[0]
    results = []
    for m in re.findall(ur'<option[^<>]+value="([^"]+)"', body):
        d = data.copy()
        d['url'] = '%s%s/%s' % (data['host'], data['data_url'], m.strip().replace(' ', '-'))
        d['country'] = m.strip().upper()
        results.append(d)
    return results


def fetch_cities(data):
    url = data['url']
    try:
        body = cm.get_data(url)
    except Exception:
        cm.dump('Error in fetching cities: %s' % url, log_name)
        return []

    start = body.find(ur'<fieldset class="cityDialog">')
    body = cm.extract_closure(body[start:], ur'<select\b', ur'</select')[0]
    results = []
    for m in re.findall(ur'<option[^<>]+value="([^"]+)"', body):
        d = data.copy()
        d['url'] = '%s/%s' % (data['url'], urllib.quote(m.strip().encode('utf-8')))
        d['city'] = m.strip().upper()
        results.append(d)
    return results


def fetch_stores(data):
    url = data['url']
    try:
        body = cm.get_data(url)
    except Exception:
        cm.dump('Error in fetching stores: %s' % url, log_name)
        return []

    store_list = []
    for m in re.finditer(ur'<div class="searchResult[^"]*"', body):
        if 'intro' in m.group():
            continue

        sub = cm.extract_closure(body[m.start():], ur'<div\b', ur'</div>')[0]
        m1 = re.search(ur'<div id=[^<>]+>(.+?)</div>', sub)
        if m1 is None:
            continue

        entry = cm.init_store_entry(data['brand_id'], data['brandname_e'], data['brandname_c'])
        entry[cm.country_e] = data['country']
        entry[cm.city_e] = data['city']

        addr_list = [tmp.strip() for tmp in cm.reformat_addr(m1.group(1)).split(',')]
        tel = cm.extract_tel(addr_list[-1])
        if tel != '':
            entry[cm.tel] = tel
            del addr_list[-1]
        else:
            m1 = re.search(ur'Tel:([^<>]+)', sub)
            if m1 is not None:
                entry[cm.tel] = cm.extract_tel(m1.group(1))
        entry[cm.addr_e] = ', '.join(addr_list)

        m1 = re.search(ur"show_map\('(-?\d+\.\d+)'\s*,\s*'(-?\d+\.\d+)'", sub)
        if m1 is not None:
            entry[cm.lat] = string.atof(m1.group(1))
            entry[cm.lng] = string.atof(m1.group(2))

        start = sub.find(ur'Opening hours:')
        if start != -1:
            entry[cm.hours] = cm.extract_closure(sub[start:], ur'<p>', ur'</p>')[0].strip()

        ret = None
        if entry[cm.lat]!='' and entry[cm.lng]!='':
            ret = gs.geocode(latlng='%f,%f' % (entry[cm.lat], entry[cm.lng]))
        if ret is None:
            tmp = [tmp1.strip() for tmp1 in entry[cm.addr_e].split(',')]
            if 'Max Mara' in tmp[0]:
                del tmp[0]
            if len(tmp) > 0:
                ret = gs.geocode(', '.join(tmp))
        if ret is not None:
            city = ''
            province = ''
            country = ''
            zip_code = ''
            tmp = ret[0]['address_components']
            for v in tmp:
                if 'locality' in v['types']:
                    city = v['long_name'].strip().upper()
                elif 'administrative_area_level_1' in v['types']:
                    province = v['long_name'].strip().upper()
                elif 'country' in v['types']:
                    country = v['long_name'].strip().upper()
                elif 'postal_code' in v['types']:
                    zip_code = v['long_name'].strip()
            entry[cm.country_e] = country
            entry[cm.province_e] = province
            entry[cm.city_e] = city
            entry[cm.zip_code] = zip_code

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
        data = {'url': 'http://www.maxmara.com/en/MaxMara-Stores',
                'host': 'http://www.maxmara.com',
                'data_url': '/en/MaxMara-Stores',
                'brand_id': 10248, 'brandname_e': u'MaxMara', 'brandname_c': u'麦丝玛拉'}

    global db
    db = cm.StoresDb()
    db.connect_db(user=user, passwd=passwd)
    db.execute(u'DELETE FROM %s WHERE brand_id=%d' % ('stores', data['brand_id']))

    results = cm.walk_tree({'func': lambda data: func(data, 0), 'data': data})
    db.disconnect_db()

    return results

