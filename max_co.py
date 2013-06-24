# coding=utf-8
import json
import string
import re
import common as cm
import geosense as gs

__author__ = 'Zephyre'

db = None
log_name = 'max_co_log.txt'


def fetch_stores(data):
    url = data['url']
    try:
        body = cm.get_data(url)
    except Exception:
        cm.dump('Error in fetching stores: %s' % url, log_name)
        return []

    store_list = []
    for m in re.finditer(ur'<div\s+class\s*=\s*"storeItem"', body):
        entry = cm.init_store_entry(data['brand_id'], data['brandname_e'], data['brandname_c'])

        sub = cm.extract_closure(body[m.end():], ur'<div\b', ur'</div>')[0]
        m1 = re.search(ur'<div class="bubbleInfo">(.+?)</div>', sub)
        if m1 is not None:
            entry[cm.addr_e] = cm.reformat_addr(m1.group(1))
        m1 = re.search(ur'lat="(-?\d+\.\d+)"', sub)
        if m1 is not None:
            entry[cm.lat] = string.atof(m1.group(1))
        m1 = re.search(ur'lng="(-?\d+\.\d+)"', sub)
        if m1 is not None:
            entry[cm.lng] = string.atof(m1.group(1))
        m1 = re.search(ur'<span>\s*Tel:\s*([^<>]+)</span>', sub)
        if m1 is not None:
            entry[cm.tel] = m1.group(1).strip()
        m1 = re.search(ur'http://maps\.google\.com/maps\?q=([^&"]+)', sub)
        if m1 is None:
            continue
        ret = gs.geocode(latlng=m1.group(1))
        if ret is None:
            tmp = [tmp1.strip() for tmp1 in entry[cm.addr_e].split(',')]
            if 'MAX' in tmp[0]:
                del tmp[0]
            if cm.extract_tel(tmp[-1])!='':
                del tmp[-1]
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
        else:
            cm.dump('Error in fetching stores: latlng=%s, addr=%s' % (m1.group(1), entry[cm.addr_e]), log_name)
            continue

    return store_list


def fetch(level=1, data=None, user='root', passwd=''):
    def func(data, level):
        """
        :param data:
        :param level: 0：国家；1：城市；2：商店列表
        """
        if level == 0:
            # 商店
            return [{'func': None, 'data': s} for s in fetch_stores(data)]
        else:
            return []

    # Walk from the root node, where level == 1.
    if data is None:
        data = {'url': 'http://www.maxandco.com/it/Negozi-Ricerca/USA/Costa%20Mesa',
                'brand_id': 10247, 'brandname_e': u'MAX&Co.', 'brandname_c': u'MAX&Co.'}

    global db
    db = cm.StoresDb()
    db.connect_db(user=user, passwd=passwd)
    db.execute(u'DELETE FROM %s WHERE brand_id=%d' % ('stores', data['brand_id']))

    results = cm.walk_tree({'func': lambda data: func(data, 0), 'data': data})
    db.disconnect_db()

    return results
