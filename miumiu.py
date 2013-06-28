# coding=utf-8
import json
import string
import re
import common as cm
import geosense as gs

__author__ = 'Zephyre'

db = None
log_name = 'miumiu_log.txt'


def fetch_stores(data):
    url = data['url']
    try:
        body = cm.get_data(url)
    except Exception, e:
        cm.dump('Error in fetching stores: %s' % url, log_name)
        return []

    start = body.find(ur'<div data-markers')
    if start == -1:
        cm.dump('Error in fetching stores: %s' % url, log_name)
        return []

    store_list = []
    for s in json.loads(cm.extract_closure(body[start:], ur'\[', ur'\]')[0]):
        entry = cm.init_store_entry(data['brand_id'], data['brandname_e'], data['brandname_c'])
        entry[cm.name_e] = s['name'].strip()
        entry[cm.city_e] = s['city'].strip().upper()
        entry[cm.country_e] = s['country'].strip().upper()
        if s['lat'] is not None and str(s['lat']).strip() != '':
            entry[cm.lat] = string.atof(s['lat'])
        if s['lng'] is not None and str(s['lng']).strip() != '':
            entry[cm.lng] = string.atof(s['lng'])
        cat = s['categories']
        if cat is not None:
            entry[cm.store_type] = ', '.join(cat)

        store_url = 'http://www.miumiu.com/en/store-locator/%s.json' % s['id']
        try:
            details = json.loads(cm.get_data(store_url))
            entry[cm.store_type] = ', '.join(item['name'] for item in details['categories'])
            entry[cm.hours] = details['hours']
            entry[cm.addr_e] = cm.reformat_addr(details['address'])
            m = re.search(re.compile(ur'phone\s*[:\.]\s*(.+?)\s*(fax|$)', re.I), details['phone'])
            if m is not None:
                entry[cm.tel] = m.group(1)
            m = re.search(re.compile(ur'fax\s*[:\.]\s*(.+?)\s*(phone|$)', re.I), details['phone'])
            if m is not None:
                entry[cm.fax] = m.group(1)
        except Exception, e:
            cm.dump('Error in fetching store details: %s' % url, log_name)
            return []

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
            # 商店
            return [{'func': None, 'data': s} for s in fetch_stores(data)]
        else:
            return []

    # Walk from the root node, where level == 1.
    if data is None:
        data = {'url': 'http://www.miumiu.com/en/store-locator?cc=CN',
                'brand_id': 10264, 'brandname_e': u'Miu Miu', 'brandname_c': u'缪缪'}

    global db
    db = cm.StoresDb()
    db.connect_db(user=user, passwd=passwd)
    db.execute(u'DELETE FROM %s WHERE brand_id=%d' % ('stores', data['brand_id']))

    results = cm.walk_tree({'func': lambda data: func(data, 0), 'data': data})
    db.disconnect_db()

    return results

