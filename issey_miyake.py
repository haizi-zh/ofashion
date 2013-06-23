# coding=utf-8
import json
import string
import re
import common as cm
import geosense as gs

__author__ = 'Zephyre'

db = None
log_name = 'issey_miyake_log.txt'


def fetch_stores(data):
    url = data['url']
    try:
        body = cm.get_data(url)
    except Exception:
        cm.dump('Error in fetching stores: %s' % url, log_name)
        return []

    store_list = []
    for m in re.finditer(ur'<item id="\d+">', body):
        sub = cm.extract_closure(body[m.start():], ur'<item\b', ur'</item>')[0]
        entry = cm.init_store_entry(data['brand_id'], data['brandname_e'], data['brandname_c'])
        m1 = re.search(ur'<country>([^<>]+)</country>', sub)
        if m1 is not None:
            tmp = m1.group(1).split('/')
            for v in tmp:
                ret = gs.look_up(v.strip().upper(), 1)
                if ret is not None:
                    entry[cm.country_e] = ret['name_e']
                    break
        m1 = re.search(ur'<city>([^<>]+)</city>', sub)
        if m1 is not None:
            entry[cm.city_e] = m1.group(1).strip().upper()
        m1 = re.search(ur'<brands>([^<>]+)</brands>', sub)
        if m1 is not None:
            tmp = m1.group(1).split('/')
            brand_list = []
            for v in tmp:
                if v.strip() != '':
                    brand_list.append(v)
            entry[cm.store_type] = ', '.join(brand_list)
        m1 = re.search(ur'<name>([^<>]+)</name>', sub)
        if m1 is not None:
            entry[cm.name_e] = m1.group(1).strip()
        m1 = re.search(ur'<address>([^<>]+)</address>', sub)
        if m1 is not None:
            entry[cm.addr_e] = cm.reformat_addr(m1.group(1))
        m1 = re.search(ur'<tel>([^<>]+)</tel>', sub)
        if m1 is not None:
            entry[cm.tel] = m1.group(1).strip()
        m1 = re.search(ur'sll=(-?\d+\.\d+),(-?\d+\.\d+)', sub)
        if m1 is not None:
            entry[cm.lat] = string.atof(m1.group(1))
            entry[cm.lng] = string.atof(m1.group(2))
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
            # 商店
            return [{'func': None, 'data': s} for s in fetch_stores(data)]
        else:
            return []

    # Walk from the root node, where level == 1.
    if data is None:
        data = {'url': 'http://www.isseymiyake.com/en/shops/xml/list.xml',
                'brand_id': 10175, 'brandname_e': u'ISSEY MIYAKE', 'brandname_c': u'三宅一生'}

    global db
    db = cm.StoresDb()
    db.connect_db(user=user, passwd=passwd)
    db.execute(u'DELETE FROM %s WHERE brand_id=%d' % ('stores', data['brand_id']))

    results = cm.walk_tree({'func': lambda data: func(data, 0), 'data': data})
    db.disconnect_db()

    return results