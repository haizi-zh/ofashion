# coding=utf-8
import json
import string
import re
import common as cm
import geosense as gs

__author__ = 'Zephyre'

db = None
log_name = 'tiffany_log.txt'


def fetch_stores(data):
    url = data['url']
    try:
        body = cm.post_data(url, {}, {'Content-Type': 'application/json; charset=utf-8',
                                      'Content-Length': 0, 'Connection': 'keep-alive',
                                      'Pragma': 'no-cache', 'Cache-Control': 'no-cache'})
    except Exception, e:
        cm.dump('Error in fetching stores: %s, %s' % (url, e), log_name)
        return []

    store_list = []

    for s in json.loads(body)[u'd']:
        entry = cm.init_store_entry(data['brand_id'], data['brandname_e'], data['brandname_c'])
        entry[cm.name_e] = s['Name'].strip()
        addr_list = []
        for tmp in [s[key].strip() for key in ['Address%d' % i for i in xrange(1, 4)]]:
            if tmp != '':
                addr_list.append(tmp)
        entry[cm.addr_e] = ', '.join(addr_list)
        if s['Lat'] is not None and s['Lat'] != '':
            entry[cm.lat] = string.atof(s['Lat'])
        if s['Long'] is not None and s['Long'] != '':
            entry[cm.lng] = string.atof(s['Long'])
        entry[cm.city_e] = s['City'].strip().upper()
        entry[cm.province_e] = s['State']
        entry[cm.zip_code] = s['Zip']
        entry[cm.country_e] = s['Country']
        if 'N/A' not in s['Phone']:
            entry[cm.tel] = s['Phone']
        entry[cm.email] = s['Email']
        entry[cm.hours] = s['RegularHrs']
        entry[cm.store_type] = s['StorePriorityTypeDisplayName']

        gs.field_sense(entry)
        if entry[cm.province_e] == '':
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
            return [{'func': None, 'data': s} for s in fetch_stores(data)]
        else:
            return []

    # Walk from the root node, where level == 1.
    if data is None:
        data = {'url': 'http://international.tiffany.com/Locations/Default.aspx/GetAllStoreLocationsForMapping',
                'brand_id': 10350, 'brandname_e': u'Tiffany&Co', 'brandname_c': u'蒂芙尼'}

    global db
    db = cm.StoresDb()
    db.connect_db(user=user, passwd=passwd)
    db.execute(u'DELETE FROM %s WHERE brand_id=%d' % ('stores', data['brand_id']))

    results = cm.walk_tree({'func': lambda data: func(data, 0), 'data': data})
    db.disconnect_db()

    return results



