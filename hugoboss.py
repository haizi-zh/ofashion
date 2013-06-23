# coding=utf-8
import json
import string
import re
import common as cm
import geosense as gs

__author__ = 'Zephyre'

db = None
log_name = 'gucci_log.txt'


def fetch_stores(data):
    url = data['url']
    try:
        body = cm.get_data(url)
    except Exception:
        cm.dump('Error in fetching stores: %s' % url, log_name)
        return []

    raw = json.loads(body)
    store_list = []
    for s in raw:
        entry = cm.init_store_entry(data['brand_id'], data['brandname_e'], data['brandname_c'])
        entry[cm.name_e] = s['storename']
        entry[cm.addr_e] = cm.reformat_addr(', '.join([s['building'].replace(u'operated by ', u''),
                                                       s['street'].strip()]))

        if s['country'] is not None:
            entry[cm.country_e] = s['country'].strip().upper()
        if s['city'] is not None:
            if s['country'].strip() == u'US':
                tmp = s['city'].split(',')
                entry[cm.city_e] = tmp[0].strip().upper()
                if len(tmp) > 1:
                    entry[cm.province_e] = tmp[1].strip().upper()
            else:
                entry[cm.city_e] = s['city'].strip().upper()

        if s['zip'] is not None:
            entry[cm.zip_code] = s['zip'].strip()
        if s['phone'] is not None:
            entry[cm.tel] = s['phone'].strip()
        if s['storeemail'] is not None:
            entry[cm.email] = s['storeemail'].strip()
        if s['storelink'] is not None and u'@' not in s['storelink']:
            entry[cm.url] = s['storelink'].strip()
        if s['storetype'] is not None:
            entry[cm.store_class] = s['storetype'].strip()
        hours = []
        for item in ['monday', 'tuesday', 'wednesday', 'thursday', 'friday', 'saturday', 'sunday']:
            if s[item] is not None:
                hours.append('%s: %s' % (item, s[item]))
        entry[cm.hours] = ', '.join(hours)
        styles = []
        for item in ['menswear', 'womenswear', 'kidswear']:
            if s[item] == '1':
                styles.append(item)
        entry[cm.store_type] = ', '.join(styles)
        if s['latitude'] is not None and s['latitude'].strip() != '':
            entry[cm.lat] = string.atof(s['latitude'])
        if s['longitude'] is not None and s['longitude'].strip() != '':
            entry[cm.lng] = string.atof(s['longitude'])

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
        data = {
            'url': 'http://www.hugoboss.com/us/en/storeLocator/storerequest.json?tx_hbstorelocator_storerequest%5Baction%5D=findStoresInQuadrant&tx_hbstorelocator_storerequest%5BbottomLeftLatitude%5D=-89.10587049475817&tx_hbstorelocator_storerequest%5BbottomLeftLongitude%5D=-179.66012576777348&tx_hbstorelocator_storerequest%5BtopRightLatitude%5D=89.208044628330452&tx_hbstorelocator_storerequest%5BtopRightLongitude%5D=179.4909668322266',
            'brand_id': 10169, 'brandname_e': u'Hugo Boss', 'brandname_c': u'雨果·博斯'}

    global db
    db = cm.StoresDb()
    db.connect_db(user=user, passwd=passwd)
    db.execute(u'DELETE FROM %s WHERE brand_id=%d' % ('stores', data['brand_id']))

    results = cm.walk_tree({'func': lambda data: func(data, 0), 'data': data})
    db.disconnect_db()

    return results
