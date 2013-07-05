# coding=utf-8
import json
import string
import re
import common as cm
import geosense as gs

__author__ = 'Zephyre'

db = None
log_name = 'tommy_log.txt'


def fetch_countries(data):
    url = data['url']
    try:
        body = cm.get_data(url)
    except Exception, e:
        cm.dump('Error in fetching countries: %s' % url, log_name)
        return []


def fetch_stores(data):
    url = data['url']
    try:
        body = cm.post_data(url)
    except Exception, e:
        cm.dump('Error in fetching stores: %s' % url, log_name)
        return []

    store_list = []
    for s in json.loads(body):
        entry = cm.init_store_entry(data['brand_id'], data['brandname_e'], data['brandname_c'])
        entry[cm.country_e]=u'CHINA'

        entry[cm.province_c]=s['region'].strip()
        city = s['city'].replace(u'市','').strip()
        if city=='':
            city = entry[cm.province_c]
        entry[cm.city_c]=city
        entry[cm.district_c]=s['area']
        entry[cm.hours]=s['business']
        entry[cm.name_e]=s['name']
        entry[cm.addr_e]=s['address']
        entry[cm.tel]=s['phone']
        if s['lat']!='':
            entry[cm.lat]=string.atof(s['lat'])
        if s['lng']!='':
            entry[cm.lng]=string.atof(s['lng'])

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
        data = {'url': 'http://www.tommy.com.cn/storelocator/index/getmapajax',
                'brand_id': 10355, 'brandname_e': u'Tommy Hilfiger', 'brandname_c': u'汤米·希尔费格'}

    global db
    db = cm.StoresDb()
    db.connect_db(user=user, passwd=passwd)
    db.execute(u'DELETE FROM %s WHERE brand_id=%d' % ('stores', data['brand_id']))

    results = cm.walk_tree({'func': lambda data: func(data, 0), 'data': data})
    db.disconnect_db()
    cm.dump('Done!', log_name)

    return results


