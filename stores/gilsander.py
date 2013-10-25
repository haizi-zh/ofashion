# coding=utf-8
import json
import string
import re
from stores import geosense as gs

__author__ = 'Zephyre'

db = None


def fetch_continents(data):
    urls=[{'continent':'ASIA','url':'/en/jil_store_countries.json?continent=asia'},
          {'continent':'EUROPE','url':'/en/jil_store_countries.json?continent=europe'},
          {'continent':'NORTH AMERICA','url':'/en/jil_store_countries.json?continent=north-america'}]

    results = []
    for value in urls:
        d=data.copy()
        d['continent']=value['continent']
        d['url']=data['host']+value['url']
        results.append(d)
    return results


def fetch_countries(data):
    url = data['url']
    try:
        body = cm.get_data(url)
    except Exception:
        print 'Error occured: %s' % url
        dump_data = {'level': 0, 'time': cm.format_time(), 'data': {'url': url}, 'brand_id': data['brand_id']}
        cm.dump(dump_data)
        return []

    raw=json.loads(body)
    results=[]
    for key in raw:
        d=data.copy()
        d['country_code']=key
        results.append(d)
    return results


def fetch_cities(data):
    url = data['host']+'/en/jil_store_cities.json'
    try:
        body = cm.get_data(url, {'country':data['country_code']})
    except Exception:
        print 'Error occured: %s' % url
        dump_data = {'level': 0, 'time': cm.format_time(), 'data': {'url': url}, 'brand_id': data['brand_id']}
        cm.dump(dump_data)
        return []

    raw=json.loads(body)
    results=[]
    for key in raw:
        d=data.copy()
        d['city']=key
        results.append(d)
    return results


def fetch_stores(data):
    url = data['host']+'/en/jil_store_stores.json'
    try:
        body = cm.get_data(url, {'city':data['city']})
    except Exception:
        print 'Error occured: %s' % url
        dump_data = {'level': 0, 'time': cm.format_time(), 'data': {'url': url}, 'brand_id': data['brand_id']}
        cm.dump(dump_data)
        return []

    raw=json.loads(body)

    store_list=[]
    for store in raw:
        entry = cm.init_store_entry(data['brand_id'], data['brandname_e'], data['brandname_c'])
        entry[cm.name_e]=store['title']
        entry[cm.country_e]=store['country'].strip().upper()
        entry[cm.city_e]=store['city'].strip().upper()
        entry[cm.lat]=string.atof(store['lat'])
        entry[cm.lng]=string.atof(store['lng'])
        entry[cm.tel]=store['telephone']

        addr=store['address']
        terms=addr.split(',')
        m = re.search(ur'\d{5,}', terms[-1])
        if m is not None:
            entry[cm.zip_code] = m.group(0)
        entry[cm.addr_e]=addr

        gs.field_sense(entry)
        print '(%s / %d) Found store: %s, %s (%s, %s)' % (data['brandname_e'], data['brand_id'],
                                                      entry[cm.name_e], entry[cm.addr_e], entry[cm.country_e],
                                                      entry[cm.continent_e])
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
            # 洲列表
            return [{'func': lambda data: func(data, 1), 'data': s} for s in fetch_continents(data)]
        if level == 1:
            # 国家列表
            return [{'func': lambda data: func(data, 2), 'data': s} for s in fetch_countries(data)]
        if level == 2:
            # 城市列表
            return [{'func': lambda data: func(data, 3), 'data': s} for s in fetch_cities(data)]
        if level == 3:
            # 商店详情
            return [{'func': None, 'data': s} for s in fetch_stores(data)]
        else:
            return []

    # Walk from the root node, where level == 1.
    if data is None:
        data = {'host': 'http://www.jilsander.com',
                'brand_id': 10183, 'brandname_e': u'Jil Sander', 'brandname_c': u'吉尔·桑达'}

    global db
    db = cm.StoresDb()
    db.connect_db(user=user, passwd=passwd)
    db.execute(u'DELETE FROM %s WHERE brand_id=%d' % ('stores', data['brand_id']))

    results = cm.walk_tree({'func': lambda data: func(data, 0), 'data': data})
    db.disconnect_db()

    return results