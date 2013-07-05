# coding=utf-8
import json
import string
import re
import common as cm
import geosense as gs
from pyquery import PyQuery as pq
import traceback

__author__ = 'Zephyre'

db = None
log_name = 'paul_joe_log.txt'


def fetch_countries(data):
    url = data['home_url']
    try:
        html = cm.get_data(url)
    except Exception:
        print 'Error occured: %s' % url
        dump_data = {'level': 0, 'time': cm.format_time(), 'data': {'url': url}, 'brand_id': data['brand_id']}
        cm.dump(dump_data)
        return []

    country_list = []
    for item in pq(html)('#country option[value!="reset"]'):
        d = data.copy()
        d['country_id'] = string.atoi(item.attrib['value'])

        country_e = cm.html2plain(item.text).strip().upper()
        ret = gs.look_up(country_e, 1)
        if ret is not None:
            country_e = ret['name_e']
        d['country_e'] = country_e
        country_list.append(d)
    return country_list


def fetch_cities(data):
    url = data['post_city']
    try:
        html = cm.post_data(url, {'country': data['country_id']})
    except Exception:
        print 'Error occured: %s' % url
        dump_data = {'level': 0, 'time': cm.format_time(), 'data': {'url': url}, 'brand_id': data['brand_id']}
        cm.dump(dump_data)
        return []

    city_list = []
    for item in pq(html)('#cities option[value!="0"]'):
        d = data.copy()
        city_e = cm.html2plain(item.text).strip().upper()
        ret = gs.look_up(city_e, 3)
        if ret is not None:
            city_e = ret['name_e']
        d['city_e'] = city_e
        city_list.append(d)

    return city_list


def fetch_stores(data):
    url = data['post_shops']
    param = {'city': data['city_e'], 'paulandjoe_women': 0, 'paulandjoe_man': 0,
             'paulandjoe_sister': 0, 'paulandjoe_little': 0, 'paulandjoe_beauty': 0}
    try:
        html = cm.post_data(url, param)
    except Exception:
        print 'Error occured: %s' % url
        dump_data = {'level': 0, 'time': cm.format_time(), 'data': {'url': url}, 'brand_id': data['brand_id']}
        cm.dump(dump_data)
        return []

    store_list = []
    try:
        for store in (pq(tmp) for tmp in pq(html)('ul')):
            try:
                entry = cm.init_store_entry(data['brand_id'], data['brandname_e'], data['brandname_c'])
                entry[cm.name_e] = cm.html2plain(store('li.first')[0].text).strip()
                entry[cm.country_e] = data[cm.country_e]
                entry[cm.city_e] = data[cm.city_e]

                addr_list = []
                for term in (cm.reformat_addr(unicode(pq(tmp))) for tmp in store('li[class!="first"]')):
                    if term != '':
                        addr_list.append(term)
                tel = cm.extract_tel(addr_list[-1])
                if tel != '':
                    entry[cm.tel] = tel
                    del addr_list[-1]
                entry[cm.addr_e] = ', '.join(addr_list)

                gs.field_sense(entry)
                ret = gs.addr_sense(entry[cm.addr_e])
                if ret[0] is not None and entry[cm.country_e] == '':
                    entry[cm.country_e] = ret[0]
                if ret[1] is not None and entry[cm.province_e] == '':
                    entry[cm.province_e] = ret[1]
                if ret[2] is not None and entry[cm.city_e] == '':
                    entry[cm.city_e] = ret[2]
                gs.field_sense(entry)
                print '(%s/%d) Found store: %s, %s (%s, %s)' % (data['brandname_e'], data['brand_id'],
                                                                entry[cm.name_e], entry[cm.addr_e], entry[cm.country_e],
                                                                entry[cm.continent_e])
                store_list.append(entry)
                db.insert_record(entry, 'stores')
            except (IndexError, TypeError) as e:
                cm.dump(u'Error in parsing %s, %s' % (url, param), log_name)
                print traceback.format_exc()
                continue
    except Exception, e:
        print traceback.format_exc()

    return store_list


def fetch(level=1, data=None, user='root', passwd=''):
    def func(data, level):
        """
        :param data:
        :param level: 0：国家；1：城市；2：商店列表
        """
        if level == 0:
            # 国家列表
            return [{'func': lambda data: func(data, 1), 'data': c} for c in fetch_countries(data)]
        elif level == 1:
            # 城市信息
            return [{'func': lambda data: func(data, 2), 'data': s} for s in fetch_cities(data)]
        elif level == 2:
            # 商店信息
            store_list = fetch_stores(data)
            return [{'func': None, 'data': s} for s in store_list]
        else:
            return []

    # Walk from the root node, where level == 1.
    if data is None:
        data = {'post_shops': 'http://www.paulandjoe.com/storelocator/index/shops/',
                'post_city': 'http://www.paulandjoe.com/storelocator/index/city/',
                'home_url': 'http://www.paulandjoe.com/storelocator/',
                'brand_id': 10297, 'brandname_e': u'Paul & Joe', 'brandname_c': u''}

    global db
    db = cm.StoresDb()
    db.connect_db(user=user, passwd=passwd)
    db.execute(u'DELETE FROM %s WHERE brand_id=%d' % ('stores', data['brand_id']))

    results = cm.walk_tree({'func': lambda data: func(data, 0), 'data': data})
    db.disconnect_db()

    return results


