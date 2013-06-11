# coding=utf-8
import json
import string
import re
import common as cm
import geosense as gs

__author__ = 'Zephyre'

db = None


def fetch_countries(data):
    url = data['home_url']
    try:
        html = cm.get_data(url)
    except Exception:
        print 'Error occured: %s' % url
        dump_data = {'level': 0, 'time': cm.format_time(), 'data': {'url': url}, 'brand_id': data['brand_id']}
        cm.dump(dump_data)
        return []

    m = re.search('<select name="country" id="country"', html)
    if m is None:
        return []
    sub, start, end = cm.extract_closure(html[m.start():], r'<select\b', r'</select>')
    if end == 0:
        return []

    country_list = []
    for m in re.findall('<option value="(\d+)".*?>(.+?)</option>', sub):
        d = data.copy()
        d['country_id'] = string.atoi(m[0])

        country_e = cm.html2plain(m[1]).strip().upper()
        ret = gs.look_up(country_e, 1)
        if ret is not None:
            country_e = ret['name_e']
        d['country_e'] = country_e
        # if country_e == u'SUISSE':
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
    for m in re.findall('<option value="([^\d]+)">.+?</option>', html):
        d = data.copy()

        city_e = cm.html2plain(m).strip().upper()
        ret = gs.look_up(city_e, 3)
        if ret is not None:
            city_e = ret['name_e']
        d['city_e'] = city_e
        city_list.append(d)

    return city_list


def fetch_stores(data):
    url = data['post_shops']
    try:
        html = cm.post_data(url, {'city': data['city_e'], 'paulandjoe_women': 0, 'paulandjoe_man': 0,
                                  'paulandjoe_sister': 0, 'paulandjoe_little': 0, 'paulandjoe_beauty': 0})
    except Exception:
        print 'Error occured: %s' % url
        dump_data = {'level': 0, 'time': cm.format_time(), 'data': {'url': url}, 'brand_id': data['brand_id']}
        cm.dump(dump_data)
        return []

    store_list = []
    # for m in re.findall('<option value="([^\d]+)">.+?</option>', html):
    for m in re.findall(ur'<li class="first">(.+?)</li>', html):
        entry = cm.init_store_entry(data['brand_id'], data['brandname_e'], data['brandname_c'])
        entry[cm.name_e] = cm.html2plain(m.strip())

        addr_terms = []
        for m1 in re.findall(ur'<li>(.+?)</li>', html):
            tmp = cm.html2plain(m1).strip()
            tel = cm.extract_tel(tmp)
            if tel != '':
                entry[cm.tel] = tmp
            else:
                tmp = re.sub(ur'<[^<>]+?>', '', tmp).strip()
                if tmp != '':
                    addr_terms.append(tmp)
        entry[cm.addr_e] = ', '.join(addr_terms)
        entry[cm.country_e] = data['country_e']
        entry[cm.city_e] = data['city_e']
        gs.field_sense(entry)
        print '(%s/%d) Found store: %s, %s (%s, %s)' % (data['brandname_e'], data['brand_id'],
                                                        entry[cm.name_e], entry[cm.addr_e], entry[cm.country_e],
                                                        entry[cm.continent_e])
        store_list.append(entry)
        db.insert_record(entry, 'stores')

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


