# coding=utf-8
import json
import string
import re
import common as cm
import geosense as gs

__author__ = 'Zephyre'

db = None


def fetch_depth1(data):
    c_list = [{'name': 'UK', 'url': 'http://www.jimmychoo.com/find-a-boutique/uk/scat/storeuk'},
              {'name': 'US', 'url': 'http://www.jimmychoo.com/find-a-boutique/usa/scat/storeus'},
              {'name': 'EUROPE', 'url': 'http://www.jimmychoo.com/find-a-boutique/europe/scat/storeeu'},
              {'name': None, 'url': 'http://www.jimmychoo.com/find-a-boutique/middle-east/scat/middleeast'},
              {'name': 'ASIA', 'url': 'http://www.jimmychoo.com/find-a-boutique/asia-pacific/scat/asia'},
              {'name': 'JAPAN', 'url': 'http://www.jimmychoo.com/find-a-boutique/japan/scat/japan'},
              {'name': 'SOUTH AMERICA',
               'url': 'http://www.jimmychoo.com/find-a-boutique/south-america/scat/southamerica'},
              {'name': 'AFRICA', 'url': 'http://www.jimmychoo.com/find-a-boutique/africa/scat/africa'},
              {'name': 'AUSTRALIA', 'url': 'http://www.jimmychoo.com/find-a-boutique/australia/scat/australia'}]

    results = []
    for value in c_list:
        d = data.copy()
        d['url'] = value['url']
        d['name'] = value['name']
        results.append(d)
    return results


def fetch_uk(body, data):
    start = body.find(u'<div class="fableft">')
    if start == -1:
        print 'Error in finding %s stores' % data['name']
        return []
    body, start, end = cm.extract_closure(body[start:], ur'<div\b', ur'</div>')
    if end == 0:
        print 'Error in finding %s stores' % data['name']
        return []

    store_list = []
    for m in re.findall(ur'<div>\s*(.+?)\s*</div>', body, re.S):
        entry = cm.init_store_entry(data['brand_id'], data['brandname_e'], data['brandname_c'])
        entry[cm.country_e] = data['name']

        addr_list = re.findall(ur'<p>\s*(.+?)\s*</p>', m)
        tel = cm.extract_tel(addr_list[-1])
        if tel != '':
            entry[cm.tel] = tel
            del addr_list[-1]

        if data['name'] == 'AUSTRALIA':
            country, province, city = gs.addr_sense(', '.join(addr_list), data['name'])
            if city is not None:
                entry[cm.city_e] = city
            if province is not None:
                entry[cm.province_e] = province
        else:
            city = addr_list[-2].strip().upper()
            entry[cm.city_e] = city
            ret = gs.look_up(city, 3)
            if ret is not None and ret['country']['name_e'] == gs.look_up('UK', 1)['name_e']:
                entry[cm.city_e] = ret['name_e']
            entry[cm.zip_code] = addr_list[-1].strip().upper()
        entry[cm.addr_e] = ', '.join(addr_list)
        entry[cm.city_e] = cm.extract_city(entry[cm.city_e])[0]

        gs.field_sense(entry)
        ret = gs.addr_sense(entry[cm.addr_e], entry[cm.country_e])
        if ret[1] is not None and entry[cm.province_e] == '':
            entry[cm.province_e] = ret[1]
        if ret[2] is not None and entry[cm.city_e] == '':
            entry[cm.city_e] = ret[2]
        gs.field_sense(entry)

        print '(%s / %d) Found store: %s, %s (%s, %s, %s)' % (data['brandname_e'], data['brand_id'],
                                                              entry[cm.name_e], entry[cm.addr_e], entry[cm.city_e],
                                                              entry[cm.country_e], entry[cm.continent_e])

        db.insert_record(entry, 'stores')
        store_list.append(entry)

    return store_list


def fetch_world(body, data):
    start = body.find(u'<div class="fableft">')
    if start == -1:
        print 'Error in finding %s stores' % data['name']
        return []
    body, start, end = cm.extract_closure(body[start:], ur'<div\b', ur'</div>')
    if end == 0:
        print 'Error in finding %s stores' % data['name']
        return []

    idx_list = []
    for m in re.finditer(ur'<h2>(.+?)</h2>', body):
        idx_list.append({'idx': m.end(), 'name': m.group(1).strip().upper()})
    idx_list.append({'idx': -1})

    country_sub = []
    for i in xrange(len(idx_list) - 1):
        country_sub.append({'name': idx_list[i]['name'],
                            'content': body[idx_list[i]['idx']:idx_list[i + 1]['idx']]})

    store_list = []
    for item in country_sub:
        body = item['content']
        country = item['name']
        for m in re.findall(ur'<div>\s*(.+?)\s*</div>', body, re.S):
            entry = cm.init_store_entry(data['brand_id'], data['brandname_e'], data['brandname_c'])
            entry[cm.country_e] = country

            addr_list = re.findall(ur'<p>\s*(.+?)\s*</p>', m)
            tel = cm.extract_tel(addr_list[-1])
            if tel != '':
                entry[cm.tel] = tel
                del addr_list[-1]

            ret = gs.addr_sense(', '.join(addr_list))
            if ret[2] is not None:
                entry[cm.city_e] = ret[2]
            if ret[1] is not None:
                entry[cm.province_e] = ret[1]
            entry[cm.addr_e] = ', '.join(addr_list)
            entry[cm.city_e] = cm.extract_city(entry[cm.city_e])[0]

            gs.field_sense(entry)
            ret = gs.addr_sense(entry[cm.addr_e], entry[cm.country_e])
            if ret[1] is not None and entry[cm.province_e] == '':
                entry[cm.province_e] = ret[1]
            if ret[2] is not None and entry[cm.city_e] == '':
                entry[cm.city_e] = ret[2]
            gs.field_sense(entry)

            print '(%s / %d) Found store: %s, %s (%s, %s, %s)' % (data['brandname_e'], data['brand_id'],
                                                                  entry[cm.name_e], entry[cm.addr_e], entry[cm.city_e],
                                                                  entry[cm.country_e], entry[cm.continent_e])
            db.insert_record(entry, 'stores')
            store_list.append(entry)

    return store_list


def fetch_stores(data):
    url = data['url']
    try:
        body = cm.get_data(url)
    except Exception:
        print 'Error occured: %s' % url
        dump_data = {'level': 0, 'time': cm.format_time(), 'data': {'url': url}, 'brand_id': data['brand_id']}
        cm.dump(dump_data)
        return []

    if data['name'] == 'UK' or data['name'] == 'US' or data['name'] == 'JAPAN' or data['name'] == 'AUSTRALIA':
        return fetch_uk(body, data)
    else:
        return fetch_world(body, data)


def fetch(level=1, data=None, user='root', passwd=''):
    def func(data, level):
        """
        :param data:
        :param level: 0：国家；1：城市；2：商店列表
        """
        if level == 0:
            # 洲列表
            return [{'func': lambda data: func(data, 1), 'data': s} for s in fetch_depth1(data)]
        if level == 1:
            # 国家列表
            return [{'func': None, 'data': s} for s in fetch_stores(data)]
        else:
            return []

    # Walk from the root node, where level == 1.
    if data is None:
        data = {'brand_id': 10184, 'brandname_e': u'Jimmy Choo', 'brandname_c': u'周仰杰'}

    global db
    db = cm.StoresDb()
    db.connect_db(user=user, passwd=passwd)
    db.execute(u'DELETE FROM %s WHERE brand_id=%d' % ('stores', data['brand_id']))

    results = cm.walk_tree({'func': lambda data: func(data, 0), 'data': data})
    db.disconnect_db()

    return results