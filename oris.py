# coding=utf-8
import json
import string
import re
import common as cm
import geosense as gs

__author__ = 'Zephyre'

db = None


def fetch_countries(data):
    url = data['url']
    try:
        html = cm.get_data(url)
    except Exception:
        print 'Error occured: %s' % url
        dump_data = {'level': 0, 'time': cm.format_time(), 'data': {'url': url}, 'brand_id': data['brand_id']}
        cm.dump(dump_data)
        return []

    # 处理重定向
    m = re.search('<h2>Object moved to <a href="(.+?)">', html)
    if m is not None:
        data['url'] = data['host'] + m.group(1)
        return fetch_countries(data)

    m = re.search('<span class="country">Choose a country</span>', html)
    if m is None:
        return []
    sub, start, end = cm.extract_closure(html[m.end():], r'<ul\b', r'</ul>')
    if end == 0:
        return []

    country_list = []
    for m in re.findall('<li><a .*?href="(.+?)">(.+?)</a></li>', sub):
        d = data.copy()
        country_e = cm.html2plain(m[1]).strip().upper()
        ret = gs.look_up(country_e, 1)
        if ret is not None:
            country_e=ret['name_e']
        d['country_e'] = country_e
        d['province_e'] = ''
        d['url'] = data['host'] + m[0]
        country_list.append(d)
    return country_list


def fetch_states(data):
    print '(%s/%d) Found country: %s' % (data['brandname_e'], data['brand_id'], data['country_e'])
    url = data['url']
    try:
        html = cm.get_data(url)
    except Exception:
        print 'Error occured: %s' % url
        dump_data = {'level': 0, 'time': cm.format_time(), 'data': {'url': url}, 'brand_id': data['brand_id']}
        cm.dump(dump_data)
        return []

    # 处理重定向
    m = re.search('<h2>Object moved to <a href="(.+?)">', html)
    if m is not None:
        data['url'] = data['host'] + m.group(1)
        return fetch_countries(data)

    m = re.search('<span class="state">Choose a state/provence</span>', html)
    if m is None:
        return []
    sub, start, end = cm.extract_closure(html[m.end():], r'<ul\b', r'</ul>')
    if end == 0:
        return []

    state_list = []
    for m in re.findall('<li><a .*?href="(.+?)">(.+?)</a></li>', sub):
        province_e = cm.html2plain(m[1]).strip().upper()
        if data['country_e'] == 'CHINA':
            # 去掉省中间的空格
            province_e = province_e.replace(' ', '')
        ret = gs.look_up(province_e, 2)
        if ret is not None:
            province_e=ret['name_e']
        d = data.copy()
        d['province_e'] = province_e
        d['url'] = data['host'] + m[0]
        state_list.append(d)

    return state_list


def fetch_cities(data):
    if data['province_e'] != '':
        print '(%s/%d) Found province: %s' % (data['brandname_e'], data['brand_id'], data['province_e'])

    url = data['url']
    try:
        html = cm.get_data(url)
    except Exception:
        print 'Error occured: %s' % url
        dump_data = {'level': 0, 'time': cm.format_time(), 'data': {'url': url}, 'brand_id': data['brand_id']}
        cm.dump(dump_data)
        return []

    # 处理重定向
    m = re.search('<h2>Object moved to <a href="(.+?)">', html)
    if m is not None:
        data['url'] = data['host'] + m.group(1)
        return fetch_countries(data)

    m = re.search('<span class="city">Choose a city</span>', html)
    if m is None:
        return []
    sub, start, end = cm.extract_closure(html[m.end():], r'<ul\b', r'</ul>')
    if end == 0:
        return []

    city_list = []
    for m in re.findall('<li><a .*?href="(.+?)">(.+?)</a></li>', sub):
        city_e = cm.html2plain(m[1]).strip().upper()
        if data['country_e'] == 'CHINA':
            # 去掉省中间的空格
            city_e = city_e.replace(' ', '')
        ret = gs.look_up(city_e, 3)
        if ret is not None:
            city_e=ret['name_e']

        d = data.copy()
        d['city_e'] = city_e
        d['url'] = data['host'] + m[0]
        city_list.append(d)

    return city_list


def fetch_stores(data):
    print '(%s/%d) Found city: %s' % (data['brandname_e'], data['brand_id'], data['city_e'])
    url = data['url']
    try:
        html = cm.get_data(url)
    except Exception:
        print 'Error occured: %s' % url
        dump_data = {'level': 0, 'time': cm.format_time(), 'data': {'url': url}, 'brand_id': data['brand_id']}
        cm.dump(dump_data)
        return []

    # 处理重定向
    m = re.search('<h2>Object moved to <a href="(.+?)">', html)
    if m is not None:
        data['url'] = data['host'] + m.group(1)
        return fetch_countries(data)

    m = re.search('var\s+data\s*=\s*', html)
    if m is None:
        return []
    sub, start, end = cm.extract_closure(html[m.end():], r'\[', r'\]')
    if end == 0:
        return []

    store_list = []
    for s in json.loads(sub):
        entry = cm.init_store_entry(data['brand_id'], data['brandname_e'], data['brandname_c'])
        name = s['Name']
        if cm.is_chinese(name):
            entry[cm.name_c] = name
        else:
            entry[cm.name_e] = name
        entry[cm.addr_e] = cm.html2plain(s['Street'])
        entry[cm.city_e] = data['city_e']
        entry[cm.country_e] = data['country_e']
        entry[cm.province_e] = data['province_e']
        pat = re.compile(ur'tel[\.: ]*', re.I)
        entry[cm.tel] = re.sub(pat, '', s['Phone']).strip()
        pat = re.compile(ur'fax[\.: ]*', re.I)
        entry[cm.fax] = re.sub(pat, '', s['Fax']).strip()
        entry[cm.email] = s['Email'].strip()
        entry[cm.url] = s['Website'].strip()
        coord = s['LatLng']
        if coord is not None and len(coord) >= 2:
            if coord[0] is not None:
                entry[cm.lat] = string.atof(coord[0])
            if coord[1] is not None:
                entry[cm.lng] = string.atof(coord[1])
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
            # 州/省信息
            state_list = fetch_states(data)
            if len(state_list) == 0:
                # 不含州/省信息
                return [{'func': lambda data: func(data, 2), 'data': s} for s in [data]]
            else:
                return [{'func': lambda data: func(data, 2), 'data': s} for s in state_list]
        elif level == 2:
            # 城市信息
            return [{'func': lambda data: func(data, 3), 'data': s} for s in fetch_cities(data)]
        elif level == 3:
            # 商店信息
            store_list = fetch_stores(data)
            return [{'func': None, 'data': s} for s in store_list]

        # elif level == 2:
        #     # 城市列表
        #     return [{'func': lambda data: func(data, 3), 'data': s} for s in fetch_cities(data)]
        # elif level == 3:
        #     # 商店的具体信息
        #     return [{'func': None, 'data': s} for s in fetch_stores(data)]
        else:
            return []

    # Walk from the root node, where level == 1.
    if data is None:
        data = {'url': 'http://www.oris.ch/en/store-locator/china/state/an-hui',
                'host': 'http://www.oris.ch',
                'brand_id': 10292, 'brandname_e': u'Oris', 'brandname_c': u'豪利时'}

    global db
    db = cm.StoresDb()
    db.connect_db(user=user, passwd=passwd)
    db.execute(u'DELETE FROM %s WHERE brand_id=%d' % ('stores', data['brand_id']))

    results = cm.walk_tree({'func': lambda data: func(data, 0), 'data': data})
    db.disconnect_db()

    return results