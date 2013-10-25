# coding=utf-8
import string
import re
from stores import geosense as gs

__author__ = 'Zephyre'

db = None


def fetch_continents(data):
    url = data['url']
    try:
        html = cm.get_data(url)
    except Exception:
        print 'Error occured: %s' % url
        dump_data = {'level': 0, 'time': cm.format_time(), 'data': {'url': url}, 'brand_id': data['brand_id']}
        cm.dump(dump_data)
        return []

    start = html.find('<select class="select_continente">')
    if start == -1:
        return []
    sub, start, end = cm.extract_closure(html[start:], ur'<select\b', ur'</select>')
    if end == 0:
        return []

    continent_list = []
    for m in re.findall(ur'<option value="(\d+)"\s*>(.+?)</option>', sub):
        d = data.copy()
        d[cm.continent_e] = m[1].strip().upper()
        d['continent_id'] = string.atoi(m[0])
        continent_list.append(d)

    return continent_list


def fetch_countries(data):
    url = '%s%d/' % (data['country_url'], data['continent_id'])
    try:
        html = cm.get_data(url)
    except Exception:
        print 'Error occured: %s' % url
        dump_data = {'level': 0, 'time': cm.format_time(), 'data': {'url': url}, 'brand_id': data['brand_id']}
        cm.dump(dump_data)
        return []

    country_list = []
    for m in re.findall(ur'<option value="(\d+)"\s*>(.+?)</option>', html):
        d = data.copy()
        d[cm.country_e] = m[1].strip().upper()
        d['country_id'] = string.atoi(m[0])
        country_list.append(d)

    return country_list


def fetch_cities(data):
    url = '%s%d/' % (data['city_url'], data['country_id'])
    try:
        html = cm.get_data(url)
    except Exception:
        print 'Error occured: %s' % url
        dump_data = {'level': 0, 'time': cm.format_time(), 'data': {'url': url}, 'brand_id': data['brand_id']}
        cm.dump(dump_data)
        return []

    city_list = []
    for m in re.findall(ur'<option value="(\d+)"\s*>(.+?)</option>', html):
        d = data.copy()
        d[cm.city_e] = m[1].strip().upper()
        d['city_id'] = string.atoi(m[0])
        city_list.append(d)

    return city_list


def fetch_stores(data):
    url = '%s%d/' % (data['store_url'], data['city_id'])
    try:
        html = cm.get_data(url)
    except Exception:
        print 'Error occured: %s' % url
        dump_data = {'level': 0, 'time': cm.format_time(), 'data': {'url': url}, 'brand_id': data['brand_id']}
        cm.dump(dump_data)
        return []

    store_list = []
    for m in re.finditer(ur'<div class="store">', html):
        store_sub, ss, se = cm.extract_closure(html[m.start():], ur'<div\b', ur'</div')
        if set == 0:
            continue

        entry = cm.init_store_entry(data['brand_id'], data['brandname_e'], data['brandname_c'])

        start = store_sub.find('<div class="store_name">')
        if start == -1:
            continue
        sub, start, end = cm.extract_closure(store_sub[start:], ur'<div\b', ur'</div>')
        if end == 0:
            continue
        m1 = re.search(ur'<p>(.+?)</p>', sub)
        if m1 is not None:
            entry[cm.name_e] = m1.group(1).strip()

        start = store_sub.find('<div class="store_address">')
        if start == -1:
            continue
        sub, start, end = cm.extract_closure(store_sub[start:], ur'<div\b', ur'</div>')
        if end == 0:
            continue
        m1 = re.search(ur'<p>(.+?)</p>', sub, re.S)
        if m1 is not None:
            addr_list = cm.reformat_addr(m1.group(1)).split(',')
            tmp = []
            tel_pat = re.compile(ur'^tel[\.: ]+', re.I)
            for term in addr_list:
                if re.search(tel_pat, term.strip()) is not None:
                    term = re.sub(tel_pat, '', term.strip())
                    entry[cm.tel] = cm.extract_tel(term)
                else:
                    tmp.append(term.strip())
            entry[cm.addr_e] = ', '.join(tmp)

        m1 = re.search(ur'<input\s.+?name="latitude"\s+value="(.+?)"\s*/>', store_sub)
        if m1 is not None:
            entry[cm.lat] = string.atof(m1.group(1))
        m1 = re.search(ur'<input\s.+?name="longitude"\s+value="(.+?)"\s*/>', store_sub)
        if m1 is not None:
            entry[cm.lng] = string.atof(m1.group(1))

        entry[cm.country_e] = data[cm.country_e]
        entry[cm.continent_e] = data[cm.continent_e]
        entry[cm.city_e] = data[cm.city_e]
        gs.field_sense(entry)
        print '(%s / %d) Found store: %s, %s (%s, %s)' % (data['brandname_e'], data['brand_id'],
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
            # 洲列表
            return [{'func': lambda data: func(data, 1), 'data': s} for s in fetch_continents(data)]
        if level == 1:
            # 国家列表
            return [{'func': lambda data: func(data, 2), 'data': s} for s in fetch_countries(data)]
        if level == 2:
            # 城市列表
            return [{'func': lambda data: func(data, 3), 'data': s} for s in fetch_cities(data)]
        if level == 3:
            # 洲列表
            return [{'func': None, 'data': s} for s in fetch_stores(data)]
        else:
            return []

    # Walk from the root node, where level == 1.
    if data is None:
        data = {'url': 'http://www.furla.com/us/store-locator/',
                'country_url': 'http://www.furla.com/ajax/get_nazioni_store/',
                'city_url': 'http://www.furla.com/ajax/get_citta_store/',
                'store_url': 'http://www.furla.com/ajax/get_stores/',
                'brand_id': 10142, 'brandname_e': u'Furla', 'brandname_c': u'芙拉'}

    global db
    db = cm.StoresDb()
    db.connect_db(user=user, passwd=passwd)
    db.execute(u'DELETE FROM %s WHERE brand_id=%d' % ('stores', data['brand_id']))

    results = cm.walk_tree({'func': lambda data: func(data, 0), 'data': data})
    db.disconnect_db()

    return results