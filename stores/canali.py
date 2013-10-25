# coding=utf-8
import string
import re
from stores import geosense as gs

__author__ = 'Zephyre'

db = None


def fetch_cities(data):
    url = data['home_url']
    try:
        body = cm.get_data(url)
    except Exception:
        cm.dump('Error in fetching cities: %s' % url, 'canali_log.txt')
        dump_data = {'level': 0, 'time': cm.format_time(), 'data': {'url': url}, 'brand_id': data['brand_id']}
        cm.dump(dump_data)
        return []

    start = body.find(u'<nav class="countrySelector">')
    if start == -1:
        cm.dump('Error occured in fetching country list: %s' % url, 'canali_log.txt')
    body = cm.extract_closure(body[start:], ur'<nav\b', ur'</nav>')[0]

    results = []
    for m in re.finditer(ur'<li><a href=".+?">(.+?)</a>', body):
        country = m.group(1).strip().upper()
        sub = cm.extract_closure(body[m.end():], ur'<ul\b', ur'</ul>')[0]
        for m1 in re.findall(ur'<li><a class=".+?" href="(.+?)">(.+?)</a></li>', sub):
            d = data.copy()
            d['country'] = country
            d['url'] = data['host'] + m1[0]
            d['city'] = m1[1].strip().upper()
            results.append(d)
    return results


def fetch_stores(data):
    url = data['url']
    try:
        body = cm.get_data(url)
    except Exception:
        cm.dump('Error occured in fetching stores: %s' % url, 'canali_log.txt')
        dump_data = {'level': 0, 'time': cm.format_time(), 'data': {'url': url}, 'brand_id': data['brand_id']}
        cm.dump(dump_data)
        return []

    store_list = []
    for m in re.finditer(ur'<div class="storeInfo">', body):
        sub = cm.extract_closure(body[m.start():], ur'<div\b', ur'</div>')[0]
        m1 = re.search(ur'<span itemprop="streetAddress">(.+?)</span>', sub)
        if m1 is None:
            cm.dump('Error: failed to find the address: %s' % url, 'canali_log.txt')
            continue
        entry = cm.init_store_entry(data['brand_id'], data['brandname_e'], data['brandname_c'])
        entry[cm.addr_e] = cm.reformat_addr(m1.group(1))
        entry[cm.country_e] = data['country']
        entry[cm.city_e] = data['city']
        ret = gs.addr_sense(entry[cm.addr_e], entry[cm.country_e])
        if ret[1] is not None:
            entry[cm.province_e] = ret[1]

        m1 = re.search(ur'<span itemprop="telephone">(.+?)</span>', sub)
        if m1 is not None:
            entry[cm.tel] = m1.group(1).strip()

        m1 = re.search(ur'data-latitude="(.+?)"', sub)
        if m1 is not None:
            entry[cm.lat] = string.atof(m1.group(1))
        m1 = re.search(ur'data-longitude="(.+?)"', sub)
        if m1 is not None:
            entry[cm.lng] = string.atof(m1.group(1))

        gs.field_sense(entry)
        cm.dump('(%s / %d) Found store: %s, %s (%s, %s)' % (data['brandname_e'], data['brand_id'],
                                                            entry[cm.name_e], entry[cm.addr_e], entry[cm.country_e],
                                                            entry[cm.continent_e]), 'canali_log.txt')
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
            # 城市列表
            return [{'func': lambda data: func(data, 1), 'data': s} for s in fetch_cities(data)]
        if level == 1:
            # 商店列表
            return [{'func': None, 'data': s} for s in fetch_stores(data)]
        else:
            return []

    # Walk from the root node, where level == 1.
    if data is None:
        data = {'home_url': 'http://www.canali.it/en/stores/',
                'host': 'http://www.canali.it',
                'brand_id': 10062, 'brandname_e': u'Canali', 'brandname_c': u'康纳利'}

    global db
    db = cm.StoresDb()
    db.connect_db(user=user, passwd=passwd)
    db.execute(u'DELETE FROM %s WHERE brand_id=%d' % ('stores', data['brand_id']))

    results = cm.walk_tree({'func': lambda data: func(data, 0), 'data': data})
    db.disconnect_db()

    return results