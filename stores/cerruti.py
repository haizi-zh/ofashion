# coding=utf-8
import json
import string
import re
from stores import geosense as gs

__author__ = 'Zephyre'

db = None
url = 'http://cerruti.com/en/stores/ajax'
brand_id = 10071
brandname_e = u'Cerruti 1881'
brandname_c = u'切瑞蒂'


def get_countries(data):
    """
    返回国家列表
    :rtype : [{'country_id':**, 'country':**}, ...]
    :param data:
    :return:
    """
    url = data['url']
    try:
        html = cm.post_data(url, {'country': -1, 'city': -1, 'recordit': -1})
    except Exception:
        print 'Error occured: %s' % url
        dump_data = {'level': 1, 'time': cm.format_time(), 'data': {'url': url}, 'brand_id': brand_id}
        cm.dump(dump_data)
        return []

    ret = []
    for m in re.findall(ur'<li>\s*?<a href=.+?country-(\d+).+?">(.+?)<\\/a><\\/li>', html, re.S):
        country_id = string.atoi(m[0].strip())
        country = m[1].replace(r'\r', '').replace(r'\n', '').strip().upper()
        ret.append({'country_id': country_id, 'country': country, 'url': url})
    return ret


def get_cities(data):
    """
    获得城市列表
    :param data:
    """
    url = data['url']
    try:
        html = cm.post_data(url, {'country': data['country_id'], 'city': -1, 'recordid': -1})
    except Exception:
        print 'Error occured: %s' % url
        dump_data = {'level': 1, 'time': cm.format_time(), 'data': {'url': url}, 'brand_id': brand_id}
        cm.dump(dump_data)
        return []
    pass

    ret = []
    for m in re.findall(ur'<li>\s*?<a href=.+?city-(\d+).+?">(.+?)<\\/a><\\/li>', html, re.S):
        city_id = string.atoi(m[0].strip())
        city = m[1].replace(r'\r', '').replace(r'\n', '').strip().upper()
        entry = dict(data)
        entry['city_id'] = city_id
        entry['city'] = city
        ret.append(entry)
    return ret


def get_store_list(data):
    """
    获得城市中的商店列表
    :param data:
    :return:
    """
    url = data['url']
    try:
        html = cm.post_data(url, {'country': data['country_id'], 'city': data['city_id'], 'recordid': -1})
    except Exception:
        print 'Error occured: %s' % url
        dump_data = {'level': 1, 'time': cm.format_time(), 'data': {'url': url}, 'brand_id': brand_id}
        cm.dump(dump_data)
        return []
    pass

    ret = []
    for m in re.findall(ur'<a href=.+?store-(\d+).+?">', html, re.S):
        store_id = string.atoi(m.strip())
        entry = dict(data)
        entry['store_id'] = store_id
        ret.append(entry)
    return ret


def get_store_details(data):
    url = data['url']
    try:
        html = cm.post_data(url, {'country': data['country_id'], 'city': data['city_id'], 'recordid': data['store_id']})
    except Exception:
        print 'Error occured: %s' % url
        dump_data = {'level': 1, 'time': cm.format_time(), 'data': {'url': url}, 'brand_id': brand_id}
        cm.dump(dump_data)
        return []

    entry = cm.init_store_entry(brand_id, brandname_e, brandname_c)
    info = json.loads(html)['elements']
    addr = cm.reformat_addr(info['address'].replace('\\', '').replace('<p>', ',').replace('</p>', ','))
    # 第一行为商店名称
    terms = addr.split(',')
    if len(terms) > 0:
        entry[cm.name_e] = cm.reformat_addr(terms[0])
    entry[cm.addr_e] = addr

    gmap_url = info['gmap']
    m = re.findall(ur'(-?\d+\.\d+),(-?\d+\.\d+)', gmap_url)
    if len(m) > 0:
        cm.update_entry(entry, {cm.lat: string.atof(m[0][0]), cm.lng: string.atof(m[0][1])})

    entry[cm.url] = info['shareurl'].replace('\\', '')
    entry[cm.hours] = info['openingtimes']
    entry[cm.comments] = info['other']

    # Geo
    country = data['country']
    city = data['city']
    cm.update_entry(entry, {cm.country_e: country, cm.city_e: city})
    entry[cm.city_e] = cm.extract_city(entry[cm.city_e])[0]

    gs.field_sense(entry)
    ret = gs.addr_sense(entry[cm.addr_e], entry[cm.country_e])
    if ret[1] is not None and entry[cm.province_e] == '':
        entry[cm.province_e] = ret[1]
    if ret[2] is not None and entry[cm.city_e] == '':
        entry[cm.city_e] = ret[2]
    gs.field_sense(entry)

    print '(%s / %d) Found store: %s, %s (%s, %s)' % (
        brandname_e, brand_id, entry[cm.name_e], entry[cm.addr_e], entry[cm.country_e],
        entry[cm.continent_e])

    db.insert_record(entry, 'stores')
    return entry


def fetch(level=1, data=None, user='root', passwd=''):
    def func(data, level):
        """
        :param data:
        :param level: 0：国家；1：城市；2：商店列表
        """
        if level == 0:
            # 国家
            return [{'func': lambda data: func(data, 1), 'data': c} for c in get_countries(data)]
        elif level == 1:
            # 城市列表
            return [{'func': lambda data: func(data, 2), 'data': s} for s in get_cities(data)]
        elif level == 2:
            # 商店列表
            return [{'func': lambda data: func(data, 3), 'data': s} for s in get_store_list(data)]
        elif level == 3:
            # 商店的具体信息
            store = get_store_details(data)
            return [{'func': None, 'data': store}]
        else:
            return []

    global db
    db = cm.StoresDb()
    db.connect_db(user=user, passwd=passwd)
    db.execute(u'DELETE FROM %s WHERE brand_id=%d' % ('stores', brand_id))


    # Walk from the root node, where level == 1.
    if data is None:
        data = {'url': url}
    results = cm.walk_tree({'func': lambda data: func(data, 0), 'data': data})
    db.disconnect_db()

    return results