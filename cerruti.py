# coding=utf-8
import json
import string
import re
import common as cm
import geosense as gs

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
    pass

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
    ret = gs.look_up(city, 3)
    if ret is not None:
        if cm.city_e in ret[0]:
            entry[cm.city_e] = ret[0][cm.city_e]
        if cm.city_c in ret[0]:
            entry[cm.city_c] = ret[0][cm.city_c]
        if 'province' in ret[0]:
            ret1 = gs.look_up(ret[0]['province'], 2)
            if ret1 is not None:
                ret1 = ret1[0]
                if cm.province_e in ret1:
                    entry[cm.province_e] = ret1[cm.province_e]
                if cm.province_c in ret1:
                    entry[cm.province_c] = ret1[cm.province_c]
    ret = gs.look_up(country, 1)
    if ret is not None:
        cm.update_entry(entry, {cm.country_e: ret[0][cm.country_e], cm.country_c: ret[0][cm.country_c]})
        ret1 = gs.look_up(ret[0]['continent'], 0)[0]
        cm.update_entry(entry, {cm.continent_e: ret1[cm.continent_e], cm.continent_c: ret1[cm.continent_c]})
    else:
        print 'Error in looking up %s' % country

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
    # Walk from the root node, where level == 1.
    if data is None:
        data = {'url': url}
    results = cm.walk_tree({'func': lambda data: func(data, 0), 'data': data})
    db.disconnect_db()
    return results