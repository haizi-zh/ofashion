# coding=utf-8
import json
import string
import re
import common as cm
import geosense as gs

__author__ = 'Zephyre'

db = None
log_name = 'movado_log.txt'


def fetch_provinces(data):
    url = data['url']
    param = {'br': '_1', 'ca': '_R', 'wr': 'HC', 'cn': u'中国'}
    try:
        body = cm.get_data(url, param)
    except Exception, e:
        cm.dump('Error in fetching provinces: %s, %s' % (url, param), log_name)
        return []

    data['country'] = 'CN'
    results = []
    for m in re.findall(ur'<a class="mgeo" href="([^"]+)"[^<>]*>([^<>]+)</a>', body):
        d = data.copy()
        # d['url'] = m[0]
        d['province'] = m[1].strip()
        results.append(d)
    return results


def fetch_cities(data):
    url = data['url']
    param = {'br': '_1', 'ca': '_R', 'wr': 'HC', 'cn': u'中国', 'cr': data['province']}
    try:
        body = cm.get_data(url, param)
    except Exception, e:
        cm.dump('Error in fetching cities: %s, %s' % (url, param), log_name)
        return []

    results = []
    for m in re.findall(ur'<a class="mgeo" href="([^"]+)"[^<>]*>([^<>]+)</a>', body):
        d = data.copy()
        # d['url'] = m[0]
        d['city'] = m[1].strip()
        results.append(d)
    if len(results) > 0:
        return results
    else:
        d = data.copy()
        d['city'] = ''
        d['body'] = body
        return [d]


def fetch_stores(data):
    if 'body' not in data:
        url = data['url']
        param = {'br': '_1', 'ca': '_R', 'wr': 'HC', 'cn': u'中国', 'cr': data['province'], 'cy': data['city']}
        try:
            body = cm.get_data(url, param)
        except Exception, e:
            cm.dump('Error in fetching cities: %s, %s' % (url, param), log_name)
            return []
    else:
        body = data['body']

    store_list = []
    city = data['city']
    if city == '':
        m = re.search(ur'<span id="m_sthead"\s*>(.+?)</span>', body)
        if m is not None:
            city = cm.reformat_addr(m.group(1))
    city = city.replace(u'市', u'').strip()
    for m in re.finditer(ur'<span id="m_stname"[^<>]*>(.+?)</span>', body):
        entry = cm.init_store_entry(data['brand_id'], data['brandname_e'], data['brandname_c'])
        entry[cm.country_e] = data['country']
        entry[cm.province_c] = data['province']
        ret = gs.look_up(data['province'], 2)
        if ret is not None:
            entry[cm.province_e] = ret['name_e']
        entry[cm.city_c] = city
        ret = gs.look_up(city, 3)
        if ret is not None:
            entry[cm.city_e] = ret['name_e']

        entry[cm.name_e] = cm.reformat_addr(m.group(1))

        m1 = re.search(ur'<span id="m_stlist"[^<>]*>(.+?)</span>', body[m.end():])
        if m1 is not None:
            addr_list = cm.reformat_addr(m1.group(1)).split(',')
            tel = cm.extract_tel(addr_list[-1]).strip()
            if tel != '':
                del addr_list[-1]
                entry[cm.tel] = tel
            entry[cm.addr_e] = ', '.join([tmp.strip() for tmp in addr_list])

        gs.field_sense(entry)
        ret = gs.addr_sense(entry[cm.addr_e], entry[cm.country_e])
        if ret[1] is not None and entry[cm.province_e] == '':
            entry[cm.province_e] = ret[1]
        if ret[2] is not None and entry[cm.city_e] == '':
            entry[cm.city_e] = ret[2]
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
            # 省列表
            return [{'func': lambda data: func(data, level + 1), 'data': s} for s in fetch_provinces(data)]
        if level == 1:
            # 城市列表
            return [{'func': lambda data: func(data, level + 1), 'data': s} for s in fetch_cities(data)]
        if level == 2:
            # 商店
            return [{'func': None, 'data': s} for s in fetch_stores(data)]
        else:
            return []

    # Walk from the root node, where level == 1.
    if data is None:
        data = {'url': 'http://int.geoserve.com/movado10cn/php/international.php',
                'brand_id': 10269, 'brandname_e': u'Movado', 'brandname_c': u'摩凡陀'}

    global db
    db = cm.StoresDb()
    db.connect_db(user=user, passwd=passwd)
    db.execute(u'DELETE FROM %s WHERE brand_id=%d' % ('stores', data['brand_id']))

    results = cm.walk_tree({'func': lambda data: func(data, 0), 'data': data})
    db.disconnect_db()

    return results
