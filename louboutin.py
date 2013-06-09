# coding=utf-8
import string
import re
import common as cm
import geosense as gs

__author__ = 'Zephyre'

db = None
url = 'http://us.christianlouboutin.com/ot_cns/storelocator'
brand_id = 10084
brandname_e = u'Christian Louboutin'
brandname_c = u'克里斯提·鲁布托'


def get_continents(data):
    """
    返回洲列表
    :rtype : [{'name':u'欧洲', 'url':'http://....'}, ...]
    :param data:
    :return:
    """
    url = data['url']
    try:
        html = cm.get_data(url)
    except Exception:
        print 'Error occured: %s' % url
        dump_data = {'level': 1, 'time': cm.format_time(), 'data': {'url': url}, 'brand_id': brand_id}
        cm.dump(dump_data)
        return []
    return [{'name': m[1], 'url': m[0]} for m in
            re.findall(ur'<a href="(http://us.christianlouboutin.com/ot_cns/storelocator/\S+)">(.+?)</a>', html)]


def get_store_list(data):
    """
    返回店铺列表，其中店铺包含国家信息。
    :rtype : [{'name':'store name', 'url':'http://...', 'city':'NEW YORK', 'country:':'AUSTRALIA'}, ...]
    :param data:
    """
    url = data['url']
    try:
        html = cm.get_data(url)
    except Exception:
        print 'Error occured: %s' % url
        dump_data = {'level': 1, 'time': cm.format_time(), 'data': {'url': url}, 'brand_id': brand_id}
        cm.dump(dump_data)
        return []

    store_list = []
    for m in re.finditer(ur'<ul>\s+?<h3 class="country-name">(.+?)</h3>', html, re.S):
        sub, start, end = cm.extract_closure(html[m.start():], ur'<ul>', ur'</ul>')
        if end == 0:
            continue
            # 得到不同国家的分割
        splits = [[m1.start(), m1.group(1)] for m1 in re.finditer(ur'<h3 class="country-name">(.+?)</h3>', sub)]
        splits.append([-1, ''])
        for i in xrange(len(splits) - 1):
            # 在同一个国家下寻找
            sub1 = sub[splits[i][0]:splits[i + 1][0]]
            country = splits[i][1].upper()
            for m1 in re.findall(ur'<li>\s*?<a href="(http://us.christianlouboutin.com/ot_cns/storelocator/\S+?)">'
                                 ur'(.+?)</a>,(.+?)</li>', sub1):
                store_list.append({'name': m1[1].strip(), 'url': m1[0], 'city': m1[2].strip().upper(),
                                   'country': country})

    return store_list


def get_store_details(data):
    url = data['url']
    try:
        html = cm.get_data(url)
    except Exception:
        print 'Error occured: %s' % url
        dump_data = {'level': 1, 'time': cm.format_time(), 'data': {'url': url}, 'brand_id': brand_id}
        cm.dump(dump_data)
        return []

    entry = cm.init_store_entry(brand_id, brandname_e, brandname_c)
    entry[cm.name_e] = data['name']
    entry[cm.url] = data['url']
    start = html.find(ur'<div class="storelocator-breadcrumbs">')
    if start == -1:
        return []
    sub, start, end = cm.extract_closure(html[start:], ur'<ul>', ur'</ul>')
    if end == 0:
        return []
        # 最后一个<li>...</li>
    m = re.findall(ur'<li>(.+?)</li>', sub, re.S)
    if len(m) > 0:
        entry[cm.addr_e] = cm.reformat_addr(m[-1])
        # 经纬度
    m = re.findall(ur'position: new google.maps.LatLng\((-?\d+\.\d+).*?(-?\d+\.\d+)\)', html)
    if len(m) > 0:
        cm.update_entry(entry, {cm.lat: string.atof(m[0][0]), cm.lng: string.atof(m[0][1])})

    # Geo
    country = data['country']
    city = data['city']
    cm.update_entry(entry, {cm.country_e: country, cm.city_e: city})
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
        :param level: 0：洲；1：商店列表；2：具体商店
        """
        if level == 0:
            # 洲
            return [{'func': lambda data: func(data, 1), 'data': c} for c in get_continents(data)]
        elif level == 1:
            # 商店列表
            store_list = get_store_list(data)
            return [{'func': lambda data: func(data, 2), 'data': s} for s in store_list]
        elif level == 2:
            # 具体商店信息
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

    for i in xrange(4):
        gs.commit_maps(i)

    return results
