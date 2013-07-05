# coding=utf-8
import re
import common as cm
import geosense as gs

__author__ = 'Zephyre'

brand_id = 10385
brandname_e = 'Y-3'
brandname_c = 'Y-3'
url = 'http://www.y-3.com/store-finder'


def fetch_stores(data):
    """
    获得门店的详细信息
    :rtype : [entries]
    :param data:
    """
    try:
        html = cm.get_data(data['url'])
    except Exception:
        print 'Error occured: %s' % url
        dump_data = {'level': 1, 'time': cm.format_time(), 'data': {'url': url}, 'brand_id': brand_id}
        cm.dump(dump_data)
        return []

    entries = []
    start = html.find(u'<ul class="store-list">')
    if start == -1:
        return entries
    start += len(u'<ul class="store-list">')
    end = html.find(u'</ul>', start)
    html = html[start:end]

    for m1 in re.findall(ur'<li class="(.*?)">(.*?)</li>', html, re.S):
        store = cm.init_store_entry(brand_id, brandname_e, brandname_c)
        store[cm.store_type] = m1[0]
        sub_html = m1[1]
        m2 = re.findall(ur'<h3 class="store-name">(.*?)</h3>', sub_html)
        if len(m2) > 0:
            store[cm.name_e] = cm.reformat_addr(m2[0])
        m2 = re.findall(ur'<p class="store-address">(.*?)</p>', sub_html, re.S)
        if len(m2) > 0:
            store[cm.addr_e] = cm.reformat_addr(m2[0])

        cm.update_entry(store, {cm.continent_e: data[cm.continent_e].strip().upper(),
                                cm.country_e: data[cm.country_e].strip().upper(),
                                cm.city_e: data[cm.city_e].strip().upper()})

        entry = store
        gs.field_sense(entry)
        ret = gs.addr_sense(entry[cm.addr_e])
        if ret[0] is not None and entry[cm.country_e] == '':
            entry[cm.country_e] = ret[0]
        if ret[1] is not None and entry[cm.province_e] == '':
            entry[cm.province_e] = ret[1]
        if ret[2] is not None and entry[cm.city_e] == '':
            entry[cm.city_e] = ret[2]
        gs.field_sense(entry)
        entry[cm.city_e] = cm.extract_city(entry[cm.city_e])[0]

        print '%s: Found store: %s, %s (%s, %s)' % (
            brandname_e, store[cm.name_e], store[cm.addr_e], store[cm.country_e],
            store[cm.continent_e])
        db.insert_record(store, 'stores')
        entries.append(store)

    return entries


def fetch_districts(url, pat):
    """
    获得洲列表
    :rtype : [{'name': 'Asia'; 'url': 'http://www.y-3.com/store-finder/asia'}]
    :param url:
    """
    try:
        html = cm.get_data(url)
    except Exception:
        print 'Error occured: %s' % url
        dump_data = {'level': 1, 'time': cm.format_time(), 'data': {'url': url}, 'brand_id': brand_id}
        cm.dump(dump_data)
        return []

    ret = []
    for m in re.findall(pat, html):
        if m[1].strip() != 'Y-3 Stores':
            ret.append({'name': m[1].strip(), 'url': m[0].strip()})
    return ret


def fetch_continents(data):
    ret = fetch_districts(data['url'],
                          '<li class=".*?"><a href="(http://www.y-3.com/store-finder/.+?/)">(.+?)</a></li>')
    return [{cm.continent_e: m['name'], 'url': m['url']} for m in ret]


def fetch_countries(data):
    pat = '<li.*?><a href="(http://www.y-3.com/store-finder/.+?/.+?/)">(.+?)</a></li>'
    ret = fetch_districts(data['url'], pat)
    return [{cm.continent_e: data[cm.continent_e], cm.country_e: m['name'], 'url': m['url']} for m in ret]


def fetch_cities(data):
    pat = '<li.*?><a href="(http://www.y-3.com/store-finder/.+?/.+?/.+?/)">(.+?)</a></li>'
    ret = fetch_districts(data['url'], pat)
    return [{cm.continent_e: data[cm.continent_e], cm.country_e: data[cm.country_e],
             cm.city_e: m['name'], 'url': m['url']} for m in ret]


func_map = {1: fetch_continents, 2: fetch_countries, 3: fetch_cities}


def fetch(level=1, data=None, user='root', passwd=''):
    def func(data, level):
        """
        :param data:
        :param level: 1: 洲；2：国家；3：城市；4：商店
        """
        if level < 4:
            con_l = func_map[level](data)
            siblings = [{'func': lambda data: func(data, level + 1), 'data': c1} for c1 in con_l]
            return siblings
        else:
            # 获得商店信息
            store_l = fetch_stores(data)
            return [{'func': None, 'data': s} for s in store_l]


    global db
    db = cm.StoresDb()
    db.connect_db(user=user, passwd=passwd)
    db.execute(u'DELETE FROM %s WHERE brand_id=%d' % ('stores', brand_id))

    # Walk from the root node, where level == 1.
    if data is None:
        data = {'url': url}
    results = cm.walk_tree({'func': lambda data: func(data, 1), 'data': data})

    db.disconnect_db()
    return results
