# coding=utf-8
import json
import string
import re
import common as cm
import geosense as gs

__author__ = 'Zephyre'

db = None


def fetch_cities(data):
    # url = data['post_url']
    # try:
    #     action=yoox_storelocator_change_country&country_id=3125&dataType=JSON
    #     js = json.loads(cm.post_data(url, {'action': 'yoox_storelocator_change_country',
    #                                        'country_id': ,
    #                                        'retail_type': data['retail_type']}).decode('unicode_escape'))
    # except Exception:
    #     print 'Error occured in getting country list: %s' % url
    #     dump_data = {'level': 1, 'time': cm.format_time(), 'data': {'url': url}, 'brand_id': data['brand_id']}
    #     cm.dump(dump_data)
    #     return []

    url = data['home_url']
    try:
        html = cm.get_data(url)
    except Exception:
        print 'Error occured: %s' % url
        dump_data = {'level': 0, 'time': cm.format_time(), 'data': {'url': url}, 'brand_id': data['brand_id']}
        cm.dump(dump_data)
        return []

    # 建立国家和城市列表
    country_map = {}
    city_map = {}

    start = html.find('<div id="storelocator-box-select-country"')
    if start == -1:
        return []
    sub, start, end = cm.extract_closure(html[start:], ur'<div\b', ur'</div>')
    for m1 in re.finditer(ur'<a href=".+?" class="depth-1" data-store-id="(\d+)">(.+?)</a>', sub):
        country_id = string.atoi(m1.group(1))
        country_e = m1.group(2).strip().upper()
        country_map[country_id] = country_e

        city_sub, s1, e1 = cm.extract_closure(sub[m1.end():], ur'<ul\b', ur'</ul>')
        for m2 in re.findall(ur'<li class=".+?"><a href=".+?" class="depth-2" data-store-id="(\d+)">(.+?)</a></li>',
                             city_sub):
            city_id = string.atoi(m2[0])
            city_e = m2[1].strip().upper()
            city_map[city_id] = {'city_e': city_e, 'parent': country_id}


    # 分析商店信息
    store_list = []
    for m1 in re.finditer(ur'jQuery\.extend\(markerOpts,', html):
        sub = html[m1.end():]
        sub, start, end = cm.extract_closure(sub, ur'\{', ur'\}')
        s = json.loads(sub)

        entry = cm.init_store_entry(data['brand_id'], data['brandname_e'], data['brandname_c'])
        entry[cm.addr_e] = s['address']
        entry[cm.lat] = string.atof(s['latlong']['lat'])
        entry[cm.lng] = string.atof(s['latlong']['lng'])
        entry[cm.name_e] = s['title']

        city = city_map[s['parent']]
        entry[cm.city_e] = cm.extract_city(city['city_e'])[0]
        entry[cm.country_e] = country_map[city['parent']]

        gs.field_sense(entry)
        ret = gs.addr_sense(entry[cm.addr_e], entry[cm.country_e])
        if ret[1] is not None and entry[cm.province_e] == '':
            entry[cm.province_e] = ret[1]
        if ret[2] is not None and entry[cm.city_e] == '':
            entry[cm.city_e] = ret[2]
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
            # 城市列表
            return [{'func': None, 'data': c} for c in fetch_cities(data)]
        # elif level == 1:
        #     # 商店信息
        #     retails = [{'func': None, 'data': s} for s in fetch_retails(data)]
        #     services = [{'func': None, 'data': s} for s in fetch_service(data)]
        #     retails.extend(services)
        #     return retails
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
        data = {'home_url': 'http://www.missoni.com/experience/us/pages/store-locator/',
                'post_url': 'http://www.missoni.com/experience/us?yoox_storelocator_action=true',
                'brand_id': 10263, 'brandname_e': u'MISSONI', 'brandname_c': u'米索尼'}

    global db
    db = cm.StoresDb()
    db.connect_db(user=user, passwd=passwd)
    db.execute(u'DELETE FROM %s WHERE brand_id=%d' % ('stores', data['brand_id']))

    results = cm.walk_tree({'func': lambda data: func(data, 0), 'data': data})
    db.disconnect_db()

    return results

