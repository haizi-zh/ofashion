# coding=utf-8
import re
from stores import geosense as gs

__author__ = 'Zephyre'

db = None
log_name = 'tsl_log.txt'


def fetch_countries(data):
    vals = {'MO': 'macaoDiv', 'CN': 'chinaDiv', 'HK': 'hksarDiv', 'MY': 'malaysiaDiv'}
    results = []
    for item in vals.items():
        d = data.copy()
        d['country_code'] = item[0]
        d['country_id'] = item[1]
        results.append(d)
    return tuple(results)


def fetch_stores(data):
    url = data['url']
    try:
        body = cm.get_data(url)
    except Exception, e:
        cm.dump('Error in fetching stores: %s' % url, log_name)
        return ()

    m = re.search(ur'<div id="%s" class="tabDiv invisible">' % data['country_id'], body)
    if not m:
        cm.dump('Error in fetching stores: %s' % url, log_name)
        return ()
    sub = cm.extract_closure(body[m.start():], ur'<div\b', ur'</div>')[0]

    store_list = []
    for city_sub in re.findall(ur'<tr>(.+?)</tr>', sub, re.S):
        m = re.search(ur"<td[^<>]+class='shopLocation'\s*>([^<>]+)</td>", city_sub)
        city_c = m.group(1).strip()
        city_e = ''
        if city_c == u'吉隆坡':
            city_e = 'KUALA LUMPUR'
        elif city_c == u'槟城':
            city_e = 'PENANG'
        else:
            ret = gs.look_up(city_c, 3)
            if ret:
                city_e = ret['name_e']
                city_c = ret['name_c']

        m = re.search(ur"<td class='storeName'>(.+?)</td>", city_sub, re.S)
        if not m:
            continue

        for name in (tmp.strip() for tmp in cm.reformat_addr(m.group(1)).split(',')):
            entry = cm.init_store_entry(data['brand_id'], data['brandname_e'], data['brandname_c'])
            entry[cm.country_e] = data['country_code']
            entry[cm.city_e], entry[cm.city_c] = city_e, city_c
            entry[cm.name_e] = name
            entry[cm.addr_e] = name

            gs.field_sense(entry)
            ret = gs.addr_sense(entry[cm.addr_e])
            if ret[0] is not None and entry[cm.country_e] == '':
                entry[cm.country_e] = ret[0]
            if ret[1] is not None and entry[cm.province_e] == '':
                entry[cm.province_e] = ret[1]
            if ret[2] is not None and entry[cm.city_e] == '':
                entry[cm.city_e] = ret[2]
            gs.field_sense(entry)

            if entry[cm.country_e] == '' or entry[cm.city_e] == '':
                ret = gs.geocode(', '.join((entry[cm.name_e], entry[cm.city_c], entry[cm.country_c])))
                if not ret:
                    ret = gs.geocode(', '.join((entry[cm.city_c], entry[cm.country_c])))
                if ret:
                    city = ''
                    province = ''
                    country = ''
                    zip_code = ''
                    tmp = ret[0]['address_components']
                    for v in tmp:
                        if 'locality' in v['types']:
                            city = v['long_name'].strip().upper()
                        elif 'administrative_area_level_1' in v['types']:
                            province = v['long_name'].strip().upper()
                        elif 'country' in v['types']:
                            country = v['long_name'].strip().upper()
                        elif 'postal_code' in v['types']:
                            zip_code = v['long_name'].strip()
                    entry[cm.country_e] = country
                    entry[cm.province_e] = province
                    entry[cm.city_e] = city
                    entry[cm.zip_code] = zip_code

                    gs.field_sense(entry)
                    ret = gs.addr_sense(entry[cm.addr_e])
                    if ret[0] is not None and entry[cm.country_e] == '':
                        entry[cm.country_e] = ret[0]
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

    return tuple(store_list)


def fetch(level=1, data=None, user='root', passwd=''):
    def func(data, level):
        """
        :param data:
        :param level: 0：国家；1：城市；2：商店列表
        """
        if level == 0:
            # 国家列表
            return [{'func': lambda data: func(data, level + 1), 'data': s} for s in fetch_countries(data)]
        if level == 1:
            # 商店
            return [{'func': None, 'data': s} for s in fetch_stores(data)]
        else:
            return ()

    # Walk from the root node, where level == 1.
    if data is None:
        data = {'url': 'http://www.tslj.com/sc/shopfinder/tslStore.html',
                'brand_id': 10361, 'brandname_e': u'TSL', 'brandname_c': u'谢瑞麟'}

    global db
    db = cm.StoresDb()
    db.connect_db(user=user, passwd=passwd)
    db.execute(u'DELETE FROM %s WHERE brand_id=%d' % ('stores', data['brand_id']))

    results = cm.walk_tree({'func': lambda data: func(data, 0), 'data': data})
    db.disconnect_db()
    cm.dump('Done!', log_name)

    return results


