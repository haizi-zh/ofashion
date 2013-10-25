# coding=utf-8
import json
import string
import re
from stores import geosense as gs

__author__ = 'Zephyre'

db = None
log_name = 'zara_log.txt'
store_map = {}
city_cnt = 0


def fetch_countries(data):
    url = data['url']
    try:
        body = cm.get_data(url)
    except Exception, e:
        cm.dump('Error in fetching countries: %s' % url, log_name)
        return ()

    m = re.search(ur'catalogId=(\d+)&fts=0&myAcctMain=1&categoryId=(\d+)&langId=(-?\d+)&storeId=(\d+)', body)
    data['catalogId'] = string.atoi(m.group(1))
    data['categoryId'] = string.atoi(m.group(2))
    data['langId'] = string.atoi(m.group(3))
    data['storeId'] = string.atoi(m.group(4))

    sub = body[body.find(ur'<div class="selectChildCont">'):]
    m = re.search(ur'<ul>(.+?)</ul>', sub, re.S)
    if m is None:
        cm.dump('Error in fetching countries: %s' % url, log_name)
        return ()
    sub = m.group(1)
    results = []
    for item in re.findall(ur'<li>.+?name="country".+?value="([A-Z]{2})', sub, re.S):
        d = data.copy()
        d['country_code'] = item
        if item != 'XE':
            results.append(d)
    return tuple(results)


def gen_city_map():
    with open('city_lite.dat', 'r') as f:
        sub = f.readlines()
    return json.loads(sub[0])


def fetch_cities(data):
    ret = gs.look_up(data['country_code'], 1)
    if ret is None:
        return ()

    country = ret['name_e']
    city_map = data['city_map']
    results = []
    if country in city_map:
        for city in city_map[country]:
            d = data.copy()
            d['country'] = country
            d['city'] = city
            d['city_lat'] = city_map[country][city]['lat']
            d['city_lng'] = city_map[country][city]['lng']
            results.append(d)
    return tuple(results)


def fetch_stores(data):
    global city_cnt
    city_cnt += 1
    cm.dump('Processing city #%d: %s, %s' % (city_cnt, data['city'], data['country']))
    url = data['data_url']
    param = {'showOnlyDeliveryShops': False, 'isPopUp': False, 'catalogId': data['catalogId'],
             'categoryId': data['categoryId'], 'langId': data['langId'], 'showSelectButton': False,
             'storeId': data['storeId'], 'latitude': '%f' % data['city_lat'],
             'longitude': '%f' % data['city_lng'], 'country': data['country_code'], 'ajaxCall': True}
    try:
        body = cm.get_data(url, param)
    except Exception, e:
        cm.dump('Error in fetching stores: %s, %s' % (url, param), log_name)
        return ()

    store_list = []
    for s in re.findall(ur'<li[^<>]+id="(liShop_\d+)"[^<>]*>(.+?)</li>', body, re.S):
        store_id = s[0]
        if store_id in store_map:
            continue
        else:
            entry = cm.init_store_entry(data['brand_id'], data['brandname_e'], data['brandname_c'])
            store_map[store_id] = entry

        entry[cm.lat] = data['city_lat']
        entry[cm.lng] = data['city_lng']
        m = re.search(ur'class="lat" value="(-?\d+\.?\d*)"', s[1])
        if m is not None:
            entry[cm.lat] = string.atof(m.group(1))
        m = re.search(ur'class="lng" value="(-?\d+\.?\d*)"', s[1])
        if m is not None:
            entry[cm.lng] = string.atof(m.group(1))

        m = re.search(ur'<a class="headRef"[^<>]*>(.+?)</a>', s[1], re.S)
        if m is not None:
            name_tuple = (tmp.strip() for tmp in cm.reformat_addr(m.group(1)).split(','))
            name_list = []
            for term in name_tuple:
                if term != '':
                    name_list.append(term)
            entry[cm.name_e] = ', '.join(name_list)

        m = re.search(ur'<div class="address">(.+?)</div>', s[1], re.S)
        if m is not None:
            addr_sub = cm.extract_closure(m.group(1), ur'<span\b', ur'</span>')[0]

            pat_title = re.compile(ur'<span class="titChain">([^<>]+)</span>')
            m = re.search(pat_title, addr_sub)
            if m:
                entry[cm.store_class] = cm.html2plain(m.group(1)).strip()
                addr_sub = re.sub(pat_title, '', addr_sub)

            pat_tel = re.compile(ur'^\s*tel\s*[\.:]', re.I)
            addr_list = []
            for term in (cm.reformat_addr(tmp) for tmp in re.split(ur'<\s*br\s*/\s*>', addr_sub)):
                if term == '':
                    continue
                elif re.search(pat_tel, term):
                    entry[cm.tel] = re.sub(pat_tel, '', term).strip()
                else:
                    addr_list.append(term)
            if 'man' in addr_list[-1].lower() or 'woman' in addr_list[-1].lower() or 'kid' in addr_list[-1].lower():
                entry[cm.store_type] = addr_list[-1]
                del addr_list[-1]
            entry[cm.addr_e] = cm.reformat_addr(', '.join(addr_list))

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
            ret = None
            location_valid = True
            if entry[cm.lat] != '' and entry[cm.lng] != '':
                ret = gs.geocode(latlng='%f,%f' % (entry[cm.lat], entry[cm.lng]))
            if ret is None:
                location_valid = False
                ret = gs.geocode('%s, %s, %s' % (entry[cm.addr_e], entry[cm.city_e], entry[cm.country_e]))

            if ret is not None:
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

                if not location_valid:
                    entry[cm.lat] = ret[0]['geometry']['location']['lat']
                    entry[cm.lng] = ret[0]['geometry']['location']['lng']

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
            # 城市列表
            return [{'func': lambda data: func(data, level + 1), 'data': s} for s in fetch_cities(data)]
        if level == 2:
            # 商店
            return [{'func': None, 'data': s} for s in fetch_stores(data)]
        else:
            return []

    # Walk from the root node, where level == 1.
    if data is None:
        data = {'data_url': 'http://www.zara.cn/webapp/wcs/stores/servlet/StoreLocatorResultPage',
                'url': 'http://www.zara.cn/cn/en/stores-c11108.html',
                'brand_id': 10394, 'brandname_e': u'Zara', 'brandname_c': u'飒拉',
                'city_map': gen_city_map()}

    global db
    db = cm.StoresDb()
    db.connect_db(user=user, passwd=passwd)
    db.execute(u'DELETE FROM %s WHERE brand_id=%d' % ('stores', data['brand_id']))

    results = cm.walk_tree({'func': lambda data: func(data, 0), 'data': data})
    db.disconnect_db()
    cm.dump('Done!', log_name)

    return results


