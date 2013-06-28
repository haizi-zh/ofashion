# coding=utf-8
import json
import string
import re
import common as cm
import geosense as gs

__author__ = 'Zephyre'

db = None
log_name = 'tagheuer_log.txt'
store_map = {}


def gen_city_map():
    with open('city_lite.dat', 'r') as f:
        sub = f.readlines()
    return json.loads(sub[0])


def fetch_countries(data):
    url = data['url']
    param = {'lang': 'en_GB'}
    try:
        body = cm.get_data(url, param)
    except Exception, e:
        cm.dump('Error in fetching countries: %s, %s' % (url, param), log_name)
        return []

    results = []
    for item in json.loads(body):
        if item is None:
            continue
        d = data.copy()
        d['country_code'] = item['isocode']
        d['country'] = cm.html2plain(item['Translation']['en_GB']['label']).strip().upper()
        d['country_id'] = item['id']
        results.append(d)
    return results


tot_processed = 0


def fetch_stores(data):
    store_list = []

    global tot_processed
    tot_processed += 1
    cm.dump('Processint city #%d' % tot_processed, log_name)

    next_val = 0
    while True:
        if next_val == -1:
            break

        url = data['store_url']
        param = {'location_form[services]': data['type_key'], 'location_form[countries]': data['country_id'],
                 'startNextValues': next_val, 'location_form[latitude]': data['city_lat'],
                 'location_form[longitude]': data['city_lng']}
        try:
            body = cm.get_data(url, param)
        except Exception, e:
            cm.dump('Error in fetching stores: %s, %s' % (url, param), log_name)
            return []

        try:
            raw = json.loads(body)
        except Exception, e:
            cm.dump('Error in fetching stores: %s, %s' % (url, param), log_name)
            return []
        if 'next' not in raw or not isinstance(raw['next'], int) or 'data' not in raw or not isinstance(raw['data'],
                                                                                                        list):
            cm.dump('Error in fetching stores: %s, %s' % (url, param), log_name)
            return []
        next_val = raw['next']

        for s in raw['data']:
            # try:
            store_id = string.atoi(s['id'])
            if store_id in store_map:
                item = store_map[store_id]
                cm.dump('Duplicated: %s, %s' % (item[cm.addr_e], item[cm.country_e]), log_name)
                if data['type_key'] == 4:
                    if 'Accessories' not in item[cm.store_type]:
                        item[cm.store_type] += ', Accessories'
                continue

            entry = cm.init_store_entry(data['brand_id'], data['brandname_e'], data['brandname_c'])
            entry[cm.country_e] = data['country']
            entry[cm.city_e] = data['city']
            entry[cm.email] = s['email'].strip()
            entry[cm.fax] = s['fax'].strip()
            entry[cm.addr_e] = cm.reformat_addr(s['address'])

            type_list = []
            type_map = {'iseyewears': 'Eyewear', 'ismobiles': 'Mobiles', 'iswatches': 'Watches'}
            for key in type_map:
                if s[key]:
                    type_list.append(type_map[key])
            entry[cm.store_type] = ', '.join(type_list)

            lat = s['latitude']
            if lat is not None and lat.strip() != '':
                entry[cm.lat] = string.atof(re.search(ur'-?\d+\.?\d*', lat).group())
            lng = s['longitude']
            if lng is not None and lng.strip() != '':
                entry[cm.lng] = string.atof(re.search(ur'-?\d+\.?\d*', lng).group())

            entry[cm.name_e] = cm.html2plain(s['name']).strip()
            entry[cm.tel] = s['phone'].strip()
            entry[cm.store_class] = s['type'].strip()
            entry[cm.zip_code] = s['zipcode'].strip()

            gs.field_sense(entry)
            ret = gs.addr_sense(entry[cm.addr_e], entry[cm.country_e])
            if ret[1] is not None and entry[cm.province_e] == '':
                entry[cm.province_e] = ret[1]
            if ret[2] is not None and entry[cm.city_e] == '':
                entry[cm.city_e] = ret[2]
            gs.field_sense(entry)

            cm.dump('(%s / %d) Found store: %s, %s (%s, %s)' % (data['brandname_e'], data['brand_id'],
                                                                entry[cm.name_e], entry[cm.addr_e],
                                                                entry[cm.country_e],
                                                                entry[cm.continent_e]), log_name)
            db.insert_record(entry, 'stores')
            store_list.append(entry)
            store_map[store_id] = entry
            # except Exception, e:
            #     cm.dump('Error in fetching stores %s, %s' % (s, e), log_name)

    return store_list


def fetch_cities(data):
    ret = gs.look_up(data['country'], 1)
    if ret is None:
        return []

    country = ret['name_e']
    city_map = gen_city_map()
    results = []
    if country in city_map:
        for city in city_map[country]:
            d = data.copy()
            d['country'] = country
            d['city'] = city
            d['city_lat'] = city_map[country][city]['lat']
            d['city_lng'] = city_map[country][city]['lng']
            results.append(d)
    return results


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

    global db
    db = cm.StoresDb()
    db.connect_db(user=user, passwd=passwd)
    db.execute(u'DELETE FROM %s WHERE brand_id=%d' % ('stores', 10345))

    results = []
    type_map = {1: 'Watches', 2: 'Eyewears', 3: 'Mobiles', 4: 'Accessorie'}
    for key in type_map:
        # Walk from the root node, where level == 1.
        data = {'store_url': 'http://www.tagheuer.cn/StoreLocator/SearchByLocation',
                'url': 'http://www.tagheuer.cn/flashservice/json/CountryListService/filter/SelectAllForService/servicecode/' +
                       type_map[key], 'type_key': key,
                'brand_id': 10345, 'brandname_e': u'Tagheuer', 'brandname_c': u'古驰',
                'city_map': gen_city_map()}

        results.extend(cm.walk_tree({'func': lambda data: func(data, 0), 'data': data}))

    for entry in store_map.values():
        db.insert_record(entry, 'stores')

    db.disconnect_db()
    cm.dump('Done!', log_name)

    return results


