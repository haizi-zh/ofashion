# coding=utf-8
import json
import string
import re
import common as cm
import geosense as gs

__author__ = 'Zephyre'

db = None
log_name = 'dior_log.txt'
store_map = None


def fetch_countries(data):
    url = data['data_url']
    param = {'node_id': data['continent_id'], 'location_id': 0}
    try:
        body = cm.post_data(url, param)
    except Exception, e:
        cm.dump('Error in fetching countries: %s, %s' % (url, param), log_name)
        return ()

    results = []
    for m in re.findall(ur'<li[^<>]*data-value="(\d+)"\s*>([^<>]+)', body):
        d = data.copy()
        d['country_id'] = string.atoi(m[0])
        d['country'] = cm.html2plain(m[1]).strip().upper()
        results.append(d)
    return tuple(results)


def fetch_stores(data):
    url = data['store_url']
    param = {'univers[mode_femme]': 'pla_women', 'univers[mode_homme]': 'pla_dior_men',
             'univers[baby_dior]': 'pla_baby_dior', 'univers[horlogerie]': 'pla_watches',
             'univers[joaillerie]': 'pla_fine_jewelry', 'univers[dior_phone]': 'pla_dior_phone',
             'continent': data['continent_id'], 'pays': data['country_id'], 'ville': data['city_id'],
             'node_id': '581', 'search': 'SEARCH'}
    if not data['no_region']:
        param['region'] = data['state_id']
    try:
        body = cm.post_data(url, param)
    except Exception, e:
        cm.dump('Error in fetching stores: %s, %s' % (url, param), log_name)
        return ()

    start = body.find(ur'<div class="wrap-list-locator">')
    if start == -1:
        cm.dump('Error in fetching stores: %s, %s' % (url, param), log_name)
        return ()
    body = cm.extract_closure(body[start:], ur'<div\b', ur'</div>')[0]

    store_list = []
    for store_sub in re.findall(ur'<li[^<>]*>(.+?)</li>', body, re.S):
        entry = cm.init_store_entry(data['brand_id'], data['brandname_e'], data['brandname_c'])
        entry[cm.country_e] = data['country']
        entry[cm.city_e] = data['city']
        entry[cm.province_e] = data['state'] if not data['no_region'] else ''

        m = re.search(ur'itemprop="latitude"\s+content="([^"]+)"', store_sub)
        try:
            if m:
                entry[cm.lat] = string.atof(m.group(1))
        except ValueError as e:
            cm.dump('Error in fetching lat: %s' % str(e), log_name)
        m = re.search(ur'itemprop="longitude"\s+content="([^"]+)"', store_sub)
        try:
            if m:
                entry[cm.lng] = string.atof(m.group(1))
        except ValueError as e:
            cm.dump('Error in fetching lng: %s' % str(e), log_name)

        m = re.search(ur'<div class="desc-boutik">', store_sub)
        if not m:
            continue
        sub = cm.extract_closure(store_sub[m.start():], ur'<div\b', ur'</div>')[0]

        m1 = re.search(ur'<h3[^<>]*>([^<>]+)', sub)
        entry[cm.name_e] = cm.html2plain(m1.group(1)) if m1 else ''

        m1 = re.search(ur'<div class="addr-boutik"[^<>]*>(.+?)</div>', sub, re.S)
        if m1:
            addr_sub = m1.group(1)
            entry[cm.addr_e] = cm.reformat_addr(addr_sub)
            m2 = re.search(ur'<span itemprop="postalCode">([^<>]+)', addr_sub)
            entry[cm.zip_code] = m2.group(1).strip() if m2 else ''

        m1 = re.search(ur'<span itemprop="telephone">([^<>]+)</span>', sub)
        if m1:
            entry[cm.tel] = re.sub(re.compile(ur'tel[\.\s:]+', re.I), '', m1.group(1))
        m1 = re.search(ur'<span itemprop="faxNumber">([^<>]+)</span>', sub)
        if m1:
            entry[cm.fax] = re.sub(re.compile(ur'fax[\.\s:]+', re.I), '', m1.group(1))

        m1 = re.search(ur'<div class="tags-boutik">(.+?)</div>', sub, re.S)
        entry[cm.store_type] = m1.group(1).strip() if m1 else ''

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

    return tuple(store_list)


def fetch_continents(data):
    url = data['url']
    try:
        body = cm.get_data(url)
    except Exception, e:
        cm.dump('Error in fetching continents: %s' % url, log_name)
        return ()

    start = body.find(ur'SELECT A CONTINENT')
    if start == -1:
        cm.dump('Error in fetching continents: %s' % url, log_name)
        return ()
    m = re.search(ur'<ul[^<>]*>(.+?)</ul>', body[start:], re.S)
    results = []
    sub = m.group(1)
    for m in re.findall(ur'<li[^<>]+data-value="(\d+)">([^<>]+)', sub):
        d = data.copy()
        d['continent_id'] = string.atoi(m[0])
        d['continent'] = m[1].strip().upper()
        results.append(d)
    return tuple(results)


def fetch_states(data):
    url = data['data_url']
    param = {'node_id': data['country_id'], 'location_id': 1}
    try:
        body = cm.post_data(url, param)
    except Exception, e:
        cm.dump('Error in fetching states: %s, %s' % (url, param), log_name)
        return ()

    data['no_region'] = False
    results = []
    for m in re.findall(ur'<li[^<>]*data-value="(\d+)"\s*>([^<>]+)', body):
        d = data.copy()
        d['state_id'] = string.atoi(m[0])
        d['state'] = cm.html2plain(m[1]).strip().upper()
        results.append(d)

    if len(results) > 0:
        return tuple(results)
    else:
        d = data.copy()
        d['no_region'] = True
        return (d,)


def fetch_cities(data):
    url = data['data_url']
    if data['no_region']:
        param = {'node_id': data['country_id'], 'location_id': 2, 'no_region': 1}
    else:
        param = {'node_id': data['state_id'], 'location_id': 2}

    try:
        body = cm.post_data(url, param)
    except Exception, e:
        cm.dump('Error in fetching cities: %s, %s' % (url, param), log_name)
        return ()

    results = []
    for m in re.findall(ur'<li[^<>]*data-value="(\d+)"\s*>([^<>]+)', body):
        d = data.copy()
        d['city_id'] = string.atoi(m[0])
        d['city'] = cm.html2plain(m[1]).strip().upper()
        results.append(d)
    return tuple(results)


def fetch_dior_beauty(data):
    url = data['url']
    store_list = []

    with open('city_lite.dat', 'r') as f:
        sub = f.readlines()
    city_map = json.loads(sub[0])
    country = 'CHINA'
    for city in city_map[country]:
        param = {'cityName': city}
        cm.dump('Searching at %s, %s' % (city, country), log_name)
        try:
            body = cm.post_data(url, param)
        except Exception, e:
            cm.dump('Error in fetching states: %s, %s' % (url, param), log_name)
            continue

        m = re.search(ur'var\s+Json\s*=', body)
        if not m:
            continue
        sub = cm.extract_closure(body[m.end():], ur'\{', ur'\}')[0]
        for store in json.loads(sub)['content']['items']:
            entry = cm.init_store_entry(data['brand_id'], data['brandname_e'], data['brandname_c'])
            entry[cm.country_e] = country
            entry[cm.comments] = 'BEAUTY'

            addr_list = []
            val = store['addressLine1']
            if val:
                addr_list.append(cm.html2plain(val).strip())
            val = store['addressLine2']
            if val:
                addr_list.append(cm.html2plain(val).strip())
            entry[cm.addr_e] = ', '.join(addr_list)

            val = store['name']
            entry[cm.name_e] = cm.html2plain(val).strip() if val else ''
            val = store['type']
            entry[cm.store_class] = cm.html2plain(val).strip() if val else ''
            val = store['url']
            entry[cm.url] = cm.html2plain(val).strip() if val else ''
            val = store['city']
            entry[cm.city_e] = cm.html2plain(val).strip().upper() if val and val != '' else ''
            val = store['zipcode']
            entry[cm.zip_code] = cm.html2plain(val).strip() if val else ''
            val = store['phone']
            entry[cm.tel] = cm.html2plain(val).strip() if val else ''
            val = store['fax']
            entry[cm.fax] = cm.html2plain(val).strip() if val else ''

            coords = store['coords']
            if coords:
                try:
                    entry[cm.lat] = string.atof(str(coords['lat']))
                except (ValueError, KeyError, TypeError) as e:
                    cm.dump('Error in fetching lat: %s' % str(e), log_name)
                try:
                    entry[cm.lng] = string.atof(str(coords['lng']))
                except (ValueError, KeyError, TypeError) as e:
                    cm.dump('Error in fetching lng: %s' % str(e), log_name)

            gs.field_sense(entry)
            ret = gs.addr_sense(entry[cm.addr_e], entry[cm.country_e])
            if ret[1] is not None and entry[cm.province_e] == '':
                entry[cm.province_e] = ret[1]
            if ret[2] is not None and entry[cm.city_e] == '':
                entry[cm.city_e] = ret[2]
            gs.field_sense(entry)

            uid = u'%s|%s|%s|%s|%s,%s' % (
                entry[cm.name_e], entry[cm.addr_e], entry[cm.city_e], entry[cm.country_e], unicode(entry[cm.lat]),
                unicode(entry[cm.lng]))
            if uid in store_map:
                cm.dump(u'%s already exists.' % uid)
                continue
            else:
                store_map[uid] = entry
                cm.dump('(%s / %d) Found store: %s, %s (%s, %s)' % (data['brandname_e'], data['brand_id'],
                                                                    entry[cm.name_e], entry[cm.addr_e],
                                                                    entry[cm.country_e],
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
            # 洲列表
            return [{'func': lambda data: func(data, level + 1), 'data': s} for s in fetch_continents(data)]
        if level == 1:
            # 国家列表
            return [{'func': lambda data: func(data, level + 1), 'data': s} for s in fetch_countries(data)]
        if level == 2:
            # 州列表
            return [{'func': lambda data: func(data, level + 1), 'data': s} for s in fetch_states(data)]
        if level == 3:
            # 国家列表
            return [{'func': lambda data: func(data, level + 1), 'data': s} for s in fetch_cities(data)]
        if level == 4:
            # 商店
            return [{'func': None, 'data': s} for s in fetch_stores(data)]
        else:
            return ()

    # Walk from the root node, where level == 1.
    if data is None:
        data = {'data_url': 'http://www.dior.com/couture/en_us/store/select',
                'store_url': 'http://www.dior.com/couture/en_us/store/search_result',
                'url': 'http://www.dior.com/couture/en_us/content/view/store_locator_form/581',
                'brand_id': 10106, 'brandname_e': u'Dior', 'brandname_c': u'迪奥'}

    global db, store_map
    store_map = {}
    db = cm.StoresDb()
    db.connect_db(user=user, passwd=passwd)
    # db.execute(u'DELETE FROM %s WHERE brand_id=%d' % ('stores', data['brand_id']))

    # results = cm.walk_tree({'func': lambda data: func(data, 0), 'data': data})
    #
    # data['url']='http://www.dior.com/beauty/gbr/en/actions/storelocator-search.ep'
    # results.extend(fetch_dior_beauty(data))

    results = []
    data['url'] = 'http://www.dior.cn/beauty/chn/zh/actions/storelocator-search.ep'
    results.extend(fetch_dior_beauty(data))

    db.disconnect_db()
    cm.dump('Done!', log_name)

    return results


