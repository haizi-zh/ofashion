# coding=utf-8
import json
import string
import re
import common as cm
import geosense as gs

__author__ = 'Zephyre'

db = None
log_name = 'cartier_log.txt'


def fetch_countries(data):
    url = data['url']
    try:
        body, cookie = cm.get_data_cookie(url)
    except Exception:
        cm.dump('Error in fetching countries: %s' % url, log_name)
        return []

    m = re.search(ur'name="form_build_id" value="(.+?)"', body)
    if m is None:
        cm.dump('Error in fetching countries: %s' % url, log_name)
        return []
    data['form_build_id'] = m.group(1)
    if cookie is None:
        data['cookie'] = ''
    else:
        data['cookie'] = cookie

    start = body.find(ur'<select id="edit-countries"')
    if start == -1:
        cm.dump('Error in fetching countries: %s' % url, log_name)
        return []
    body = cm.extract_closure(body[start:], ur'<select\b', ur'</select>')[0]

    results = []
    for m in re.findall(ur'<option.+?value="([A-Z]{3})".*?>(.+?)</option>', body):
        d = data.copy()
        d['country_code'] = m[0]
        d['country'] = m[1].strip()
        print 'Country: %s, %s' % (d['country_code'], d['country'])
        results.append(d)
    return results


def fetch_cities(data):
    url = data['data_url']
    param = {'countries': data['country_code'],
             'form_build_id': data['form_build_id'],
             'form_id': 'cartierfo_generic_store_locator_search_form',
             '_triggering_element_name': 'countries'}
    try:
        body, cookie = cm.post_data_cookie(url, param, cookie=data['cookie'])
    except Exception:
        cm.dump('Error in fetching cities: %s, %s' % (url, param), log_name)
        return []

    if cookie is not None:
        data['cookie'] = cookie

    raw = json.loads(body)
    body = None
    for item in raw:
        if 'data' in item and item['data'] != '':
            body = item['data']
            break
    if body is None:
        cm.dump('Error in fetching cities: %s, %s' % (url, param), log_name)
        return []

    # body = body.decode('unicode_escape')
    start = body.find(ur'<select id="edit-cities"')
    if start == -1:
        cm.dump('Error in fetching cities: %s, %s' % (url, param), log_name)
        return []
    body = cm.extract_closure(body[start:], ur'<select\b', ur'</select>')[0]

    results = []
    for m in re.findall(ur'<option.+?value="([^"]+?)".*?>.+?</option>', body):
        d = data.copy()
        d['city'] = cm.html2plain(m)
        results.append(d)
        print 'Country: %s, City: %s' % (data['country'], d['city'])

    return results


def parse_store(data, body=None):
    if body is None:
        url = data['url']
        try:
            body = cm.post_data(url)
        except Exception:
            cm.dump('Error in fetching stores: %s' % url, log_name)
            return []

    start = body.find(ur'jQuery.extend(Drupal.settings,')
    latlng_map = {}
    if start != -1:
        for item in json.loads(cm.extract_closure(body[start:], ur'\{', ur'\}')[0])['getlocations']['key_1']['latlons']:
            latlng_map[cm.reformat_addr(item[3])] = {'lat': string.atof(item[0]), 'lng': string.atof(item[1])}

    store_list = []
    for m in re.finditer(ur'<li class="store-item', body):
        sub = cm.extract_closure(body[m.end():], ur'<li\b', ur'</li>')[0]
        start = sub.find(u'<div class="l-col-sub l-col-sub2">')
        store_type = ''
        if start != -1:
            sub1 = cm.extract_closure(sub[start:], ur'<div\b', ur'</div>')[0]
            store_type = ', '.join(re.findall(ur'<li>(.+?)</li>', sub1))

        start = sub.find(u'<div class="l-col-sub l-col-sub1">')
        if start == -1:
            cm.dump('Error in parsing %s' % m.group(1), log_name)
            continue
        sub1 = cm.extract_closure(sub[start:], ur'<div\b', ur'</div>')[0]

        entry = cm.init_store_entry(data['brand_id'], data['brandname_e'], data['brandname_c'])
        entry[cm.store_type] = store_type

        m1 = re.search(ur'<p class="store-item-name">(.+?)</p>', sub1, re.S)
        if m1 is not None:
            entry[cm.name_e] = cm.reformat_addr(m1.group(1))
        m1 = re.search(ur'<p class="store-item-adress">(.+?)</p>', sub1, re.S)
        if m1 is not None:
            entry[cm.addr_e] = cm.reformat_addr(m1.group(1))

        entry[cm.tel] = cm.extract_tel(sub1)
        ret = gs.look_up(data['country_code'], 1)
        if ret is not None:
            entry[cm.country_e] = ret['name_e']
        ret = gs.addr_sense(entry[cm.addr_e], entry[cm.country_e])
        if ret[1] is not None:
            entry[cm.province_e] = ret[1]
        if ret[2] is not None:
            entry[cm.city_e] = ret[2]
        else:
            entry[cm.city_e] = data['city'].strip().upper()

        if entry[cm.name_e] in latlng_map:
            tmp = latlng_map[entry[cm.name_e]]
            entry[cm.lat] = tmp['lat']
            entry[cm.lng] = tmp['lng']

        gs.field_sense(entry)
        cm.dump('(%s / %d) Found store: %s, %s (%s, %s)' % (data['brandname_e'], data['brand_id'],
                                                            entry[cm.name_e], entry[cm.addr_e], entry[cm.country_e],
                                                            entry[cm.continent_e]), log_name)
        db.insert_record(entry, 'stores')
        store_list.append(entry)

    return store_list


def fetch_stores(data):
    url = data['url']
    param = {'countries': data['country_code'],
             'cities': data['city'],
             'op': 'Submit',
             'form_build_id': data['form_build_id'],
             'form_id': 'cartierfo_generic_store_locator_search_form'}
    try:
        body = cm.post_data(url, param)
    except Exception:
        cm.dump('Error in fetching stores: %s, %s' % (url, param), log_name)
        return []

    m = re.search(ur'The document has moved <A HREF="(.+?)">here', body, re.I)
    if m is not None:
        data['url'] = m.group(1)
        return parse_store(data)
    else:
        return parse_store(data, body)


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
            pass
        else:
            return []

    # Walk from the root node, where level == 1.
    if data is None:
        data = {'url': 'http://www.cartier.us/find-boutique',
                'data_url': 'http://www.cartier.us/system/ajax',
                'brand_id': 10066, 'brandname_e': u'Cartier', 'brandname_c': u'卡地亚'}

    global db
    db = cm.StoresDb()
    db.connect_db(user=user, passwd=passwd)
    db.execute(u'DELETE FROM %s WHERE brand_id=%d' % ('stores', data['brand_id']))

    results = cm.walk_tree({'func': lambda data: func(data, 0), 'data': data})
    db.disconnect_db()

    return results

