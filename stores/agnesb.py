# coding=utf-8
import string
import re
from stores import geosense as gs

__author__ = 'Zephyre'

db = None
log_name = 'agnesb_log.txt'


def fetch_countries(data):
    url = data['url']
    try:
        body = cm.get_data(url)
    except Exception, e:
        cm.dump('Error in fetching countries: %s' % url, log_name)
        return []
    m = re.search(ur'<select name="zone"[^<>]*>(.+?)</select>', body, re.S)
    if m is None:
        cm.dump('Error in fetching countries: %s' % url, log_name)
        return []

    results = []
    for item in re.findall(ur'<option value="(\d+)">([^<>]+)</option>', m.group(1), re.S):
        d = data.copy()
        d['zone_id'] = string.atoi(item[0])
        d['zone'] = item[1].strip()
        results.append(d)
    return tuple(results)


def fetch_store_details(data):
    url = data['url']
    try:
        body = cm.get_data(url)
    except Exception, e:
        cm.dump('Error in fetching stores: %s' % url, log_name)
        return []

    m = re.search(ur'<div id="coordonnees"[^<>]*>(.+?)</div>', body, re.S)
    if m is None:
        cm.dump('Error in fetching stores: %s' % url, log_name)
        return []

    entry = cm.init_store_entry(data['brand_id'], data['brandname_e'], data['brandname_c'])

    addr_sub, info_sub = m.group(1).split('Practical Info')
    m = re.search(ur'<h2>(.+?)</h2>', addr_sub)
    if m is not None:
        entry[cm.name_e] = cm.html2plain(m.group(1))
    addr_list = []
    for term in re.findall(ur'<p>(.+?)</p>', addr_sub):
        tmp = cm.reformat_addr(term)
        if 'tel' in tmp.lower():
            tel = cm.extract_tel(tmp)
            if tel != '':
                entry[cm.tel] = tel
        elif 'fax' in tmp.lower():
            fax = cm.extract_tel(tmp)
            if fax != '':
                entry[cm.fax] = fax
        elif tmp != '':
            addr_list.append(tmp)
    entry[cm.addr_e] = ', '.join(addr_list)

    for term in (tmp.strip() for tmp in cm.reformat_addr(info_sub).split(',')):
        if '@' in term and '.' in term:
            entry[cm.email] = term
        elif 'www.' in term or '.com' in term or '.cn' in term:
            entry[cm.url] = term
        else:
            m = re.search(ur'^Lines:(.+)', term)
            if m is not None:
                entry[cm.store_type] = m.group(1).strip()

    m = re.search(ur'<span id="latitude"[^<>]*>(-?\d+\.\d+)', body)
    if m is not None:
        entry[cm.lat] = string.atof(m.group(1))
    m = re.search(ur'<span id="longitude"[^<>]*>(-?\d+\.\d+)', body)
    if m is not None:
        entry[cm.lng] = string.atof(m.group(1))

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
            ret = gs.geocode(entry[cm.addr_e])

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

    cm.dump('(%s / %d) Found store: %s, %s (%s, %s)' % (data['brandname_e'], data['brand_id'],
                                                        entry[cm.name_e], entry[cm.addr_e], entry[cm.country_e],
                                                        entry[cm.continent_e]), log_name)
    db.insert_record(entry, 'stores')
    return [entry]


def fetch_store_list(data):
    url = data['data_url']
    param = {'zone': data['zone_id']}
    try:
        body = cm.post_data(url, param)
    except Exception, e:
        cm.dump('Error in fetching store list: %s, %s' % (url, param), log_name)
        return []

    results = []
    for item in re.findall(ur'<div class="boutique"\s*>(.+?)</div>', body, re.S):
        m = re.search(ur'<a class="info" href="([^"]+)"', item)
        if m is not None:
            d = data.copy()
            d['url'] = data['host'] + m.group(1)
            results.append(d)
    return tuple(results)


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
            # 国家列表
            return [{'func': lambda data: func(data, level + 1), 'data': s} for s in fetch_store_list(data)]
        if level == 2:
            # 商店
            return [{'func': None, 'data': s} for s in fetch_store_details(data)]
        else:
            return []

    # Walk from the root node, where level == 1.
    if data is None:
        data = {'data_url': 'http://usa.agnesb.com/en/boutiques/updateZone',
                'url': 'http://usa.agnesb.com/en/shops',
                'host': 'http://usa.agnesb.com',
                'brand_id': 10006, 'brandname_e': u'Agnes B', 'brandname_c': u'阿尼亚斯贝'}

    global db
    db = cm.StoresDb()
    db.connect_db(user=user, passwd=passwd)
    db.execute(u'DELETE FROM %s WHERE brand_id=%d' % ('stores', data['brand_id']))

    results = cm.walk_tree({'func': lambda data: func(data, 0), 'data': data})
    db.disconnect_db()
    cm.dump('Done!', log_name)

    return results


