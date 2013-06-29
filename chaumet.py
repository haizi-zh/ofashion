# coding=utf-8
import json
import string
import re
import common as cm
import geosense as gs

__author__ = 'Zephyre'

db = None
log_name = 'chaumet_log.txt'


def fetch_store_list(data):
    url = data['url']
    try:
        body, cookie = cm.get_data_cookie(url)
        data['cookie'] = cookie
    except Exception, e:
        cm.dump('Error in fetching stores: %s' % url, log_name)
        return ()

    results = []
    for m1 in re.finditer(ur'<li class="size20"\s*><a\s*>([^<>]+)', body):
        continent = m1.group(1).strip().upper()
        continent_sub = cm.extract_closure(body[m1.start():], ur'<li\b', ur'</li>')[0]
        for m2 in re.finditer(ur'<li>([^<>]+)</li>', continent_sub):
            country = cm.html2plain(m2.group(1)).strip().upper()
            country_sub = cm.extract_closure(continent_sub[m2.end():], ur'<ul\b', ur'</ul>')[0]
            for m3 in re.findall(ur'<li class="boutique-detail"\s*>(.+?)</li>', country_sub, re.S):
                m4 = re.search(ur'<a href="([^"]+)"', m3)
                if m4:
                    d = data.copy()
                    d['country'] = country
                    d['url'] = m4.group(1)
                    results.append(d)
    return tuple(results)


def fetch_store_details(data):
    url = data['url']
    try:
        body = cm.get_data(url, cookie=data['cookie'])
    except Exception, e:
        cm.dump('Error in fetching store details: %s' % url, log_name)
        return ()

    start = body.find(ur'<div class="storeLocation">')
    if start == -1:
        cm.dump('Error in fetching store details: %s' % url, log_name)
        return ()
    sub = cm.extract_closure(body[start:], ur'<div\b', ur'</div>')[0]

    entry = cm.init_store_entry(data['brand_id'], data['brandname_e'], data['brandname_c'])
    entry[cm.country_e] = data['country']
    entry[cm.url] = url

    m = re.search(ur'<h2 class="title"\s*>([^<>]+)</h2>', sub)
    entry[cm.name_e] = cm.html2plain(m.group(1)).strip() if m else ''

    m = re.search(ur'<address class="address"\s*>(.+?)</address>', sub, re.S)
    entry[cm.addr_e] = cm.reformat_addr(m.group(1)) if m else ''

    m = re.search(ur'<p>(.+)</p>', sub[m.end():], re.S)
    contact_list = []
    pat_tel = re.compile(ur'phone[\s\d]*[:\.]\s*', re.I)
    pat_fax = re.compile(ur'fax[\s\d]*[:\.]\s*', re.I)
    pat_email = re.compile(ur'email[\s\d]*[:\.]\s*', re.I)
    for term in (tmp.strip() for tmp in cm.reformat_addr(m.group(1)).split(',')):
        if re.search(pat_tel, term):
            entry[cm.tel] = re.sub(pat_tel, '', term).strip()
        elif re.search(pat_fax, term):
            entry[cm.fax] = re.sub(pat_fax, '', term).strip()
        elif re.search(pat_email, term):
            entry[cm.email] = re.sub(pat_email, '', term).strip()

    m = re.search(ur'name="geo\.position"\s*content="(-?\d+\.\d+)\s*;\s*(-?\d+\.\d+)"', body)
    if m:
        entry[cm.lat] = string.atof(m.group(1))
        entry[cm.lng] = string.atof(m.group(2))

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
        if entry[cm.lat] != '' and entry[cm.lng] != '':
            ret = gs.geocode(latlng='%f,%f' % (entry[cm.lat], entry[cm.lng]))
        if ret is None:
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
    return (entry)


def fetch(level=1, data=None, user='root', passwd=''):
    def func(data, level):
        """
        :param data:
        :param level: 0：国家；1：城市；2：商店列表
        """
        if level == 0:
            # 商店列表
            return [{'func': lambda data: func(data, level + 1), 'data': s} for s in fetch_store_list(data)]
        if level == 1:
            # 商店
            return [{'func': None, 'data': s} for s in fetch_store_details(data)]
        else:
            return ()

    # Walk from the root node, where level == 1.
    if data is None:
        data = {'url': 'http://www.chaumet.com/all-points-of-sale',
                'brand_id': 10076, 'brandname_e': u'Chaumet', 'brandname_c': u'尚美巴黎'}

    global db
    db = cm.StoresDb()
    db.connect_db(user=user, passwd=passwd)
    db.execute(u'DELETE FROM %s WHERE brand_id=%d' % ('stores', data['brand_id']))

    results = cm.walk_tree({'func': lambda data: func(data, 0), 'data': data})
    db.disconnect_db()
    cm.dump('Done!', log_name)

    return results


