# coding=utf-8
import json
import string
import re
import common as cm
import geosense as gs

__author__ = 'Zephyre'

db = None
log_name = 'xxxx_log.txt'


def fetch_countries(data):
    url = data['url']
    try:
        body = cm.get_data(url)
    except Exception, e:
        cm.dump('Error in fetching countries: %s' % url, log_name)
        return []

    results = []
    for m in re.findall(ur'<td\s+[^<>]*class\s*=\s*"country"[^<>]*><a id="(\d+)" href="([^"]+)"\s*>([^<>]+)', body):
        d = data.copy()
        d['country_id'] = string.atoi(m[0])
        d['country'] = cm.html2plain(m[2]).strip().upper()
        d['url'] = m[1]
        results.append(d)
    return tuple(results)


def fetch_stores(data):
    url = data['host'] + data['url'] + '.xml'
    try:
        body = cm.get_data(url)
    except Exception, e:
        cm.dump('Error in fetching stores: %s' % url, log_name)
        return []

    start = body.find(ur'<![CDATA')
    if start == -1:
        cm.dump('Error in fetching countries: %s' % url, log_name)
        return []
    body = cm.extract_closure(body[start + 6:], ur'\[', ur'\]')[0]

    store_list = []
    for m in re.finditer(ur'<div class="store ', body):
        s = cm.extract_closure(body[m.end():], ur'<div\b', ur'</div>')[0]
        entry = cm.init_store_entry(data['brand_id'], data['brandname_e'], data['brandname_c'])

        m1 = re.search(ur'<h6>([^<>]+)</h6>', s)
        if m1 is not None:
            entry[cm.name_e] = m1.group(1).strip()

        addr_sub = cm.extract_closure(s, ur'<p>', ur'</p>')[0]
        addr_list = [term.strip() for term in cm.reformat_addr(addr_sub).split(',')]
        tel = cm.extract_tel(addr_list[-1])
        if tel != '':
            entry[cm.tel] = tel
            del addr_list[-1]
        entry[cm.addr_e] = ', '.join(addr_list)

        m1 = re.search(ur'll=(-?\d+\.\d+),(-?\d+\.\d+)', addr_sub)
        if m1 is not None:
            entry[cm.lat] = string.atof(m1.group(1))
            entry[cm.lng] = string.atof(m1.group(2))

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
        store_list.append(entry)

    return store_list


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
            return []

    # Walk from the root node, where level == 1.
    if data is None:
        data = {'host': 'http://www.valentino.com',
                'url': 'http://www.valentino.com/en/home/store_locator/',
                'brand_id': 10367, 'brandname_e': u'Valentino', 'brandname_c': u'华伦天奴'}

    global db
    db = cm.StoresDb()
    db.connect_db(user=user, passwd=passwd)
    db.execute(u'DELETE FROM %s WHERE brand_id=%d' % ('stores', data['brand_id']))

    results = cm.walk_tree({'func': lambda data: func(data, 0), 'data': data})
    db.disconnect_db()

    return results


