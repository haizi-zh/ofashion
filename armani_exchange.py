# coding=utf-8
import json
import string
import re
import common as cm
import geosense as gs

__author__ = 'Zephyre'

db = None
log_name = 'armani_exchange_log.txt'


def fetch_countries(data):
    url = data['url']
    try:
        body, cookie = cm.get_data_cookie(url)
    except Exception, e:
        cm.dump('Error in fetching countries: %s' % url, log_name)
        return ()

    data['cookie'] = cookie
    results = []
    for m in re.finditer(ur'href="/storelocator.do\?method=submit&country=([A-Z]{2})"', body):
        d = data.copy()
        code = m.group(1)
        d['country_code'] = code
        d['url'] = data['host'] + '/storelocator.do?method=submit&country=' + code
        results.append(d)

    for code in ('US', 'CA'):
        d = data.copy()
        d['country_code'] = code
        d['url'] = data['host'] + '/storelocator.do?method=submit&country=' + code
        results.append(d)

    return tuple(results)


def fetch_stores(data):
    url = data['url']
    try:
        body = cm.get_data(url, cookie=data['cookie'])
    except Exception, e:
        cm.dump('Error in fetching stores: %s' % url, log_name)
        return ()

    store_list = []
    for s in re.findall(ur'<div class="searchboxitem">(.+?)</div>', body, re.S):
        entry = cm.init_store_entry(data['brand_id'], data['brandname_e'], data['brandname_c'])
        entry[cm.country_e] = data['country_code']

        pat_tel = re.compile(ur'([^<>]+)<br>\s*<!--phone added-->')
        m = re.search(pat_tel, s)
        if m:
            entry[cm.tel] = m.group(1).strip()
            entry[cm.hours] = cm.reformat_addr(s[m.end():])
            s = re.sub(pat_tel, '', s[:m.start()])

        m = re.search(ur'<b>([^<>]+)</b>', s)
        entry[cm.name_e] = cm.html2plain(m.group(1)).strip() if m else ''
        entry[cm.addr_e] = cm.reformat_addr(re.sub(ur'<b>([^<>]+)</b>', u'', s))

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
                ret = gs.geocode(', '.join((entry[cm.addr_e], entry[cm.country_e])))

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
        data = {'data_url': 'http://www.armaniexchange.com/storelocator.do',
                'url': 'http://www.armaniexchange.com/storelocator.do',
                'host': 'http://www.armaniexchange.com',
                'brand_id': 10017, 'brandname_e': u'Armani Exchange', 'brandname_c': u'A/X'}

    global db
    db = cm.StoresDb()
    db.connect_db(user=user, passwd=passwd)
    db.execute(u'DELETE FROM %s WHERE brand_id=%d' % ('stores', data['brand_id']))

    results = cm.walk_tree({'func': lambda data: func(data, 0), 'data': data})
    db.disconnect_db()
    cm.dump('Done!', log_name)

    return results


