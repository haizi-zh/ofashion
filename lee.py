# coding=utf-8
import json
import string
import re
import common as cm
import geosense as gs

__author__ = 'Zephyre'

db = None
log_name = 'lee_log.txt'
from pyquery import PyQuery as pq


def fetch_countries_eu(data):
    url = data['url_eu']
    try:
        body = cm.get_data(url)
    except Exception, e:
        cm.dump('Error in fetching EU countries: %s' % url, log_name)
        return ()
    m = re.search(ur'<div class="cf store-big-links">(.+?)</div>', body, re.S)
    if not m:
        cm.dump('Error in fetching EU countries: %s' % url, log_name)
        return ()
    sub = m.group(1)
    results = []
    for m in re.findall(ur'<a class="big-store-link" data-term="([^"]+)"', sub):
        d = data.copy()
        d['city_code'] = m
        d['city'] = cm.html2plain(m).strip().upper()
        results.append(d)
    return tuple(results)


def fetch_stores_eu(data):
    url = data['data_url_eu']
    param = {'query': data['city_code']}
    try:
        body = cm.post_data(url, param)
    except Exception, e:
        cm.dump('Error in fetching stores: %s, %s' % (url, param), log_name)
        return ()

    store_list = []
    for s in json.loads(body)['data']:
        entry = cm.init_store_entry(data['brand_id'], data['brandname_e'], data['brandname_c'])

        val = s['name']
        entry[cm.name_e] = cm.html2plain(val).strip() if val else ''
        val = s['city']
        entry[cm.city_e] = cm.html2plain(val).strip().upper() if val else ''
        val = s['phone']
        entry[cm.tel] = val.strip() if val else ''
        val = s['zip']
        entry[cm.zip_code] = val.strip() if val else ''
        val = s['street']
        entry[cm.addr_e] = cm.reformat_addr(val) if val else ''

        term_list = {'lee101': 'Lee 101', 'leearchives': 'Archives', 'leekids': 'Kids',
                     'mencollection': 'Men Collection',
                     'womencollection': 'Women Collection'}
        type_list = []
        for item in term_list.items():
            if string.atoi(str(s[item[0]])) != 0:
                type_list.append(item[1])
        entry[cm.store_type] = ', '.join(type_list)

        try:
            entry[cm.lat] = string.atof(s['latitude']) if s['latitude'] != '' else ''
        except (ValueError, KeyError, TypeError) as e:
            cm.dump('Error in fetching lat: %s' % str(e), log_name)
        try:
            entry[cm.lng] = string.atof(s['longitude']) if s['longitude'] != '' else ''
        except (ValueError, KeyError, TypeError) as e:
            cm.dump('Error in fetching lng: %s' % str(e), log_name)

        gs.field_sense(entry)
        ret = gs.addr_sense(entry[cm.addr_e])
        if ret[0] is not None and entry[cm.country_e] == '':
            entry[cm.country_e] = ret[0]
        if ret[1] is not None and entry[cm.province_e] == '':
            entry[cm.province_e] = ret[1]
        if ret[2] is not None and entry[cm.city_e] == '':
            entry[cm.city_e] = ret[2]
        gs.field_sense(entry)

        if (entry[cm.country_e] == '' or entry[cm.city_e] == ''):
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

                entry['is_geocoded'] = 1

                gs.field_sense(entry)
                ret = gs.addr_sense(entry[cm.addr_e])
                if ret[0] is not None and entry[cm.country_e] == '':
                    entry[cm.country_e] = ret[0]
                if ret[1] is not None and entry[cm.province_e] == '':
                    entry[cm.province_e] = ret[1]
                if ret[2] is not None and entry[cm.city_e] == '':
                    entry[cm.city_e] = ret[2]
                gs.field_sense(entry)

        cm.dump('(%s / %d) Found store: %s, %s (%s, %s, %s)' % (data['brandname_e'], data['brand_id'],
                                                                entry[cm.name_e], entry[cm.addr_e], entry[cm.city_e],
                                                                entry[cm.country_e], entry[cm.continent_e]), log_name)
        db.insert_record(entry, 'stores')
        store_list.append(entry)

    return tuple(store_list)


def fetch_cn(data):
    url = 'http://www.lee.com.cn/xml/storefinder.xml'

    store_list = []
    for store in (pq(tmp) for tmp in pq(url=url)('NewDataSet Table')):
        entry = cm.init_store_entry(data['brand_id'], data['brandname_e'], data['brandname_c'])
        tmp = store('shop_name')[0]
        entry[cm.name_e] = tmp.text if tmp.text else ''
        entry[cm.country_e] = 'CHINA'
        tmp = store('city')[0]
        entry[cm.city_e] = tmp.text if tmp.text else ''
        tmp = store('province')[0]
        entry[cm.province_e] = tmp.text if tmp.text else ''
        tmp = store('district')[0]
        entry[cm.district_e] = tmp.text if tmp.text else ''
        tmp = store('address')[0]
        entry[cm.addr_e] = tmp.text if tmp.text else ''
        tmp = store('tel')[0]
        entry[cm.tel] = tmp.text if tmp.text else ''

        gs.field_sense(entry)
        ret = gs.addr_sense(entry[cm.addr_e])
        if ret[0] is not None and entry[cm.country_e] == '':
            entry[cm.country_e] = ret[0]
        if ret[1] is not None and entry[cm.province_e] == '':
            entry[cm.province_e] = ret[1]
        if ret[2] is not None and entry[cm.city_e] == '':
            entry[cm.city_e] = ret[2]
        gs.field_sense(entry)
        cm.dump('(%s / %d) Found store: %s, %s (%s, %s, %s)' % (data['brandname_e'], data['brand_id'],
                                                                entry[cm.name_e], entry[cm.addr_e], entry[cm.city_e],
                                                                entry[cm.country_e], entry[cm.continent_e]), log_name)
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
            return [{'func': lambda data: func(data, level + 1), 'data': s} for s in fetch_countries_eu(data)]
        if level == 1:
            # 商店
            return [{'func': None, 'data': s} for s in fetch_stores_eu(data)]
        else:
            return ()

    # Walk from the root node, where level == 1.
    if data is None:
        data = {'data_url_eu': 'http://eu.lee.com/ajax/getMapIcons.php',
                'url_eu': 'http://eu.lee.com/stores',
                'brand_id': 10213, 'brandname_e': u'LEE', 'brandname_c': u'LEE'}

    global db
    db = cm.StoresDb()
    db.connect_db(user=user, passwd=passwd)
    # db.execute(u'DELETE FROM %s WHERE brand_id=%d' % ('stores', data['brand_id']))

    # results = cm.walk_tree({'func': lambda data: func(data, 0), 'data': data})

    results = fetch_cn(data)

    db.disconnect_db()
    cm.dump('Done!', log_name)

    return results


