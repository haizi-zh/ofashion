# coding=utf-8
import json
import string
import re
import traceback
import common as cm
import geosense as gs
from pyquery import PyQuery as pq

__author__ = 'Zephyre'

db = None
log_name = 'tommy_global_log.txt'
store_map = {}
id_set = None


def fetch_countries(data):
    url = data['url']
    try:
        body = cm.get_data(url)
    except Exception, e:
        cm.dump('Error in fetching countries: %s' % url, log_name)
        return ()

    m = re.search(ur'<option value="">COUNTRY</option>(.+?)</select>', body, re.S)
    sub = m.group(1)
    results = []
    for m in re.findall(ur'<option value="([^"]+)"[^<>]*>([^<>]+)', sub):
        d = data.copy()
        d['country_code'] = m[0]
        d['country'] = cm.html2plain(m[1]).strip().upper()
        if re.search(ur'\d+', d['country']) or re.search(ur'^[A-Z]{2}$', d['country']):
            continue
        results.append(d)
    return tuple(results)


def fetch_stores(data):
    url = data['url']
    param = {'storecountry': data['country_code']}
    try:
        body = cm.get_data(url, param)
    except Exception, e:
        cm.dump('Error in fetching stores: %s, %s' % (url, param), log_name)
        return ()

    store_list = []
    for store in pq(body)('li'):
        keys = store.attrib.keys()
        if 'id' not in keys or 'class' not in keys or 'data-longitude' not in keys or \
                        'data-latitude' not in keys:
            continue

        entry = cm.init_store_entry(data['brand_id'], data['brandname_e'], data['brandname_c'])
        entry[cm.country_e] = data['country']
        store_id = store.attrib['id']
        if store_id in store_map:
            continue
        else:
            store_map[store_id] = entry

        if store_id in id_set:
            cm.dump('%s already exists.' % store_id, log_name)
            continue

        try:
            try:
                entry[cm.lat] = string.atof(store.attrib['data-latitude'])
            except (ValueError, KeyError, TypeError) as e:
                cm.dump('Error in fetching lat: %s' % str(e), log_name)
            try:
                entry[cm.lng] = string.atof(store.attrib['data-longitude'])
            except (ValueError, KeyError, TypeError) as e:
                cm.dump('Error in fetching lng: %s' % str(e), log_name)

            if entry[cm.lat] == 0 and entry[cm.lng] == 0:
                entry[cm.lat], entry[cm.lng] = '', ''

            item = pq(store)
            tmp = item('h1')
            entry[cm.name_e] = cm.html2plain(tmp[0].text).strip() if len(tmp) > 0 and tmp[0].text else ''

            tmp = item('dd.location')
            tmp = tmp[0].text if len(tmp) > 0 and tmp[0].text else ''
            entry[cm.city_e] = cm.extract_city(tmp)[0]

            tmp = item('dd.street')
            tmp = tmp[0].text if len(tmp) > 0 and tmp[0].text else ''
            entry[cm.addr_e] = cm.reformat_addr(tmp)

            tmp = item('dd.phone')
            tmp = tmp[0].text if len(tmp) > 0 and tmp[0].text else ''
            entry[cm.tel] = tmp.strip()

            tmp = item('dd.hours')
            tmp = tmp[0].text if len(tmp) > 0 and tmp[0].text else ''
            entry[cm.hours] = tmp.strip()

            tmp = item('dd.products')
            tmp = tmp[0].text if len(tmp) > 0 and tmp[0].text else ''
            entry[cm.store_type] = tmp.strip()

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
                    entry[cm.native_id] = store_id
                else:
                    entry[cm.native_id] = ''
            else:
                entry[cm.native_id] = store_id

            entry[cm.city_e] = cm.extract_city(entry[cm.city_e])[0]
            cm.dump('(%s / %d) Found store: %s, %s (%s, %s)' % (data['brandname_e'], data['brand_id'],
                                                                entry[cm.name_e], entry[cm.addr_e], entry[cm.country_e],
                                                                entry[cm.continent_e]), log_name)
            db.insert_record(entry, 'stores')
            store_list.append(entry)
        except (IndexError, TypeError) as e:
            cm.dump('Error in parsing stores', log_name)
            cm.dump(traceback.format_exc(), log_name)
            continue

    return tuple(store_list)


def fetch_stores_old(data):
    url = data['url']
    param = {'storecountry': data['country_code']}
    try:
        body = cm.get_data(url, param)
    except Exception, e:
        cm.dump('Error in fetching stores: %s, %s' % (url, param), log_name)
        return ()
    m = re.search(ur'<div class="tommy-list stores">', body)
    if not m:
        cm.dump('Error in fetching stores: %s, %s' % (url, param), log_name)
        return ()
    sub = cm.extract_closure(body[m.start():], ur'<div\b', ur'</div>')[0]
    store_list = []
    for m in re.finditer(ur'<li id="([^"]+)"[^<>]+data-longitude="([^"]+)" data-latitude="([^"]+)"', sub):
        entry = cm.init_store_entry(data['brand_id'], data['brandname_e'], data['brandname_c'])
        store_id = m.group(1)
        if store_id in id_set:
            cm.dump('%s already exists.' % store_id, log_name)
            continue

        if store_id in store_map:
            continue
        else:
            store_map[store_id] = entry

        entry[cm.country_e] = data['country']
        store_sub = cm.extract_closure(sub[m.start():], ur'<li\b', ur'</li>')[0]
        try:
            entry[cm.lat] = string.atof(m.group(2))
        except (ValueError, KeyError, TypeError) as e:
            cm.dump('Error in fetching lat: %s' % str(e), log_name)
        try:
            entry[cm.lng] = string.atof(m.group(3))
        except (ValueError, KeyError, TypeError) as e:
            cm.dump('Error in fetching lng: %s' % str(e), log_name)

        m1 = re.search(ur'<h1>([^<>]+)</h1>', store_sub)
        entry[cm.name_e] = m1.group(1).strip() if m1 else ''

        m1 = re.search(ur'<dd class="location">([^<>]+)</dd>', store_sub)
        if m1:
            m2 = re.search(ur'(.*?)[,$]', m1.group(1))
            entry[cm.city_e] = cm.extract_city(m2.group(1))[0] if m2 else ''

        m1 = re.search(ur'<dd class="street">([^<>]+)</dd>', store_sub)
        entry[cm.addr_e] = cm.html2plain(m1.group(1)).strip() if m1 else ''

        m1 = re.search(ur'<dd class="phone">([^<>]+)</dd>', store_sub)
        entry[cm.tel] = m1.group(1).strip() if m1 else ''

        m1 = re.search(ur'<dd class="hours">(.+?)</dd>', store_sub, re.S)
        entry[cm.hours] = cm.reformat_addr(m1.group(1)) if m1 else ''

        m1 = re.search(ur'<dd class="products">(.+?)</dd>', store_sub, re.S)
        entry[cm.store_type] = cm.reformat_addr(m1.group(1)) if m1 else ''

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
        # db.insert_record(entry, 'stores')
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
        data = {'url': 'http://global.tommy.com/int/en/Stores/Find-A-Store',
                'brand_id': 10355, 'brandname_e': u'Tommy Hilfiger', 'brandname_c': u'汤米·希尔费格'}

    global db, id_set
    db = cm.StoresDb()
    db.connect_db(user=user, passwd=passwd)
    # db.execute(u'DELETE FROM %s WHERE brand_id=%d' % ('stores', data['brand_id']))
    rs = db.query_all(
        'SELECT native_id FROM stores WHERE brand_id=%d and not native_id=""' % data['brand_id'])
    id_set = [tmp[0] for tmp in rs]

    results = cm.walk_tree({'func': lambda data: func(data, 0), 'data': data})
    db.disconnect_db()
    cm.dump('Done!', log_name)

    return results


