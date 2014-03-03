# coding=utf-8
import string
import re
from stores import geosense as gs

__author__ = 'Zephyre'

db = None
log_name = 'movado_log.txt'
store_map = {}
city_cnt = 0


def fetch_stores(data):
    global city_cnt
    city_cnt += 1
    cm.dump('Processing city #%d: %s, %s' % (city_cnt, data['city'], data['country']))

    url = data['url']
    param = {'division': '06', 'retailer': 1, 'address': ', '.join((data['city'], data['country']))}
    try:
        body = cm.get_data(url, param)
    except Exception, e:
        cm.dump('Error in fetching stores: %s, %s' % (url, param), log_name)
        return ()

    start = body.find(ur'<div id="Layer2">')
    if start == -1:
        cm.dump('Error in fetching stores: %s, %s' % (url, param), log_name)
        return ()
    sub = cm.extract_closure(body[start:], ur'<div\b', ur'</div>')[0]

    store_list = []
    for m in re.finditer(ur'<tr class="altrow\d+">', sub):
        entry = cm.init_store_entry(data['brand_id'], data['brandname_e'], data['brandname_c'])
        # entry[cm.country_e] = data['country']
        # entry[cm.city_e] = data['city']

        store_sub = cm.extract_closure(sub[m.start():], ur'<tr\b', ur'</tr')[0]
        pat = re.compile(ur'<strong>([^<>]+)</strong>')
        m1 = re.search(pat, store_sub)
        if m1 is not None:
            entry[cm.name_e] = cm.html2plain(m1.group(1)).strip()
            store_sub = re.sub(pat, '', store_sub)
        start = store_sub.find(ur'<table>')
        if start != -1:
            addr_list = []
            pat_tel = re.compile(ur'phone\s*[:\.]', re.I)
            pat_fax = re.compile(ur'fax\s*[:\.]', re.I)
            for term in [tmp.strip() for tmp in cm.reformat_addr(store_sub[:start]).split(',')]:
                if term == '':
                    continue
                elif re.search(pat_tel, term):
                    entry[cm.tel] = re.sub(pat_tel, '', term).strip()
                elif re.search(pat_fax, term):
                    entry[cm.fax] = re.sub(pat_fax, '', term).strip()
                else:
                    addr_list.append(term)
            entry[cm.addr_e] = ', '.join(addr_list)

        m1 = re.search(ur'href="([^"]+id=\d+)"', store_sub)
        if m1 is None or m1.group(1) in store_map:
            continue

        store_map[m1.group(1)] = entry
        start = body.find(m1.group(1))
        if start != -1:
            idx = body[:start].rfind(ur'addMarker')
            if idx != -1:
                m2 = re.search(ur'(-?\d+\.\d+)\s*,\s*(-?\d+\.\d+)', body[idx:])
                if m2 is not None:
                    entry[cm.lat] = string.atof(m2.group(1))
                    entry[cm.lng] = string.atof(m2.group(2))

        gs.field_sense(entry)
        ret = gs.addr_sense(entry[cm.addr_e])
        if ret[0] is not None and entry[cm.country_e] == '':
            entry[cm.country_e] = ret[0]
        if ret[1] is not None and entry[cm.province_e] == '':
            entry[cm.province_e] = ret[1]
        if ret[2] is not None and entry[cm.city_e] == '':
            entry[cm.city_e] = ret[2]
        gs.field_sense(entry)

        if entry[cm.city_e] == '' or entry[cm.country_e] == '':
            ret = None
            if entry[cm.lat] != '' and entry[cm.lng] != '':
                ret = gs.geocode(latlng='%f,%f' % (entry[cm.lat], entry[cm.lng]))
            if ret is None:
                ret = gs.geocode(', '.join((entry[cm.addr_e], data['zone'])))
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

        entry[cm.city_e] = cm.extract_city(entry[cm.city_e])[0]
        cm.dump('(%s / %d) Found store: %s, %s (%s, %s)' % (data['brandname_e'], data['brand_id'],
                                                            entry[cm.name_e], entry[cm.addr_e], entry[cm.country_e],
                                                            entry[cm.continent_e]), log_name)
        db.insert_record(entry, 'stores')
        store_list.append(entry)

    return tuple(store_list)


def get_func_chain():
    return fetch_cities, fetch_stores


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
            return ()

    # Walk from the root node, where level == 1.
    if data is None:
        data = {'url': 'http://movado.com.gotlocations.com/index.php',
                'brand_id': 10269, 'brandname_e': u'Movado', 'brandname_c': u'摩凡陀'}

    global db
    db = cm.StoresDb()
    db.connect_db(user=user, passwd=passwd)
    db.execute(u'DELETE FROM %s WHERE brand_id=%d' % ('stores', data['brand_id']))

    results = cm.walk_tree({'func': lambda data: func(data, 0), 'data': data})
    db.disconnect_db()
    cm.dump('Done!', log_name)

    return results


