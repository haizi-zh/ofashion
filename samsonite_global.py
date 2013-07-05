# coding=utf-8
import json
import string
import re
import common as cm
import geosense as gs
import xml.etree.ElementTree as et

__author__ = 'Zephyre'

db = None
log_name = 'samsonite_log.txt'


def fetch_countries(data):
    url = data['url']
    try:
        body = cm.get_data(url)
    except Exception, e:
        cm.dump('Error in fetching countries: %s' % url, log_name)
        return ()
    m = re.search(ur'<select id="drp_storelocator_country"[^<>]*>(.+?)</select>', body, re.S)
    if not m:
        cm.dump('Error in fetching countries: %s' % url, log_name)
        return ()
    sub = m.group(1)
    results = []
    for m in re.findall(ur'<option value="(\d+)">([^<>\(\)]+)', sub):
        d = data.copy()
        d['country_id'] = string.atoi(m[0])
        d['country'] = cm.html2plain(m[1]).strip().upper()
        results.append(d)
    return tuple(results)


def fetch_stores(data):
    url = data['store_url']
    param = {'s': 'SAMS', 'cid': data['country_id'], 'ctid': data['city_id'], 'search': data['search_type']}
    try:
        body = cm.get_data(url, param)
    except Exception, e:
        cm.dump('Error in fetching stores: %s, %s' % (url, param), log_name)
        return ()

    store_list = []
    tree = et.fromstring(body.encode('utf-8'))
    for s in tree.getchildren():
        entry = cm.init_store_entry(data['brand_id'], data['brandname_e'], data['brandname_c'])
        entry[cm.country_e] = data['country']
        entry[cm.city_e] = cm.extract_city(data['city'])[0]
        entry[cm.store_type] = {'dealer': 'Stockist', 'repaircenter': 'Repair Centre'}[data['search_type']]

        val = s.getiterator('fld_Deal_State')[0].text
        # entry[cm.province_e] = cm.html2plain(val).strip().upper() if val else ''

        val = s.getiterator('fld_Deal_StoreName')[0].text
        entry[cm.name_e] = cm.html2plain(val).strip() if val else ''

        addr_list = []
        for key in ('fld_Deal_Address1', 'fld_Deal_Address2', 'fld_Deal_Address3'):
            val = s.getiterator(key)[0].text
            if val:
                val = cm.html2plain(val).strip()
                if val != '':
                    addr_list.append(val)
        entry[cm.addr_e] = ', '.join(addr_list)

        val = s.getiterator('fld_Deal_Zip')[0].text
        entry[cm.zip_code] = val.strip() if val else ''

        val = s.getiterator('fld_Deal_Prefix')[0].text
        prefix = val.strip() if val else ''

        val = s.getiterator('fld_Deal_Phone')[0].text
        entry[cm.tel] = ('%s %s' % (prefix, val.strip()) if val else '').strip()

        val = s.getiterator('fld_Deal_Fax')[0].text
        entry[cm.fax] = ('%s %s' % (prefix, val.strip()) if val else '').strip()

        val = s.getiterator('fld_Deal_Email')[0].text
        entry[cm.email] = val.strip() if val else ''

        val = s.getiterator('fld_Deal_Website')[0].text
        entry[cm.url] = val.strip() if val else ''

        val = s.getiterator('fld_Deal_Coordinates')[0].text
        m = re.search(ur'(-?\d+\.\d+)\s*,\s*(-?\d+\.\d+)', val if val else '')
        if m:
            entry[cm.lat] = string.atof(m.group(1))
            entry[cm.lng] = string.atof(m.group(2))

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


def fetch_cities(data):
    url = data['url']
    param = {'ref': 'com', 'fld_Coun_Id': data['country_id'], 'search': data['search_type']}
    try:
        body = cm.post_data(url, param)
    except Exception, e:
        cm.dump('Error in fetching cities: %s, %s' % (url, param), log_name)
        return ()
    m = re.search(ur'<select id="drp_storelocator_city"[^<>]*>(.+?)</select>', body, re.S)
    if not m:
        cm.dump('Error in fetching cities: %s, %s' % (url, param), log_name)
        return ()
    sub = m.group(1)
    results = []
    for m in re.findall(ur'<option value="(\d+)">([^<>\(\)]+)', sub):
        d = data.copy()
        d['city_id'] = string.atoi(m[0])
        d['city'] = cm.html2plain(m[1]).strip().upper()
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
            # 城市列表
            return [{'func': lambda data: func(data, level + 1), 'data': s} for s in fetch_cities(data)]
        if level == 2:
            # 商店
            return [{'func': None, 'data': s} for s in fetch_stores(data)]
        else:
            return ()

    # Walk from the root node, where level == 1.
    if data is None:
        data = {'store_url': 'http://www.samsonite.co.uk/data-exchange/getDealerLocatorMapV2.aspx',
                'url': 'http://www.samsonite.co.uk/en/store-locator.htm',
                'brand_id': 10309, 'brandname_e': u'Samsonite', 'brandname_c': u'新秀丽', 'search_type': 'dealer'}

    global db
    db = cm.StoresDb()
    db.connect_db(user=user, passwd=passwd)
    # db.execute(u'DELETE FROM %s WHERE brand_id=%d' % ('stores', data['brand_id']))

    results = cm.walk_tree({'func': lambda data: func(data, 0), 'data': data})

    data['search_type'] = 'repaircenter'
    results.extend(cm.walk_tree({'func': lambda data: func(data, 0), 'data': data}))

    db.disconnect_db()
    cm.dump('Done!', log_name)

    return results


