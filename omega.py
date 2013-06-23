# coding=utf-8
import json
import string
import re
import common as cm
import geosense as gs

__author__ = 'Zephyre'

db = None
log_name = 'omega_log.txt'


def fetch_store_list(data):
    url = data['data_url']
    param = {'storelocator': 1, 'dofilter': 1, 'L': 0, 'map_sw': '-90.0%2C-180.0', 'map_ne': '90.0%2C180.0',
             'slst': 'c', 'storetype': 1}
    # storelocator=1&dofilter=1&L=0&map_sw=-90.0%2C-180.0&map_ne=90.0%2C180.0&slst=c&storetype=1
    try:
        body = cm.post_data(url, param)
    except Exception:
        cm.dump('Error in fetching stores: %s, %s' % (url, param), log_name)
        return []

    results = []
    for m in re.findall(ur'"id"\s*:\s*"([^"]+)"', body):
        tmp = m.split(',')
        for val in tmp:
            d = data.copy()
            d['store_id'] = string.atoi(val)
            results.append(d)

    return results


def fetch_store_details(data):
    url = '%s/%d' % (data['url'], data['store_id'])
    try:
        body = cm.get_data(url)
    except Exception:
        cm.dump('Error in fetching countries: %s' % url, log_name)
        return []

    entry = cm.init_store_entry(data['brand_id'], data['brandname_e'], data['brandname_c'])
    m = re.search(ur'<h1 class="with-back-option">\s*([^<>]+)\s*[<>]', body)
    if m is not None:
        entry[cm.name_e] = m.group(1).strip()

    start = body.find(ur'<div class="store-details">')
    if start != -1:
        sub = cm.extract_closure(body[start:], ur'<div\b', ur'</div>')[0]
        addr = cm.extract_closure(sub, ur'<p\b', ur'</p>')[0]
        m = re.search(ur'<span class="locality">([^<>]+?)</span>', addr)
        if m is not None:
            entry[cm.city_e] = m.group(1).split(',')[0].strip().upper()
        m = re.search(ur'<span class="postal-code">([^<>]+?)</span>', addr)
        if m is not None:
            entry[cm.zip_code] = m.group(1).strip()
        m = re.search(ur'<span class="country-name">([^<>]+?)</span>', addr)
        if m is not None:
            entry[cm.country_e] = m.group(1).strip().upper()
        entry[cm.addr_e] = cm.reformat_addr(addr)

    start = body.find(ur'<div class="contact">')
    if start != -1:
        sub = cm.extract_closure(body[start:], ur'<div\b', ur'</div>')[0]
        m = re.search(ur'<span class="tel">(.+?)</span>', sub)
        if m is not None:
            entry[cm.tel] = m.group(1).strip()
        m = re.search(ur'<span class="fax">(.+?)</span>', sub)
        if m is not None:
            entry[cm.fax] = m.group(1).strip()
        m = re.search(ur'<a href="mailto:([^"]+)">Email</a>', sub)
        if m is not None:
            entry[cm.email] = m.group(1).strip()

    start = body.find(ur'<h3>Opening hours</h3>')
    if start != -1:
        tmp = []
        sub = cm.extract_closure(body[start:], ur'<table>', ur'</table>')[0]
        for m in re.findall(ur'<t[hd][^<>]*>([^<>]+)</t[hd]>', sub):
            tmp.append(m)
        entry[cm.hours] = ' '.join(tmp)

    m = re.search(ur'LatLng\((-?\d+\.\d+)\s*,\s*(-?\d+\.\d+)\)', body)
    if m is not None:
        entry[cm.lat] = string.atof(m.group(1))
        entry[cm.lng] = string.atof(m.group(2))

    gs.field_sense(entry)
    ret = gs.addr_sense(entry[cm.addr_e], entry[cm.country_e])
    if ret[1] is not None:
        entry[cm.province_e] = ret[1]
        gs.field_sense(entry)

    cm.dump('(%s / %d) Found store: %s, %s (%s, %s)' % (data['brandname_e'], data['brand_id'],
                                                        entry[cm.name_e], entry[cm.addr_e], entry[cm.country_e],
                                                        entry[cm.continent_e]), log_name)
    db.insert_record(entry, 'stores')
    return [entry]


def fetch(level=1, data=None, user='root', passwd=''):
    def func(data, level):
        """
        :param data:
        :param level: 0：国家；1：城市；2：商店列表
        """
        if level == 0:
            # 国家列表
            return [{'func': lambda data: func(data, level + 1), 'data': s} for s in fetch_store_list(data)]
        if level == 1:
            # 商店
            return [{'func': None, 'data': s} for s in fetch_store_details(data)]
        else:
            return []

    # Walk from the root node, where level == 1.
    if data is None:
        data = {'data_url': 'http://www.omegawatches.com/index.php?eID=slmapmarkers&storeLocatorStoreDetailsPID=1008',
                'url': 'http://www.omegawatches.com/stores/search-on-map/store-details',
                'brand_id': 10288, 'brandname_e': u'Omega', 'brandname_c': u'欧米茄'}

    global db
    db = cm.StoresDb()
    db.connect_db(user=user, passwd=passwd)
    db.execute(u'DELETE FROM %s WHERE brand_id=%d' % ('stores', data['brand_id']))

    results = cm.walk_tree({'func': lambda data: func(data, 0), 'data': data})
    db.disconnect_db()

    return results