# coding=utf-8
import json
import string
import re
import common as cm
import geosense as gs

__author__ = 'Zephyre'

db = None
log_name = 'swatch_log.txt'


def fetch_countries(data):
    results = []
    for uid in gs.country_map['data']:
        d = data.copy()
        d['country'] = gs.country_map['data'][uid]['name_e'].lower()
        results.append(d)
    return results


def fetch_stores(data):
    url = data['url']
    param = {'searchinput': data['country']}
    try:
        body = cm.post_data(url, param)
    except Exception, e:
        cm.dump('Error in fetching stores: %s, %s' % (url, param), log_name)
        return []

    store_list = []
    for sub in re.findall(ur'<li>(.+?)</li>', body, re.S):
        entry = cm.init_store_entry(data['brand_id'], data['brandname_e'], data['brandname_c'])
        entry[cm.country_e] = data['country'].upper()

        m1 = re.search(ur'<div class="StoreNumber">(.+?)</div>', sub)
        if m1 is not None:
            m2 = re.search(ur'title="([^"]+)"', m1.group(1))
            if m2 is not None:
                entry[cm.store_class] = m2.group(1).strip()
        start = sub.find(ur'<div class="storeInfo">')
        if start == -1:
            continue
        detail_sub = cm.extract_closure(sub[start:], ur'<div\b', ur'</div>')[0]
        m1 = re.search(ur'<h4>([^<>]+)</h4>', detail_sub)
        if m1 is not None:
            entry[cm.store_class] = m1.group(1).strip()
        m1 = re.search(ur'<address>(.+?)</address>', detail_sub, re.S)
        addr = m1.group(1).strip()
        m1 = re.search(ur'<span class="fn org">', addr)
        if m1 is not None:
            name_sub = cm.extract_closure(addr[m1.start():], ur'<span\b', ur'</span>')[0]
            entry[cm.name_e] = cm.reformat_addr(name_sub)
        addr_list = []
        for m1 in re.findall(ur'<span class="street-address">(.+?)</span>', addr):
            tmp = cm.reformat_addr(m1)
            if tmp != '':
                addr_list.append(tmp)
        m1 = re.search(ur'<span class="postal-code">(.+?)</span>', addr)
        if m1 is not None:
            zip_code = m1.group(1).strip()
            entry[cm.zip_code] = zip_code
        else:
            zip_code = None
        m1 = re.search(ur'<span class="locality">(.+?)</span>', addr)
        if m1 is not None:
            tmp = cm.html2plain(m1.group(1)).strip()
            entry[cm.city_e] = tmp.upper()
            if zip_code is None:
                addr_list.append(tmp)
            else:
                addr_list.append(' '.join((zip_code, tmp)))
        entry[cm.addr_e] = cm.reformat_addr(', '.join(addr_list))

        for m1 in re.findall(ur'<div class="tel">(.+?)</div>', detail_sub):
            m2 = re.search(ur'<span class="value">(.+?)</span>', m1)
            if m2 is not None:
                entry[cm.tel] = cm.html2plain(m2.group(1)).strip()
                continue
            m2 = re.search(ur'<span class="fax">(.+?)</span>', m1)
            if m2 is not None:
                entry[cm.fax] = cm.html2plain(m2.group(1)).strip()
                continue

        m1 = re.search(ur'<[^<>]*class="latitude"[^<>]*value="(-?\d+\.\d+)"', sub)
        if m1 is not None:
            entry[cm.lat] = string.atof(m1.group(1))
        m1 = re.search(ur'<[^<>]*class="longitude"[^<>]*value="(-?\d+\.\d+)"', sub)
        if m1 is not None:
            entry[cm.lng] = string.atof(m1.group(1))

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
        data = {'url': 'http://www.swatch.com/zz_en/storelocator/locator.html',
                'brand_id': 10340, 'brandname_e': u'Swatch', 'brandname_c': u'斯沃琪'}

    global db
    db = cm.StoresDb()
    db.connect_db(user=user, passwd=passwd)
    db.execute(u'DELETE FROM %s WHERE brand_id=%d' % ('stores', data['brand_id']))

    results = cm.walk_tree({'func': lambda data: func(data, 0), 'data': data})
    db.disconnect_db()

    return results
