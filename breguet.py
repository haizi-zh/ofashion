# coding=utf-8
import json
import string
import re
import common as cm
import geosense as gs

__author__ = 'Zephyre'

db = None
log_name = 'breguet_log.txt'


def fetch_countries(data):
    url = data['url']
    try:
        body = cm.get_data(url)
    except Exception, e:
        cm.dump('Error in fetching countries: %s' % url, log_name)
        return ()

    m = re.search(ur'<select id="collection" name="\(country\)"[^<>]*>(.+?)</select>', body, re.S)
    if not m:
        cm.dump('Error in fetching countries: %s' % url, log_name)
        return ()
    sub = m.group(1)
    results = []
    for m in re.findall(ur'<option value="([A-Z]{2})">([^<>]+)', sub):
        d = data.copy()
        d['country_code'] = m[0]
        d['country'] = cm.html2plain(m[1]).strip().upper()
        results.append(d)
    return tuple(results)


def fetch_stores(data):
    url = data['data_url']
    param = {'DestinationURL': 'Worldwide-retailers', '(country)': data['country_code'], '(city)': data['city_id']}
    try:
        body = cm.post_data(url, param)
        # m = re.search(ur'META HTTP-EQUIV="Location"\s+Content="([^"]+)"', body)
        # if not m:
        #     raise IOError()
        # body = cm.get_data(m.group(1))
    except Exception, e:
        cm.dump('Error in fetching stores: %s, %s' % (url, param), log_name)
        return ()

    store_list = []
    for s in re.findall(ur'<td width="211">(.+?)</td>', body, re.S):
        entry = cm.init_store_entry(data['brand_id'], data['brandname_e'], data['brandname_c'])
        entry[cm.country_e] = data['country_code']
        entry[cm.city_e] = cm.extract_city(data['city'])[0]

        addr_list = []
        pat_tel = re.compile(ur'phone\s*[:\.]?\s*', re.I)
        pat_fax = re.compile(ur'fax\s*[:\.]?\s*', re.I)
        pat_email = re.compile(ur'email\s*[:\.]?\s*', re.I)
        for term in (tmp.strip() for tmp in cm.reformat_addr(s).split(',')):
            if term == '':
                continue
            elif re.search(pat_tel, term):
                entry[cm.tel] = re.sub(pat_tel, '', term).strip()
            elif re.search(pat_fax, term):
                entry[cm.fax] = re.sub(pat_fax, '', term).strip()
            elif re.search(pat_email, term):
                entry[cm.email] = re.sub(pat_email, '', term).strip()
            else:
                addr_list.append(term)
        entry[cm.name_e] = addr_list[0]
        entry[cm.addr_e] = ', '.join(addr_list[1:])

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
    url = data['data_url']
    param = {'DestinationURL': 'Worldwide-retailers', '(country)': data['country_code']}
    try:
        body = cm.post_data(url, param)
        # m = re.search(ur'META HTTP-EQUIV="Location"\s+Content="([^"]+)"', body)
        # if not m:
        #     raise IOError()
        # body = cm.get_data(m.group(1))
    except Exception, e:
        cm.dump('Error in fetching cities: %s, %s' % (url, param), log_name)
        return ()

    m = re.search(ur'<select id="collection" name="\(city\)"[^<>]*>(.+?)</select>', body, re.S)
    if not m:
        cm.dump('Error in fetching cities: %s' % url, log_name)
        return ()
    sub = m.group(1)
    results = []
    for m in re.findall(ur'<option value="(\d+)">([^<>]+)', sub):
        d = data.copy()
        d['city_id'] = m[0]
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
        data = {'data_url': 'http://www.breguet.com/en/content/action',
                'url': 'http://www.breguet.com/en/Worldwide-retailers',
                'brand_id': 10053, 'brandname_e': u'Breguet', 'brandname_c': u'宝玑'}

    global db
    db = cm.StoresDb()
    db.connect_db(user=user, passwd=passwd)
    db.execute(u'DELETE FROM %s WHERE brand_id=%d' % ('stores', data['brand_id']))

    results = cm.walk_tree({'func': lambda data: func(data, 0), 'data': data})
    db.disconnect_db()
    cm.dump('Done!', log_name)

    return results


