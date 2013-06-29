# coding=utf-8
import json
import string
import re
import common as cm
import geosense as gs

__author__ = 'Zephyre'

db = None
log_name = 'coach_log.txt'


def fetch_countries(data):
    url = data['url']
    try:
        body = cm.get_data(url)
    except Exception, e:
        cm.dump('Error in fetching countries: %s' % url, log_name)
        return ()

    states_set = set([])
    for m in re.findall(ur'<a href="[^"]+" onclick="setStateCode\(\'([A-Z]{2})\'\)', body, re.S):
        states_set.add(m)

    m = re.search(ur'<select id="countryCode"[^<>]*>(.+?)</select>', body, re.S)
    if m is None:
        cm.dump('Error in fetching countries: %s' % url, log_name)
        return ()
    sub = m.group(1)
    results = []
    country_set = set([])
    for m in re.findall(ur'<option\s+value="([A-Z]{2})"[^<>]*>([^<>]+)', sub):
        d = data.copy()
        code = m[0]
        if code in country_set:
            continue
        else:
            country_set.add(code)
        d['country_code'] = code
        d['states_set'] = states_set if code == 'US' else None
        d['country'] = cm.html2plain(m[1]).strip().upper()
        # if code in ('CN','CA','US'):
        results.append(d)
    return tuple(results)


def fetch_stores(data):
    url = data['data_url']
    param = {'storeId': 10551, 'catalogId': 10051, 'countryTab': 'in', 'countryCode': data['country_code']}
    if data['country_code'] == 'US':
        param['radius'] = 20
        param['state'] = data['state_code']
    try:
        body = cm.get_data(url, param)
    except Exception, e:
        cm.dump('Error in fetching stores: %s, %s' % (url, param), log_name)
        return ()

    store_list = []
    for m in re.finditer(ur'<div class="vcard"\s*>', body):
        entry = cm.init_store_entry(data['brand_id'], data['brandname_e'], data['brandname_c'])
        entry[cm.country_e] = data['country_code']
        sub = cm.extract_closure(body[m.start():], ur'<div\b', ur'</div>')[0]

        m1 = re.search(ur'<div class="fn org">(.+?)</div>', sub, re.S)
        entry[cm.name_e] = cm.reformat_addr(m1.group(1)) if m1 else ''

        m1 = re.search(ur'<div class="adr">', sub, re.S)
        if m1:
            addr_sub = cm.extract_closure(sub[m1.start():], ur'<div\b', ur'</div>')[0]
            entry[cm.addr_e] = cm.reformat_addr(addr_sub)
            m2 = re.search(ur'<span class="locality">([^<>?]+?),*\s*</span>', addr_sub)
            city = cm.html2plain(m2.group(1)).strip().upper() if m2 else ''
            entry[cm.city_e] = city if city != ',' else ''
            m2 = re.search(ur'<span\s+class="region"\s+title="([^"]+)"[^<>]*>', addr_sub)
            entry[cm.province_e] = cm.html2plain(m2.group(1)).strip().upper() if m2 else ''
            m2 = re.search(ur'<span\s+class="postal-code"[^<>]*>([^<>]+)', addr_sub)
            entry[cm.zip_code] = m2.group(1).strip() if m2 else ''

        m1 = re.search(ur'<div class="tel">([^<>]+)</div>', sub, re.S)
        entry[cm.tel] = m1.group(1).strip() if m1 else ''

        m1 = re.search(ur'<div class="hours_wrapper">', sub)
        if m1:
            hours_sub = cm.extract_closure(sub[m1.start():], ur'<div\b', ur'</div>')[0]
            hours_list = []
            for term in (tmp.strip() for tmp in cm.reformat_addr(hours_sub).split(',')):
                term = re.sub(re.compile(ur'store hours\s*:', re.I), '', term).strip()
                if term != '':
                    hours_list.append(term)
            entry[cm.hours] = ', '.join(hours_list)

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


def fetch_states(data):
    if data['country_code'] != 'US':
        d = data.copy()
        d['state_code'] = None
        return [d]

    results = []
    for state in data['states_set']:
        d = data.copy()
        d['state_code'] = state
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
            # 州列表
            return [{'func': lambda data: func(data, level + 1), 'data': s} for s in fetch_states(data)]
        if level == 2:
            # 商店
            return [{'func': None, 'data': s} for s in fetch_stores(data)]
        else:
            return ()

    # Walk from the root node, where level == 1.
    if data is None:
        data = {'data_url': 'http://www.coach.com/online/handbags/COAStoreLocatorWSCmd',
                'url': 'http://www.coach.com/online/handbags/StoreLocatorGMView?storeId=10551&catalogId=10051&LOC=BN',
                'brand_id': 10093, 'brandname_e': u'Coach', 'brandname_c': u'蔻驰'}

    global db
    db = cm.StoresDb()
    db.connect_db(user=user, passwd=passwd)
    db.execute(u'DELETE FROM %s WHERE brand_id=%d' % ('stores', data['brand_id']))

    results = cm.walk_tree({'func': lambda data: func(data, 0), 'data': data})
    db.disconnect_db()
    cm.dump('Done!', log_name)

    return results


