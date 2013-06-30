# coding=utf-8
import json
import string
import re
import common as cm
import geosense as gs

__author__ = 'Zephyre'

db = None
log_name = 'swarovski_log.txt'


def fetch_countries(data):
    url = data['url']
    try:
        body, data['cookie'] = cm.get_data_cookie(url)
    except Exception, e:
        cm.dump('Error in fetching countries: %s' % url, log_name)
        return ()

    m = re.search(ur'<select id="bfselect-country" name="CurrentCountryID"[^<>]+>(.+?)</select>', body, re.S)
    if not m:
        cm.dump('Error in fetching countries: %s' % url, log_name)
        return ()
    sub = m.group(1).strip()
    results = []
    for m in re.findall(ur'<option value="([A-Z]{2})"', sub):
        d = data.copy()
        d['country_code'] = m
        results.append(d)
    return tuple(results)


def fetch_store_details(data):
    url = data['url']
    try:
        body, data['cookie'] = cm.get_data_cookie(url, cookie=data['cookie'])
    except Exception, e:
        cm.dump('Error in fetching stores: %s' % url, log_name)
        return ()

    m = re.search(ur'<div class="col">\s*<h3>Boutique</h3>\s*<div class="content">(.+?)</div>', body, re.S)
    if not m:
        return ()
    sub = m.group(1)

    entry = cm.init_store_entry(data['brand_id'], data['brandname_e'], data['brandname_c'])
    entry[cm.country_e] = data['country_code']
    entry[cm.city_e] = data['city']
    entry[cm.province_e] = data['state'] if data['state_code'] else ''

    sub_list = re.findall(ur'<p>(.+?)</p>', m.group(1), re.S)
    if len(sub_list) < 2:
        return ()
    title_list = tuple(tmp.strip() for tmp in cm.reformat_addr(sub_list[0]).split(','))
    entry[cm.name_e] = title_list[0]
    if len(title_list) > 1:
        entry[cm.store_class] = title_list[1]

    entry[cm.addr_e] = cm.reformat_addr(sub_list[1])

    if len(sub_list) > 2:
        pat_tel = re.compile(ur'(telephone|tel|phone)\s*[\.: ]\s*', re.I)
        pat_fax = re.compile(ur'fax\s*[\.: ]\s*', re.I)
        pat_email = re.compile(ur'email\s*[\.: ]\s*', re.I)
        for term in (tmp.strip() for tmp in cm.reformat_addr(sub_list[2]).split(',')):
            if re.search(pat_tel, term):
                entry[cm.tel] = re.sub(pat_tel, '', term)
            elif re.search(pat_fax, term):
                entry[cm.fax] = re.sub(pat_fax, '', term)
            elif re.search(pat_email, term):
                entry[cm.email] = re.sub(pat_email, '', term)

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

    return (entry,)


def fetch_cities(data):
    url = data['url']
    param = {'IsFooterForm': 'true', 'CurrentCountryID': data['country_code']}
    if data['state_code']:
        param['CurrentRegionID'] = data['state_code']
    try:
        body, data['cookie'] = cm.get_data_cookie(url, param, cookie=data['cookie'])
    except Exception, e:
        cm.dump('Error in fetching cities: %s, %s' % (url, param), log_name)
        return ()

    m = re.search(ur'<select id="bfselect-city-footer" name="CurrentCityID"[^<>]+>(.+?)</select>', body, re.S)
    if not m:
        cm.dump('Error in fetching cities: %s' % url, log_name)
        return ()
    sub = m.group(1).strip()
    results = []
    for m in re.findall(ur'<option value="([^"]+)"\s*>([^<>]+)', sub):
        if m[0].strip().lower() == 'all':
            continue
        d = data.copy()
        d['city_code'] = m[0]
        d['city'] = cm.html2plain(m[1]).strip().upper()
        results.append(d)
    return tuple(results)


def fetch_states(data):
    url = data['url']
    param = {'IsFooterForm': 'true', 'CurrentCountryID': data['country_code']}
    try:
        body, data['cookie'] = cm.get_data_cookie(url, param, cookie=data['cookie'])
    except Exception, e:
        cm.dump('Error in fetching states: %s, %s' % (url, param), log_name)
        return ()

    m = re.search(ur'<select id="bfselect-region-footer" name="CurrentRegionID"[^<>]+>(.+?)</select>', body, re.S)
    if not m:
        cm.dump('Error in fetching states: %s' % url, log_name)
        return ()
    sub = m.group(1).strip()
    results = []
    for m in re.findall(ur'<option value="([^"]+)"\s*>([^<>]+)', sub):
        d = data.copy()
        d['state_code'] = m[0].strip()
        d['state'] = cm.html2plain(m[1]).strip().upper()
        results.append(d)
    if len(results) == 0:
        d = data.copy()
        d['state_code'] = None
        return (d,)
    return tuple(results)


def fetch_store_list(data):
    url = data['store_url']
    param = {'CurrentCountryID': data['country_code'], 'CurrentCityID': ('    %s' % data['city_code'])[-5:]}
    if data['state_code']:
        param['CurrentRegionID'] = data['state_code']
    try:
        body, data['cookie'] = cm.get_data_cookie(url, param, cookie=data['cookie'])
    except Exception, e:
        cm.dump('Error in fetching store list: %s, %s' % (url, param), log_name)
        return ()

    m = re.search(ur'<div class="paging"\s*>(.+?)</div>', body, re.S)
    if m:
        pages = re.findall(ur'<li>\s*<a href="([^"]+)"', m.group(1), re.S)
    else:
        pages = []

    results = []
    page_idx = -1
    while True:
        for m in re.findall(ur'<!--\s*Result Row Start\s*-->(.+?)<!--\s*Result Rown End\s*-->', body, re.S):
            m1 = re.search(ur'<td class="col-desc"\s*>\s*<a href="([^"]+)"', m, re.S)
            if m1:
                d = data.copy()
                d['url'] = m1.group(1)
                results.append(d)

        page_idx += 1
        if page_idx >= len(pages):
            break

        try:
            body, data['cookie'] = cm.get_data(url, cookie=data['cookie'])
        except Exception, e:
            cm.dump('Error in fetching store list for page: %s' % url, log_name)
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
            # 城市列表
            return [{'func': lambda data: func(data, level + 1), 'data': s} for s in fetch_cities(data)]
        if level == 3:
            # 城市列表
            return [{'func': lambda data: func(data, level + 1), 'data': s} for s in fetch_store_list(data)]
        if level == 4:
            # 商店
            return [{'func': None, 'data': s} for s in fetch_store_details(data)]
        else:
            return ()

    # Walk from the root node, where level == 1.
    if data is None:
        data = {
            'url': 'http://www.swarovski.com.cn/is-bin/INTERSHOP.enfinity/WFS/SCO-Web_CN-Site/en_US/-/CNY/SMOD_Storefinder-ViewStorefinderSelects',
            'store_url': 'http://www.swarovski.com.cn/Web_CN/en/boutique_search',
            'brand_id': 10339, 'brandname_e': u'Swarovski', 'brandname_c': u'施华洛世奇'}

    global db
    db = cm.StoresDb()
    db.connect_db(user=user, passwd=passwd)
    db.execute(u'DELETE FROM %s WHERE brand_id=%d' % ('stores', data['brand_id']))

    results = cm.walk_tree({'func': lambda data: func(data, 0), 'data': data})
    db.disconnect_db()
    cm.dump('Done!', log_name)

    return results


