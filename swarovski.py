# coding=utf-8
import json
import logging
import logging.config
import string
import re
import common as cm
import geosense as gs
from pyquery import PyQuery as pq

__author__ = 'Zephyre'


def fetch_countries(data, logger):
    url = data['url']
    try:
        body, data['cookie'] = cm.get_data_cookie(url)
        q = pq(body)
    except Exception as e:
        logger.error(unicode.format(u'Error in fetching countries: {0}', url))
        return ()

    results = []
    for item in q('#bfselect-country option[value!=""]'):
        d = data.copy()
        d['country_code'] = item.attrib['value']
        temp = item.text.strip().upper()
        d['country'] = temp.decode('utf-8') if isinstance(temp, str) else temp
        results.append(d)
    return tuple(results)


def fetch_store_details(db, data, logger):
    url = data['url']
    try:
        body, data['cookie'] = cm.get_data_cookie(url, cookie=data['cookie'])
        q = pq(body)
    except Exception, e:
        # cm.dump('Error in fetching stores: %s' % url, log_name)
        return ()

    m = re.search(ur'<div class="col">\s*<h3>Boutique</h3>\s*<div class="content">(.+?)</div>', body, re.S)
    if not m:
        return ()
    sub = m.group(1)

    entry = cm.init_store_entry(data['brand_id'], data['brandname_e'], data['brandname_c'])
    entry[cm.country_e] = data['country_code']
    if entry[cm.country_e] == 'US':
        tmp_list = tuple(tmp.strip() for tmp in cm.reformat_addr(data['city']).strip(','))
        if len(tmp_list) == 2:
            if re.search('[A-Z]{2}', tmp_list[1]) or tmp_list[1] == 'D.C.':
                entry[cm.province_e] = tmp_list[1]
    entry[cm.city_e] = cm.extract_city(data['city'])[0]
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

    logger.info('(%s / %d) Found store: %s, %s (%s, %s)' % (data['brandname_e'], data['brand_id'],
                                                            entry[cm.name_e], entry[cm.addr_e], entry[cm.country_e],
                                                            entry[cm.continent_e]))
    cm.insert_record(db, entry, 'spider_stores.stores')

    return (entry,)


def fetch_cities(data, logger):
    url = data['url']
    param = {'IsFooterForm': 'true', 'CurrentCountryID': data['country_code']}
    if data['state_code']:
        param['CurrentRegionID'] = data['state_code']
    try:
        body, data['cookie'] = cm.get_data_cookie(url, param, cookie=data['cookie'])
        q = pq(body)
    except Exception, e:
        # cm.dump('Error in fetching cities: %s, %s' % (url, param), log_name)
        return ()

    results = []
    for item in q('#bfselect-city-footer option[value!=""]'):
        city_code = item.attrib['value'].strip().lower()
        if city_code == 'all':
            continue
        d = data.copy()
        d['city_code'] = city_code
        temp = item.text.strip().upper()
        d['city'] = temp.decode('utf-8') if isinstance(temp, str) else temp
        results.append(d)
    return tuple(results)


def fetch_states(data, logger):
    url = data['url']
    param = {'IsFooterForm': 'true', 'CurrentCountryID': data['country_code']}
    try:
        body, data['cookie'] = cm.get_data_cookie(url, param, cookie=data['cookie'])
        q = pq(body)
    except Exception, e:
        # cm.dump('Error in fetching states: %s, %s' % (url, param), log_name)
        return ()

    results = []
    for item in q('#bfselect-region-footer option[value!=""]'):
        d = data.copy()
        d['state_code'] = item.attrib['value']
        temp = item.text.strip().upper()
        d['state'] = temp.decode('utf-8') if isinstance(temp, str) else temp
        results.append(d)
    if len(results) == 0:
        d = data.copy()
        d['state_code'] = None
        return d,
    return tuple(results)


def fetch_store_list(data, logger):
    url = data['store_url']
    param = {'CurrentCountryID': data['country_code'], 'CurrentCityID': ('    %s' % data['city_code'])[-5:]}
    if data['state_code']:
        param['CurrentRegionID'] = data['state_code']
    try:
        body, data['cookie'] = cm.get_data_cookie(url, param, cookie=data['cookie'])
        q = pq(body)
    except Exception, e:
        # cm.dump('Error in fetching store list: %s, %s' % (url, param), log_name)
        return ()

    def func(content):
        for store in content('tr td.col-desc a[href].dotted'):
            d = data.copy()
            d['url'] = store.attrib['href']
            results.append(d)

    pages = (temp.attrib['href'] for temp in q('div.paging li a[href]'))
    results = []
    func(q)
    for p in pages:
        try:
            body, data['cookie'] = cm.get_data_cookie(p, param, cookie=data['cookie'])
            q = pq(body)
        except Exception, e:
            # cm.dump('Error in fetching store list: %s, %s' % (url, param), log_name)
            return ()
        func(q)

    return tuple(results)

    # page_idx = -1
    # while True:
    #     for m in re.findall(ur'<!--\s*Result Row Start\s*-->(.+?)<!--\s*Result Rown End\s*-->', body, re.S):
    #         m1 = re.search(ur'<td class="col-desc"\s*>\s*<a href="([^"]+)"', m, re.S)
    #         if m1:
    #             d = data.copy()
    #             d['url'] = m1.group(1)
    #             results.append(d)
    #
    #     page_idx += 1
    #     if page_idx >= len(pages):
    #         break
    #
    #     try:
    #         body, data['cookie'] = cm.get_data(url, cookie=data['cookie'])
    #     except Exception, e:
    #         pass
    #         # cm.dump('Error in fetching store list for page: %s' % url, log_name)
    # return tuple(results)
    #
    #
    # m = re.search(ur'<div class="paging"\s*>(.+?)</div>', body, re.S)
    # if m:
    #     pages = re.findall(ur'<li>\s*<a href="([^"]+)"', m.group(1), re.S)
    # else:
    #     pages = []
    #
    # results = []
    # page_idx = -1
    # while True:
    #     for m in re.findall(ur'<!--\s*Result Row Start\s*-->(.+?)<!--\s*Result Rown End\s*-->', body, re.S):
    #         m1 = re.search(ur'<td class="col-desc"\s*>\s*<a href="([^"]+)"', m, re.S)
    #         if m1:
    #             d = data.copy()
    #             d['url'] = m1.group(1)
    #             results.append(d)
    #
    #     page_idx += 1
    #     if page_idx >= len(pages):
    #         break
    #
    #     try:
    #         body, data['cookie'] = cm.get_data(url, cookie=data['cookie'])
    #     except Exception, e:
    #         pass
    #         # cm.dump('Error in fetching store list for page: %s' % url, log_name)
    # return tuple(results)


def fetch(db, data=None, user='root', passwd=''):
    logging.config.fileConfig('swarovski.cfg')
    logger = logging.getLogger('firenzeLogger')

    def func(data, level):
        """
        :param data:
        :param level: 0：国家；1：城市；2：商店列表
        """
        if level == 0:
            # 国家列表
            return [{'func': lambda data: func(data, level + 1), 'data': s} for s in fetch_countries(data, logger)]
        if level == 1:
            # 州列表
            return [{'func': lambda data: func(data, level + 1), 'data': s} for s in fetch_states(data, logger)]
        if level == 2:
            # 城市列表
            return [{'func': lambda data: func(data, level + 1), 'data': s} for s in fetch_cities(data, logger)]
        if level == 3:
            # 城市列表
            return [{'func': lambda data: func(data, level + 1), 'data': s} for s in fetch_store_list(data, logger)]
        if level == 4:
            # 商店
            return [{'func': None, 'data': s} for s in fetch_store_details(db, data, logger)]
        else:
            return ()

    # Walk from the root node, where level == 1.
    if data is None:
        data = {
            'url': 'http://www.swarovski.com.cn/is-bin/INTERSHOP.enfinity/WFS/SCO-Web_CN-Site/en_US/-/CNY/SMOD_Storefinder-ViewStorefinderSelects',
            'store_url': 'http://www.swarovski.com.cn/Web_CN/en/boutique_search',
            'brand_id': 10339, 'brandname_e': u'Swarovski', 'brandname_c': u'施华洛世奇'}

    db.query(str.format('DELETE FROM spider_stores.stores WHERE brand_id={0}', data['brand_id']))
    results = cm.walk_tree({'func': lambda data: func(data, 0), 'data': data})
    logger.info('Done')

    return results


