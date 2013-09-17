# coding=utf-8
import json
import logging
import string
import re
import common as cm
import geosense as gs
import logging.config

__author__ = 'Zephyre'


def fetch_countries(data):
    url = data['host'] + 'boutique'
    try:
        body = cm.get_data(url)
    except Exception, e:
        # cm.dump('Error in fetching countries: %s' % url, log_name)
        return []

    start = body.find(ur'<list id="country_list">')
    if start == -1:
        # cm.dump('Error in fetching countries: %s' % url, log_name)
        return []
    body = cm.extract_closure(body[start:], ur'<list\b', ur'</list>')[0]
    results = []
    for m in re.finditer(ur'<object type="[^"]+" id="([^"]+)" value="(\d+)"\s*>(.+?)<\s*/\s*object\s*>', body, re.S):
        d = data.copy()
        d['country_id'] = string.atoi(m.group(2))
        d['country_alias'] = m.group(1).strip()
        m1 = re.search(ur'<p [^<>]*>(.+?)</p', m.group(3))
        if m1 is None:
            continue
        d['country_name'] = cm.html2plain(m1.group(1)).strip().upper()
        m1 = re.search(ur'<link target="_self"><!\[CDATA\[([^\[\]]+)\]\]></link>', m.group(3))
        if m1 is None:
            continue
        d['url'] = m1.group(1).strip()
        results.append(d)
    return results


def fetch_cities(data):
    url = data['host'] + data['url']
    try:
        body = cm.get_data(url)
    except Exception, e:
        # cm.dump('Error in fetching cities: %s' % url, log_name)
        return []

    states_map = {}
    start = body.find(ur'<list id="prefecture_list">')
    if start != -1:
        state_sub = cm.extract_closure(body[start:], ur'<list\b', ur'</list>')[0]
        for m in re.finditer(ur'<object type="[^"]+" id="([^"]+)" value="(\d+)"\s*>(.+?)<\s*/\s*object\s*>',
                             state_sub, re.S):
            item = {}
            item['state_id'] = string.atoi(m.group(2))
            item['state_alias'] = m.group(1).strip()
            m1 = re.search(ur'<p [^<>]*>(.+?)</p', m.group(3))
            if m1 is None:
                continue
            item['state_name'] = cm.html2plain(m1.group(1)).strip().upper()
            states_map[item['state_id']] = item

    start = body.find(ur'<list id="city_list">')
    if start == -1:
        # cm.dump('Error in fetching cities: %s' % url, log_name)
        return []
    city_sub = cm.extract_closure(body[start:], ur'<list\b', ur'</list>')[0]
    results = []
    for m in re.finditer(
            ur'<object type="[^"]+" id="([^"]+)" value="(\d+)" prefecture="(\d+)"\s*>(.+?)<\s*/\s*object\s*>',
            city_sub, re.S):
        d = data.copy()
        d['city_id'] = string.atoi(m.group(2))
        d['city_alias'] = m.group(1).strip()
        state_id = string.atoi(m.group(3))
        if state_id in states_map:
            d['state'] = states_map[state_id]
        else:
            d['state'] = None
        m1 = re.search(ur'<p [^<>]*>(.+?)</p', m.group(4))
        if m1 is None:
            continue
        d['city_name'] = cm.html2plain(m1.group(1)).strip().upper()
        results.append(d)

    # if len(results) == 0:
    #     cm.dump('Error in fetching cities: %s' % url, log_name)
    return results


def fetch_stores(data):
    url = data['host'] + 'after-sales-services/boutique-finder'
    param = {'productOffer': 'All', 'city': data['city_id'], 'boutiqueType': 'All',
             'country': data['country_id']}
    if data['state'] is not None:
        param['prefecture'] = data['state']['state_id']

    page = 0
    totStore = -1
    store_list = []
    while True:
        if totStore != -1 and len(store_list) >= totStore:
            break
        else:
            page += 1

        param['numPageToGet'] = page
        try:
            body = cm.get_data(url, param)
        except Exception, e:
            # cm.dump('Error in fetching stores: %s, %s' % (url, param), log_name)
            break

        m = re.search(ur'<list id="WS_boutique_list" nbBoutique="(\d+)">', body)
        if m is None:
            # cm.dump('Error in fetching stores: %s, %s' % (url, param), log_name)
            break
        totStore = string.atoi(m.group(1))

        sub = cm.extract_closure(body[m.start():], ur'<list\b', ur'</list>')[0]
        for m in re.finditer(ur'<list id="WS_boutique_\d+">', sub):
            store_sub = cm.extract_closure(sub[m.start():], ur'<list\b', ur'</list>')[0]
            entry = cm.init_store_entry(data['brand_id'], data['brandname_e'], data['brandname_c'])
            entry[cm.city_e] = cm.extract_city(data['city_name'])[0]
            entry[cm.country_e] = data['country_name']
            if data['state'] is not None:
                entry[cm.province_e] = data['state']['state_name']

            m1 = re.search(ur'productOffers="([^"]+)"', store_sub)
            if m1 is not None:
                entry[cm.store_type] = m1.group(1).strip()
            m1 = re.search(ur'boutiqueType="([^"]+)">', store_sub)
            if m1 is not None:
                entry[cm.store_class] = m1.group(1).strip()
            m1 = re.search(ur"<p class='boutique_title'>(.+?)</p>", store_sub)
            if m1 is not None:
                entry[cm.name_e] = m1.group(1).strip()
            m1 = re.search(ur'<object type="text" id="WS_boutique_detail[^"]+">(.+?)</object>', store_sub, re.S)
            if m1 is not None:
                m2 = re.search(ur'<p [^<>]*>(.+?)</p>', m1.group(1), re.S)
                if m2 is not None:
                    addr_list = []
                    for term in (tmp.strip() for tmp in cm.reformat_addr(m2.group(1)).split(',')):
                        pat_tel = re.compile(ur'phone:\s*', re.I)
                        pat_fax = re.compile(ur'fax:\s*', re.I)
                        pat_email = re.compile(
                            r'(?:[a-z0-9!#$%&\'*+/=?^_`{|}~-]+(?:\.[a-z0-9!#$%&\'*+/=?^_`{|}~-]+)*|"(?:[\x01-\x08\x0b\x0c\x0e-\x1f\x21\x23-\x5b\x5d-\x7f]|\\[\x01-\x09\x0b\x0c\x0e-\x7f])*")@(?:(?:[a-z0-9](?:[a-z0-9-]*[a-z0-9])?\.)+[a-z0-9](?:[a-z0-9-]*[a-z0-9])?|\[(?:(?:25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)\.){3}(?:25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?|[a-z0-9-]*[a-z0-9]:(?:[\x01-\x08\x0b\x0c\x0e-\x1f\x21-\x5a\x53-\x7f]|\\[\x01-\x09\x0b\x0c\x0e-\x7f])+)\])')
                        if re.search(pat_tel, term) is not None:
                            entry[cm.tel] = re.sub(pat_tel, '', term).strip()
                        elif re.search(pat_fax, term) is not None:
                            entry[cm.fax] = re.sub(pat_fax, '', term).strip()
                        elif re.search(pat_email, term) is not None:
                            entry[cm.email] = re.search(pat_email, term).group()
                        else:
                            addr_list.append(term)
                    entry[cm.addr_e] = ', '.join(addr_list)

            gs.field_sense(entry)
            ret = gs.addr_sense(entry[cm.addr_e], entry[cm.country_e])
            if ret[1] is not None and entry[cm.province_e] == '':
                entry[cm.province_e] = ret[1]
            if ret[2] is not None and entry[cm.city_e] == '':
                entry[cm.city_e] = ret[2]
            gs.field_sense(entry)
            logger.info('(%s / %d) Found store: %s, %s (%s, %s)' % (data['brandname_e'], data['brand_id'],
                                                                    entry[cm.name_e], entry[cm.addr_e],
                                                                    entry[cm.country_e],
                                                                    entry[cm.continent_e]))
            # db.insert_record(entry, 'stores')
            store_list.append(entry)

    return store_list


def fetch(db, data=None, user='root', passwd=''):
    logging.config.fileConfig('cartier.cfg')
    logger = logging.getLogger('firenzeLogger')
    logger.info(u'cartier STARTED')

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
            return []

    # Walk from the root node, where level == 1.
    if data is None:
        data = {'host': 'http://www.cartier.com/layout/set/flash/',
                'brand_id': 10066, 'brandname_e': u'Cartier', 'brandname_c': u'卡地亚'}

    # db.execute(u'DELETE FROM %s WHERE brand_id=%d' % ('stores', data['brand_id']))
    results = cm.walk_tree({'func': lambda data: func(data, 0), 'data': data})
    logging.info(u'DONE')

    return results
