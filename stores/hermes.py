# coding=utf-8
import json
import string
import re
from stores import geosense as gs

__author__ = 'Zephyre'

db = None
log_name = 'hermes_log.txt'


def fetch_store_list(data):
    url = data['url']
    try:
        body = cm.get_data(url)
    except Exception:
        cm.dump('Error in fetching countries: %s' % url, log_name)
        return []

    start = body.find(ur"'country_select'")
    if start == -1:
        cm.dump('Error in fetching countries: %s' % url, log_name)
        return []
    country_raw = json.loads(cm.extract_closure(body[start:], ur'\[', ur'\]')[0])
    country_map = {}
    for c in country_raw:
        country_map[string.atoi(c['id'])] = c['name']

    start = body.find(ur'loadQuickSearch')
    if start == -1:
        cm.dump('Error in fetching store list: %s' % url, log_name)
        return []
    raw = json.loads(cm.extract_closure(body[start:], ur'\[', ur'\]')[0])

    city_map = {}
    results = []
    for item in raw:
        if item['type'] == 'city':
            country = country_map[string.atoi(item['parent_id'])]
            city_map[string.atoi(item['id'])] = {'name': item['name'], 'country': country}

    for item in raw:
        if item['type'] == 'store':
            d = data.copy()
            d['name'] = item['name']
            d['city'] = dict(city_map[string.atoi(item['parent_id'])])
            d['url'] = data['url'] + item['store_url_alias']
            d['id'] = string.atoi(item['id'])
            results.append(d)
        elif item['type'] == 'city':
            continue

    return results


def fetch_store_details(data):
    url = data['url']
    try:
        body = cm.get_data(url)
    except Exception:
        cm.dump('Error in fetching store details: %s' % url, log_name)
        return []

    start = body.find(ur'<div class="col first" itemprop="address"')
    if start == -1:
        cm.dump('Error in fetching store details: %s' % url, log_name)
        return []

    entry = cm.init_store_entry(data['brand_id'], data['brandname_e'], data['brandname_c'])

    addr = cm.extract_closure(body[start:], ur'<p>', ur'</p>')[0]
    m = re.search(ur'<span itemprop="postalCode">([^<>]+)</span>', addr, re.S)
    if m is not None:
        entry[cm.zip_code] = m.group(1).strip()
    entry[cm.addr_e] = cm.reformat_addr(addr)

    start = body.find(ur'<div class="col" itemprop="contactPoints"')
    if start != -1:
        sub = cm.extract_closure(body[start:], ur'<p>', ur'</p>')[0]
        m = re.search(ur'<span itemprop="telephone">([^<>]+)</span>', sub, re.S)
        if m is not None:
            entry[cm.tel] = m.group(1).strip()
        m = re.search(ur'<span itemprop="faxNumber">([^<>]+)</span>', sub, re.S)
        if m is not None:
            entry[cm.fax] = m.group(1).strip()

    start = body.find(ur'<h2>opening hours</h2>')
    if start != -1:
        sub = cm.extract_closure(body[start:], ur'<table\b', ur'</table>')[0]
        tmp = []
        for m in re.findall(ur'<td>(.+?)</td>', sub):
            tmp.append(cm.html2plain(m).strip())
        entry[cm.hours] = ' '.join(tmp)

    m = re.search(ur'"latitude":(-?\d+\.\d+),"longitude":(-?\d+\.\d+)', body)
    if m is not None:
        entry[cm.lat] = string.atof(m.group(1))
        entry[cm.lng] = string.atof(m.group(2))

    entry[cm.name_e] = data['name']
    entry[cm.country_e] = data['city']['country'].strip().upper()
    entry[cm.city_e] = data['city']['name'].strip().upper()
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
        data = {'data_url': 'http://stores.hermes.com/get_name_list/get_name_list/(parent)',
                'url': 'http://stores.hermes.com',
                'brand_id': 10166, 'brandname_e': u'Hermes', 'brandname_c': u'爱马仕'}

    global db
    db = cm.StoresDb()
    db.connect_db(user=user, passwd=passwd)
    db.execute(u'DELETE FROM %s WHERE brand_id=%d' % ('stores', data['brand_id']))

    results = cm.walk_tree({'func': lambda data: func(data, 0), 'data': data})
    db.disconnect_db()

    return results


