# coding=utf-8
import json
import re
from stores import geosense as gs

__author__ = 'Zephyre'

db = None
log_name = 'dkny_log.txt'
region_map = {}


def fetch_countries(data):
    url = data['url']
    #<li class="param param-country">
    try:
        body = cm.get_data(url)
    except Exception, e:
        cm.dump('Error in fetching countries: %s' % url, log_name)
        return ()

    start = body.find(ur'<li class="param param-country">')
    m = re.search(ur'<select>(.+?)</select>', body[start:], re.S)
    sub = m.group(1)
    results = []
    for m in re.findall(ur'<option value="/store\?country=([a-zA-Z]{2})"[^<>]*>([^<>]+)', sub):
        d = data.copy()
        d['country_code'] = m[0]
        d['url'] = '%s/store?country=%s' % (data['host'], m[0])
        d['country'] = cm.html2plain(m[1]).strip()
        # if m[0]=='us':
        results.append(d)
    return tuple(results)


def fetch_stores(data):
    url = data['data_url']
    param = {'output': 'json', 'country': data['country_code'], 'brand': 'dkny'}
    page = 0
    tot_page = -1
    store_list = []
    while True:
        page += 1
        if tot_page != -1 and page > tot_page:
            break
        param['p'] = page
        try:
            body = cm.get_data(url, param)
        except Exception, e:
            cm.dump('Error in fetching stores: %s, %s' % (url, param), log_name)
            return ()

        raw = json.loads(body)
        tot_page = raw['Stores']['TotalPages']
        if data['country_code'] not in region_map:
            # 构造州列表
            region_map[data['country_code']] = dict((item['RegionId'], item['Name']) for item in raw['Regions'])

        for s in raw['Stores']['Items']:
            entry = cm.init_store_entry(data['brand_id'], data['brandname_e'], data['brandname_c'])
            entry[cm.country_e] = data['country_code'].upper()
            entry[cm.city_e] = cm.extract_city(s['City'])[0]
            entry[cm.name_e] = cm.html2plain(s['Name']).strip()
            entry[cm.addr_e] = cm.reformat_addr(s['Address'])
            entry[cm.tel] = s['Phone'].strip() if s['Phone'] else ''
            entry[cm.fax] = s['Fax'].strip() if s['Fax'] else ''
            entry[cm.email] = s['Email'].strip() if s['Email'] else ''
            entry[cm.lat] = s['Latitude'] if s['Latitude'] else ''
            entry[cm.lng] = s['Longitude'] if s['Longitude'] else ''
            region_id = s['RegionId']
            if region_id in region_map[data['country_code']]:
                entry[cm.province_e] = cm.html2plain(region_map[data['country_code']][region_id]).strip().upper()

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
    pass


def fetch_cities(data):
    pass


def fetch(level=1, data=None, user='root', passwd=''):
    def func(data, level):
        """
        :param data:
        :param level: 0：国家；1：城市；2：商店列表
        """
        if level == 0:
            # 国家列表
            return [{'func': lambda data: func(data, level + 1), 'data': s} for s in fetch_countries(data)]
            # if level == 1:
        #     # 州列表
        #     return [{'func': lambda data: func(data, level + 1), 'data': s} for s in fetch_states(data)]
        # if level == 2:
        #     # 城市列表
        #     return [{'func': lambda data: func(data, level + 1), 'data': s} for s in fetch_cities(data)]
        if level == 1:
            # 商店
            return [{'func': None, 'data': s} for s in fetch_stores(data)]
        else:
            return ()

    # Walk from the root node, where level == 1.
    if data is None:
        data = {'data_url': 'http://www.dkny.com/store/listpartial',
                'url': 'http://www.dkny.com/store',
                'host': 'http://www.dkny.com',
                'brand_id': 10108, 'brandname_e': u'DKNY', 'brandname_c': u'唐可娜儿'}

    global db
    db = cm.StoresDb()
    db.connect_db(user=user, passwd=passwd)
    db.execute(u'DELETE FROM %s WHERE brand_id=%d' % ('stores', data['brand_id']))

    results = cm.walk_tree({'func': lambda data: func(data, 0), 'data': data})
    db.disconnect_db()
    cm.dump('Done!', log_name)

    return results


