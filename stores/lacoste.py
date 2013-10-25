# coding=utf-8
import json
import logging
import logging.config
import string

from pyquery import PyQuery as pq

import common as cm
from stores import geosense as gs


__author__ = 'Zephyre'


def fetch_continents(data, logger):
    vals = {1: u'Africa', 2: u'Asia', 3: u'Europe', 4: u'North America', 5: u'South America', 6: u'Oceania'}
    results = []
    for key in vals:
        d = data.copy()
        d['continent_id'] = key
        d['continent'] = vals[key]
        results.append(d)
    return results


def fetch_countries(data, logger):
    url = data['url']
    param = {'action': 'getCountriesByContinent', 'idContinent': data['continent_id'],
             'filter': 'clothing;lacoste%20l!ve'}
    try:
        body = cm.get_data(url, param)
        q = pq(body)
    except Exception:
        # cm.dump('Error in fetching countries: %s, %s' % (url, param), log_name)
        return []

    raw = json.loads(body)['root']['DATA']['countries']
    results = []
    for c in raw:
        d = data.copy()
        code = c['country']['iso2']
        d['country_id'] = c['country']['id']
        d['country_code'] = code
        ret = gs.look_up(code, 1)
        if ret is not None:
            uid = gs.country_map['lookup'][code]
            gs.country_map['data'][uid]['iso3'] = c['country']['iso3']
            gs.country_map['lookup'][c['country']['iso3']] = uid
        results.append(d)
    return results


def fetch_cities(data, logger):
    url = data['url']
    param = {'action': 'getRegionsAndCitiesByCountry', 'idCountry': data['country_id'],
             'filter': 'clothing;lacoste%20l!ve'}
    try:
        body = cm.get_data(url, param)
        q = pq(body)
    except Exception:
        # cm.dump('Error in fetching cities: %s, %s' % (url, param), log_name)
        return []

    raw = json.loads(body)['root']['DATA']['regions']
    results = []
    for region in raw:
        region_name = region['region']['name']
        for city in region['region']['cities']:
            d = data.copy()
            d['city_id'] = city['city']['id']
            d['city'] = city['city']['name']
            if region_name != 'noRegion':
                d['province_id'] = region['region']['id']
                d['province'] = region['region']['name']
            results.append(d)
    return results


def fetch_stores(db, data, logger):
    def proc_quotes(value):
        return value.replace('"', r'\"')

    url = data['url']
    param = {'action': 'getStoresByCity', 'idCity': data['city_id'],
             'filter': 'clothing;lacoste%20l!ve'}
    try:
        body = cm.get_data(url, param)
        q = pq(body)
    except Exception:
        # cm.dump('Error in fetching stores: %s, %s' % (url, param), log_name)
        return []

    raw = json.loads(body)['root']['DATA']['stores']
    store_list = []
    for s in [tmp['store'] for tmp in raw]:
        entry = cm.init_store_entry(data['brand_id'], data['brandname_e'], data['brandname_c'])
        entry[cm.name_e] = s['name'].strip()
        entry[cm.country_e] = data['country_code']
        entry[cm.addr_e] = cm.html2plain(s['address']).strip()
        entry[cm.store_type] = s['category'].strip()
        entry[cm.city_e] = cm.extract_city(s['city'])[0]
        if s['email'] is not None:
            entry[cm.email] = s['email'].strip()
        if s['fax'] is not None:
            entry[cm.fax] = s['fax'].strip()
        if s['infoHours'] is not None:
            entry[cm.hours] = s['infoHours'].strip()
        if s['latitude'] is not None and s['latitude'].strip() != '':
            entry[cm.lat] = string.atof(s['latitude'])
        if s['longitude'] is not None and s['longitude'].strip() != '':
            entry[cm.lng] = string.atof(s['longitude'])
        if s['phone'] is not None:
            entry[cm.tel] = s['phone'].strip()
        if s['postCode'] is not None:
            entry[cm.zip_code] = s['postCode'].strip()

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

        uid = unicode.format(u'{0}|{1}|{2}', entry[cm.addr_e], entry[cm.name_e], entry[cm.city_e])
        db.query(
            unicode.format(
                u'SELECT idstores FROM spider_stores.stores WHERE name_e="{0}" && addr_e="{1}" && city_e="{2}"',
                *map(lambda key: proc_quotes(entry[key]), (cm.name_e, cm.addr_e, cm.city_e))).encode('utf-8'))
        idstores_list = tuple(int(temp[0]) for temp in db.store_result().fetch_row(maxrows=0))
        cnt = len(idstores_list)
        if cnt == 1:
            db.query(str.format('UPDATE spider_stores.stores SET lat={0},lng={1} WHERE idstores={2}', entry[cm.lat],
                                entry[cm.lng], idstores_list[0]))
        else:
            logger.error(unicode.format(u'NOT UNIQUE: {0}', uid))


        # db.insert_record(entry, 'stores')
        store_list.append(entry)
    return store_list


def fetch(db, data=None, user='root', passwd=''):
    logging.config.fileConfig('lacoste.cfg')
    logger = logging.getLogger('firenzeLogger')
    logger.info(u'lacoste STARTED')

    def func(data, level):
        """
        :param data:
        :param level: 0：国家；1：城市；2：商店列表
        """
        if level == 0:
            # 洲列表
            return [{'func': lambda data: func(data, level + 1), 'data': s} for s in fetch_continents(data, logger)]
        if level == 1:
            # 国家列表
            return [{'func': lambda data: func(data, level + 1), 'data': s} for s in fetch_countries(data, logger)]
        if level == 2:
            # 城市列表
            return [{'func': lambda data: func(data, level + 1), 'data': s} for s in fetch_cities(data, logger)]
        if level == 3:
            # 商店
            return [{'func': None, 'data': s} for s in fetch_stores(db, data, logger)]
        else:
            return []

    # Walk from the root node, where level == 1.
    if data is None:
        data = {'url': 'http://www.lacoste.com/includes/api/stores.api.php',
                'brand_id': 10204, 'brandname_e': u'LACOSTE', 'brandname_c': u'鳄鱼'}

    # db.execute(u'DELETE FROM %s WHERE brand_id=%d' % ('stores', data['brand_id']))

    results = cm.walk_tree({'func': lambda data: func(data, 0), 'data': data})
    logging.info(u'DONE')
    # gs.commit_maps(1)

    return results
