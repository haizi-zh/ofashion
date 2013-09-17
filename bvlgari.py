# coding=utf-8
import json
import logging
import string
import re
import common as cm
import geosense as gs
import logging.config

__author__ = 'Zephyre'

type_map = {'wa': u'Watches', 'pfm': u'Fragrances', 'eyw': u'Eyewear', 'jwl': u'Jewels',
            'acc': u'Accessories', 'skn': u'Skincare'}


def fetch_continents(db, data, logger):
    url = data['host'] + data['geo_url']
    param = {'lang': 'EN_US', 'geo_id': 1}

    logger.info(u'FETCHING CONTINENTS...')
    # try:
    body = cm.get_data(url, param)
    # except Exception, e:
    #     logger.error('Error in fetching continents: %s, %s' % (url, param))
    #     return []

    results = []
    for c in json.loads(body)['geoEntityLocaleList']:
        d = data.copy()
        d['continent_id'] = string.atoi(c['geoEntity']['id'])
        d['node_id'] = str.format('continent_id:{0}', d['continent_id'])
        d['continent'] = c['geoEntity']['name'].strip()
        results.append(d)
    return tuple(results)


def fetch_countries(db, data, logger):
    url = data['host'] + data['geo_url']
    param = {'lang': 'EN_US', 'geo_id': data['continent_id']}
    logging.info(unicode.format(u'FETCHING COUNTRIES AT {0}', data['continent']))

    # try:
    body = cm.get_data(url, param)
    # except Exception, e:
    #     logger.error('Error in fetching countries: %s, %s' % (url, param))
    #     return []

    results = []
    for c in json.loads(body)['geoEntityLocaleList']:
        d = data.copy()
        d['country_id'] = string.atoi(c['geoEntity']['id'])
        d['node_id'] = str.format('country_id:{0}', d['country_id'])
        d['country'] = cm.html2plain(c['geoEntity']['name']).strip()
        results.append(d)

    for item in results:
        if gs.look_up(item['country'].upper(), 1) is None:
            print 'Cannot look up %s' % item['country']
    return tuple(results)


def fetch_states(db, data, logger):
    url = data['host'] + data['geo_url']
    param = {'lang': 'EN_US', 'geo_id': data['country_id']}
    logging.info(unicode.format(u'FETCHING STATES AT {0}', data['country']))

    # try:
    body = cm.get_data(url, param)
    # except Exception, e:
    #     logger.error('Error in fetching states: %s, %s' % (url, param))
    #     return []

    results = []
    raw = json.loads(body)
    if 'geoEntityLocaleList' not in raw:
        return ()
    for c in raw['geoEntityLocaleList']:
        d = data.copy()
        if c['geoEntity']['type']['name'] == 'CITY':
            d['state_id'] = data['country_id']
            d['state'] = ''
            d['node_id'] = str.format('state_id:{0}', d['state_id'])
            return d,
        else:
            d['state_id'] = string.atoi(c['geoEntity']['id'])
            d['state'] = cm.html2plain(c['geoEntity']['name']).strip()
            d['node_id'] = str.format('state_id:{0}', d['state_id'])
            results.append(d)
    return tuple(results)


def fetch_cities(db, data, logger):
    url = data['host'] + data['geo_url']
    param = {'lang': 'EN_US', 'geo_id': data['state_id']}
    logging.info(unicode.format(u'FETCHING CITIES AT {0}', data['state']))

    # try:
    body = cm.get_data(url, param)
    # except Exception, e:
    #     logger.error('Error in fetching cities: %s, %s' % (url, param))
    #     return []

    results = []
    try:
        raw = json.loads(body)
    except ValueError as e:
        logger.error(unicode.format(u'Error in fetching {0} / {1}', url, param))
        return ()

    if 'geoEntityLocaleList' not in raw:
        return ()
    for c in raw['geoEntityLocaleList']:
        d = data.copy()
        d['city_id'] = string.atoi(c['geoEntity']['id'])
        d['node_id'] = str.format('city_id:{0}', d['city_id'])
        d['city'] = cm.html2plain(c['geoEntity']['name']).strip()
        results.append(d)
    return tuple(results)


def fetch_stores(db, data, logger):
    url = data['host'] + data['store_url']
    param = {'lang': 'EN_US', 'geo_id': data['city_id']}
    logging.info(unicode.format(u'FETCHING STORES AT {0}', data['city']))

    # try:
    body = cm.get_data(url, param)
    # except Exception, e:
    #     logger.error('Error in fetching cities: %s, %s' % (url, param))
    #     return []

    store_list = []
    raw = json.loads(body)
    if 'storeList' not in raw:
        return []
    for s in raw['storeList']:
        entry = cm.init_store_entry(data['brand_id'], data['brandname_e'], data['brandname_c'])
        entry[cm.city_e] = cm.extract_city(data['city'])[0]
        entry[cm.province_e] = data['state'].upper()
        entry[cm.country_e] = data['country'].upper()
        entry[cm.store_class] = s['type']['name']
        entry[cm.store_type] = ', '.join(type_map[item['name']] for item in s['categories'])
        entry[cm.name_e] = s['name'].strip()
        entry[cm.native_id] = int(s['id'])

        loc = s['location']
        entry[cm.addr_e] = cm.reformat_addr(loc['address'])
        if 'phone' in loc and loc['phone'] is not None:
            entry[cm.tel] = loc['phone'].strip()
        if 'fax' in loc and loc['fax'] is not None:
            entry[cm.fax] = loc['fax'].strip()
        if 'postalCode' in loc and loc['postalCode'] is not None:
            entry[cm.zip_code] = loc['postalCode'].strip()
        if 'latitude' in loc and loc['latitude'] is not None and loc['latitude'].strip() != '':
            entry[cm.lat] = string.atof(loc['latitude'])
        if 'longitude' in loc and loc['longitude'] is not None and loc['longitude'].strip() != '':
            entry[cm.lng] = string.atof(loc['longitude'])

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
        cm.insert_record(db, entry, data['update_table'] if data['update'] else data['table'])

        store_list.append(entry)

    return ()


def get_logger():
    logging.config.fileConfig('bvlgari.cfg')
    logger = logging.getLogger('firenzeLogger')
    return logger


def get_func_chain():
    return fetch_continents, fetch_countries, fetch_states, fetch_cities, fetch_stores


def get_data():
    return {'host': 'http://stores.bulgari.com', 'geo_url': '/blgsl/js-geoentities.html',
            'store_url': '/blgsl/js-stores.html', 'brand_id': 10058, 'brandname_e': u'BVLGARI',
            'brandname_c': u'宝格丽', 'node_id': 0}


def merge(db, data, logger):
    pass


def init(db, data, logger=None):
    # db.query(str.format('DELETE FROM {0} WHERE brand_id={1}',
    #                     data['update_table'] if data['update'] else data['table'],
    #                     data['brand_id']))
    pass


# def fetch(db, data=None, user='root', passwd='', table='spider_stores', update=True, update_table='update_temp'):
#     logger = get_logger()
#
#     def func(data, level):
#         """
#         :param data:
#         :param level: 0：国家；1：城市；2：商店列表
#         """
#         if level == 0:
#             # 洲列表
#             return [{'func': lambda data: func(data, level + 1), 'data': s} for s in fetch_continents(data, logger)]
#         if level == 1:
#             # 国家列表
#             return [{'func': lambda data: func(data, level + 1), 'data': s} for s in fetch_countries(data, logger)]
#         if level == 2:
#             # 州列表
#             return [{'func': lambda data: func(data, level + 1), 'data': s} for s in fetch_states(data, logger)]
#         if level == 3:
#             # 城市列表
#             return [{'func': lambda data: func(data, level + 1), 'data': s} for s in fetch_cities(data, logger)]
#         if level == 4:
#             # 商店
#             return [{'func': None, 'data': s} for s in fetch_stores(db, data, logger)]
#         else:
#             return []
#
#     # Walk from the root node, where level == 1.
#     if data is None:
#         data = {'host': 'http://stores.bulgari.com',
#                 'geo_url': '/blgsl/js-geoentities.html',
#                 'store_url': '/blgsl/js-stores.html',
#                 'brand_id': 110058, 'brandname_e': u'BVLGARI', 'brandname_c': u'宝格丽'}
#
#     db.query(u'DELETE FROM %s WHERE brand_id=%d' % ('spider_stores.stores', data['brand_id']))
#     results = cm.walk_tree({'func': lambda data: func(data, 0), 'data': data})
#     logger.info(u'DONE')
#
#     return results

