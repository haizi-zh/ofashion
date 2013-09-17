# coding=utf-8
import json
import string
import re
import common as cm
import geosense as gs
import logging
import logging.config

__author__ = 'Zephyre'


def fetch_countries(data):
    """
    获得国家列表
    :param data:
    :return:
    """
    url = data['home_url']
    try:
        html = cm.get_data(url)
    except Exception:
        print 'Error occured: %s' % url
        dump_data = {'level': 0, 'time': cm.format_time(), 'data': {'url': url}, 'brand_id': data['brand_id']}
        cm.dump(dump_data)
        return []

    country_list = []
    for m in re.findall(ur'<option value="(\d+)" class="3">(.+?)</option>', html):
        d = data.copy()
        d['country_id'] = string.atoi(m[0])
        d['country_e'] = m[1].strip().upper()
        country_list.append(d)

    return country_list


def fetch_retails(data):
    data['retail_type'] = 'retail'
    return fetch_stores(data)


def fetch_service(data):
    data['retail_type'] = 'service'
    return fetch_stores(data)


def fetch_stores(data):
    """
    获得商店信息
    :param data:
    :return:
    """
    url = data['post_url']
    try:

        js = json.loads(cm.post_data(url, {'country_id': data['country_id'], 'retail_city': '',
                                           'retail_type': data['retail_type']}).decode('unicode_escape'))
    except Exception:
        print 'Error occured in getting country list: %s' % url
        dump_data = {'level': 1, 'time': cm.format_time(), 'data': {'url': url}, 'brand_id': data['brand_id']}
        cm.dump(dump_data)
        return []

    # country_id=108&retail_city=&retail_type=retail
    # country_id=99&retail_city=&retail_type=service
    store_list = []
    for s in js:
        entry = cm.init_store_entry(data['brand_id'], data['brandname_e'], data['brandname_c'])
        tmp = s['retail_name'].strip()
        if cm.is_chinese(tmp):
            entry[cm.name_c] = tmp
        else:
            entry[cm.name_e] = tmp
        entry[cm.addr_e] = s['retail_gmap'].strip()
        entry[cm.zip_code] = s['retail_zipcode'].strip()
        entry[cm.city_e] = s['retail_city'].strip().upper()
        if s['retail_email'] is not None:
            entry[cm.email] = s['retail_email'].strip()
        if s['retail_website'] is not None:
            entry[cm.url] = s['retail_website'].strip()
        if data['retail_type'] == 'retail':
            entry[cm.store_class] = 'Retail'
        else:
            entry[cm.store_class] = 'Service Center'
        entry[cm.country_e] = s['country_name'].strip().upper()
        entry[cm.continent_e] = s['continent_name'].strip().upper()

        gs.field_sense(entry)
        print '(%s / %d) Found store: %s, %s (%s, %s)' % (data['brandname_e'], data['brand_id'],
                                                          entry[cm.name_e], entry[cm.addr_e], entry[cm.country_e],
                                                          entry[cm.continent_e])
        store_list.append(entry)
        # db.insert_record(entry, 'stores')

    return store_list


def fetch(db, data=None, user='root', passwd=''):
    logging.config.fileConfig('maurice_lacroix.cfg')
    logger = logging.getLogger('firenzeLogger')
    logger.info(u'maurice_lacroix STARTED')

    def func(data, level):
        """
        :param data:
        :param level: 0：国家；1：城市；2：商店列表
        """
        if level == 0:
            # 国家列表
            return [{'func': lambda data: func(data, 1), 'data': c} for c in fetch_countries(data)]
        elif level == 1:
            # 商店信息
            retails = [{'func': None, 'data': s} for s in fetch_retails(data)]
            services = [{'func': None, 'data': s} for s in fetch_service(data)]
            retails.extend(services)
            return retails
        # elif level == 2:
        #     # 城市列表
        #     return [{'func': lambda data: func(data, 3), 'data': s} for s in fetch_cities(data)]
        # elif level == 3:
        #     # 商店的具体信息
        #     return [{'func': None, 'data': s} for s in fetch_stores(data)]
        else:
            return []

    # Walk from the root node, where level == 1.
    if data is None:
        data = {'home_url': 'http://www.mauricelacroix.com/RetailAndService/Finder.sls',
                'post_url': 'http://www.mauricelacroix.com/RetailAndService/FinderJson.sls',
                'brand_id': 10245, 'brandname_e': u'Maurice Lacroix', 'brandname_c': u'艾美'}

    # db.execute(u'DELETE FROM %s WHERE brand_id=%d' % ('stores', data['brand_id']))
    results = cm.walk_tree({'func': lambda data: func(data, 0), 'data': data})
    logger.info(u'DONE')

    return results
