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


def fetch_continents(data, logger):
    """
    获得洲列表
    :param data:
    :return:
    """
    url = data['home_url']
    try:
        html = cm.get_data(url)
        body = pq(html)
    except Exception:
        print 'Error occured: %s' % url
        dump_data = {'level': 0, 'time': cm.format_time(), 'data': {'url': url}, 'brand_id': data['brand_id']}
        cm.dump(dump_data)
        return []

    continents_list = []
    for item in body('option[value!="0"]'):
        d = data.copy()
        d['continent_id'] = int(item.attrib['value'])
        d['continent_e'] = item.text.upper().strip().decode('utf-8')
        continents_list.append(d)
    return continents_list


def fetch_countries(data, logger):
    """
    获得国家列表
    :param data:
    """
    url = data['post_url']
    try:
        html = cm.post_data(url, {'pid': data['continent_id'], 'lang': 'en', 'action': 'popola_select'})
        body = pq(html)
    except Exception:
        print 'Error occured in getting country list: %s' % url
        dump_data = {'level': 1, 'time': cm.format_time(), 'data': {'url': url}, 'brand_id': data['brand_id']}
        cm.dump(dump_data)
        return []

    country_list = []
    for item in body('option[value!="0"]'):
        d = data.copy()
        d['country_id'] = int(item.attrib['value'])
        d['country_e'] = item.text.upper().strip().decode('utf-8')
        country_list.append(d)
    return country_list


def fetch_cities(data, logger):
    """
    获得城市列表
    :param data:
    """
    url = data['post_url']
    try:
        html = cm.post_data(url, {'pid': data['country_id'], 'lang': 'en', 'action': 'popola_select_city'})
        body = pq(html)
    except Exception:
        print 'Error occured in getting city list: %s' % url
        dump_data = {'level': 2, 'time': cm.format_time(), 'data': {'url': url}, 'brand_id': data['brand_id']}
        cm.dump(dump_data)
        return []

    city_list = []
    for item in body('option[value!="0"]'):
        d = data.copy()
        d['city_id'] = int(item.attrib['value'])
        d['city_e'] = item.text.upper().strip().decode('utf-8')
        city_list.append(d)

    return city_list


def fetch_stores(db, data, logger):
    """
    获得商店信息
    :param data:
    """
    url = data['post_url']
    try:
        html = cm.post_data(url, {'pid': data['city_id'], 'lang': 'en', 'action': 'popola_box_DX'})
        if html.strip() == u'':
            logger.error(unicode.format(u'Failed to fetch stores for city {0}', data['city_id']))
            return []
        body = pq(html)
    except Exception as e:
        print 'Error occured in getting city list: %s' % url
        dump_data = {'level': 2, 'time': cm.format_time(), 'data': {'url': url}, 'brand_id': data['brand_id']}
        cm.dump(dump_data)
        return []

    store_list = []
    for item in (pq(temp) for temp in body('a[href]')):
        entry = cm.init_store_entry(data['brand_id'], data['brandname_e'], data['brandname_c'])
        entry[cm.url] = item[0].attrib['href']
        entry[cm.name_e] = item('h3.titleShop')[0].text.strip().decode('utf-8')

        # terms = cm.reformat_addr(item('div.txtBoxSingleStore p.lineHeight14')[0].text).split(',')
        terms = cm.reformat_addr(unicode(item('div.txtBoxSingleStore p.lineHeight14'))).split(',')
        tel = cm.extract_tel(terms[-1])
        if tel != '':
            terms = terms[:-1]
            entry[cm.tel] = tel
        entry[cm.addr_e] = u', '.join([v.strip() for v in terms])
        entry['country_e'] = data['country_e']
        entry['city_e'] = data['city_e']
        gs.field_sense(entry)

        logger.info('(%s / %d) Found store: %s, %s (%s, %s)' % (data['brandname_e'], data['brand_id'],
                                                                entry[cm.name_e], entry[cm.addr_e], entry[cm.country_e],
                                                                entry[cm.continent_e]))
        store_list.append(entry)
        cm.insert_record(db, entry, 'spider_stores.stores')

    return store_list


def fetch(db, data=None, user='root', passwd=''):
    logging.config.fileConfig('sergio.cfg')
    logger = logging.getLogger('firenzeLogger')

    def func(data, level):
        """
        :param data:
        :param level: 0：国家；1：城市；2：商店列表
        """
        if level == 0:
            # 国家
            return [{'func': lambda data: func(data, 1), 'data': c} for c in fetch_continents(data, logger)]
        elif level == 1:
            # 国家列表
            return [{'func': lambda data: func(data, 2), 'data': s} for s in fetch_countries(data, logger)]
        elif level == 2:
            # 城市列表
            return [{'func': lambda data: func(data, 3), 'data': s} for s in fetch_cities(data, logger)]
        elif level == 3:
            # 商店的具体信息
            return [{'func': None, 'data': s} for s in fetch_stores(db, data, logger)]
        else:
            return []

    # Walk from the root node, where level == 1.
    if data is None:
        data = {'home_url': 'http://www.sergiorossi.com/experience/en/pages/stores/',
                'post_url': 'http://www.sergiorossi.com/experience/en/wpapi/store-services/',
                'brand_id': 10316, 'brandname_e': u'Sergio Rossi', 'brandname_c': u'塞乔·罗西'}

    db.query(str.format('DELETE FROM spider_stores.stores WHERE brand_id={0}', data['brand_id']))
    results = cm.walk_tree({'func': lambda data: func(data, 0), 'data': data})
    logger.info(u'Done')

    return results