# coding=utf-8
import json
import logging
import string
import re
import common as cm
import geosense as gs
from pyquery import PyQuery as pq
import traceback
import logging.config

__author__ = 'Zephyre'


def fetch_countries(data, logger):
    url = data['new_home_url']
    q = pq(url=url)
    country_list = []
    for item in q('#country option[value!=""]'):
        d = data.copy()
        d['country_id'] = item.attrib['value']
        country_e = item.text.strip().upper()
        ret = gs.look_up(country_e, 1)
        if ret is not None:
            country_e = ret['name_e']
        d['country_e'] = country_e
        country_list.append(d)
    return country_list


    # try:
    #     html = cm.get_data(url)
    #     q = pq(html)
    # except Exception as e:
    #     logger.error(unicode.format(u'Error occured for {0}', url))
    #     # print 'Error occured: %s' % url
    #     # dump_data = {'level': 0, 'time': cm.format_time(), 'data': {'url': url}, 'brand_id': data['brand_id']}
    #     # cm.dump(dump_data)
    #     return ()
    #
    # country_list = []
    # for item in q('#country option[value!="reset"]'):
    #     d = data.copy()
    #     d['country_id'] = string.atoi(item.attrib['value'])
    #
    #     country_e = cm.html2plain(item.text).strip().upper()
    #     ret = gs.look_up(country_e, 1)
    #     if ret is not None:
    #         country_e = ret['name_e']
    #     d['country_e'] = country_e
    #     country_list.append(d)
    # return country_list


def fetch_cities(data, logger):
    url = data['post_city']
    try:
        html = cm.post_data(url, {'country': data['country_id']})
        q = pq(html)
    except Exception:
        logger.error(unicode.format(u'Error occured for country:{0}', data['country_id']))
        # print 'Error occured: %s' % url
        # dump_data = {'level': 0, 'time': cm.format_time(), 'data': {'url': url}, 'brand_id': data['brand_id']}
        # cm.dump(dump_data)
        return ()

    city_list = []
    for item in q('#cities option[value!="0"]'):
        d = data.copy()
        city_e = cm.html2plain(item.text).strip().upper()
        ret = gs.look_up(city_e, 3)
        if ret is not None:
            city_e = ret['name_e']
        d['city_e'] = city_e
        city_list.append(d)

    return city_list


def fetch_stores(db, data, logger):
    q = pq(url='http://www.paulandjoe.com/en/ozcms/stores/list/?country_id=&postcode=')

    store_list = []

    # Country
    country_a = q('#store_list>li>a')
    country_b = q('#store_list>li>ul')
    assert (len(country_a) == len(country_b))
    for i in xrange(len(country_a)):
        country = country_a[i].text.strip().upper()
        store_a = pq(country_b[i])('a.marker-store')
        store_b = pq(country_b[i])('span.store-infos')
        assert (len(store_a) == len(store_b))
        for j in xrange(len(store_a)):
            entry = cm.init_store_entry(data['brand_id'], data['brandname_e'], data['brandname_c'])

            lat = store_a[j].attrib['data-latitude']
            lat = float(lat) if lat else None
            lng = store_a[j].attrib['data-longitude']
            lng = float(lng) if lng else None
            if lat and lng:
                entry[cm.lat], entry[cm.lng] = lat, lng

            entry[cm.name_e] = store_a[j].text
            entry[cm.addr_e] = cm.reformat_addr(str(pq(store_b[j])))# cm.reformat_addr(str(store_b[j]))
            entry[cm.country_e] = country

            gs.field_sense(entry)
            ret = gs.addr_sense(entry[cm.addr_e])
            if ret[0] is not None and entry[cm.country_e] == '':
                entry[cm.country_e] = ret[0]
            if ret[1] is not None and entry[cm.province_e] == '':
                entry[cm.province_e] = ret[1]
            if ret[2] is not None and entry[cm.city_e] == '':
                entry[cm.city_e] = ret[2]
            gs.field_sense(entry)
            logger.info('(%s/%d) Found store: %s, %s (%s, %s)' % (data['brandname_e'], data['brand_id'],
                                                                  entry[cm.name_e], entry[cm.addr_e],
                                                                  entry[cm.country_e],
                                                                  entry[cm.continent_e]))
            store_list.append(entry)
            cm.insert_record(db, entry, 'spider_stores.stores')

    return tuple(store_list)



def fetch(db, data=None, user='root', passwd=''):
    logging.config.fileConfig('paul_joe.cfg')
    logger = logging.getLogger('firenzeLogger')
    logger.info(u'paul_joe STARTED')

    def func(data, level):
        """
        :param data:
        :param level: 0：国家；1：城市；2：商店列表
        """
        if level == 0:
            # 商店信息
            store_list = fetch_stores(db, data, logger)
            return [{'func': None, 'data': s} for s in store_list]
        else:
            return []

    # Walk from the root node, where level == 1.
    if data is None:
        data = {'post_shops': 'http://www.paulandjoe.com/storelocator/index/shops/',
                'post_city': 'http://www.paulandjoe.com/storelocator/index/city/',
                'home_url': 'http://www.paulandjoe.com/storelocator/',
                'new_store_url': 'http://www.paulandjoe.com/en/ozcms/stores/form/',
                'new_home_url': 'http://www.paulandjoe.com/en/ozcms/stores/search/',
                'brand_id': 10297, 'brandname_e': u'Paul & Joe', 'brandname_c': u''}

    db.query(str.format('DELETE FROM spider_stores.stores WHERE brand_id={0}', data['brand_id']))
    results = cm.walk_tree({'func': lambda data: func(data, 0), 'data': data})
    logging.info(u'DONE')

    return results


