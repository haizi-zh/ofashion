# coding=utf-8
import json
import logging
import logging.config
import string
import re
import traceback
import common as cm
import geosense as gs
from pyquery import PyQuery as pq

__author__ = 'Zephyre'


def fetch_countries(data):
    results = []
    for m in (2, 5):
        d = data.copy()
        d['m'] = m
        results.append(d)
    return tuple(results)


def fetch_stores(db, data, logger):
    url = data['url']
    try:
        body = cm.get_data(url, {'m': data['m']})
    except Exception, e:
        cm.dump('Error in fetching stores: %s' % url, log_name)
        return ()

    store_list = []
    if data['m'] == 2:
        entry = cm.init_store_entry(data['brand_id'], data['brandname_e'], data['brandname_c'])
        entry[cm.country_e] = u'USA'
        entry[cm.city_e] = u'NEW YORK'
        entry[cm.name_e] = u'New York'
        entry[cm.addr_e] = u'40 Mercer St, New York, NY 10013'
        entry[cm.zip_code] = u'10013'
        entry[cm.tel] = u'212 966 2398'
        entry[cm.hours] = u'Monday thru Saturday 11:30 AM to 7:00 PM, Sunday 12:00 to 6:00 PM'
        gs.field_sense(entry)
        ret = gs.addr_sense(entry[cm.addr_e], entry[cm.country_e])
        if ret[1] is not None and entry[cm.province_e] == '':
            entry[cm.province_e] = ret[1]
        if ret[2] is not None and entry[cm.city_e] == '':
            entry[cm.city_e] = ret[2]
        gs.field_sense(entry)
        logger.info('(%s / %d) Found store: %s, %s (%s, %s, %s)' % (data['brandname_e'], data['brand_id'],
                                                                    entry[cm.name_e], entry[cm.addr_e],
                                                                    entry[cm.city_e],
                                                                    entry[cm.country_e], entry[cm.continent_e]))
        cm.insert_record(db, entry, 'spider_stores.stores')
        store_list.append(entry)
    elif data['m'] == 5:
        for country in (pq(tmp) for tmp in pq(body)('table[cellpadding="6"]')):
            country_e = cm.html2plain(country('td[style="color:#FFF;"]')[0].text).strip().upper()
            country_e = 'UAE' if 'arab emirates' in country_e.lower() else country_e
            for store in country('td[valign="top"]'):
                if 'bgcolor' in store.attrib:
                    continue
                addr_raw = cm.reformat_addr(unicode(pq(store)))
                if addr_raw == '':
                    continue
                addr_list = [tmp.strip() for tmp in addr_raw.split(',')]
                entry = cm.init_store_entry(data['brand_id'], data['brandname_e'], data['brandname_c'])
                entry[cm.name_e] = addr_list[0]
                entry[cm.country_e] = country_e
                del addr_list[0]
                if country_e in ('HONG KONG', 'JAPAN', 'UAE') or (
                            country_e == 'THAILAND' and 'ext.' in addr_list[-1]):
                    entry[cm.tel] = addr_list[-1]
                    del addr_list[-1]
                entry[cm.addr_e] = ', '.join(addr_list)

                gs.field_sense(entry)
                ret = gs.addr_sense(entry[cm.addr_e], entry[cm.country_e])
                if ret[1] is not None and entry[cm.province_e] == '':
                    entry[cm.province_e] = ret[1]
                if ret[2] is not None and entry[cm.city_e] == '':
                    entry[cm.city_e] = ret[2]
                gs.field_sense(entry)
                logger.info('(%s / %d) Found store: %s, %s (%s, %s, %s)' % (data['brandname_e'], data['brand_id'],
                                                                            entry[cm.name_e], entry[cm.addr_e],
                                                                            entry[cm.city_e],
                                                                            entry[cm.country_e], entry[cm.continent_e]))
                cm.insert_record(db, entry, 'spider_stores.stores')
                store_list.append(entry)

    return tuple(store_list)


def fetch(db, data=None, user='root', passwd=''):
    logging.config.fileConfig('viviennetam.cfg')
    logger = logging.getLogger('firenzeLogger')

    def func(data, level):
        """
        :param data:
        :param level: 0：国家；1：城市；2：商店列表
        """
        if level == 0:
            # 国家列表
            return [{'func': lambda data: func(data, level + 1), 'data': s} for s in fetch_countries(data)]
        if level == 1:
            # 商店
            return [{'func': None, 'data': s} for s in fetch_stores(db, data, logger)]
        else:
            return ()

    # Walk from the root node, where level == 1.
    if data is None:
        data = {'url': 'http://viviennetam.com/contact/store-locator',
                'brand_id': 10400, 'brandname_e': u'Vivienne Tam', 'brandname_c': u'Vivienne Tam'}

    db.query(str.format('DELETE FROM spider_stores.stores WHERE brand_id={0}', data['brand_id']))
    results = cm.walk_tree({'func': lambda data: func(data, 0), 'data': data})
    logger.info(u'Done')

    return results


