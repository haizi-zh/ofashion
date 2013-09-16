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


def fetch_stores(db, data, logger):
    brand_id, brand_name, url = (data[key] for key in ('brand_id', 'brandname_c', 'url'))

    # try:
    body = cm.get_data(url)
    q=pq(body)
    # except Exception, e:
    #     logger.error(unicode.format(u'Error in fetching contents for {0}', url))
    #     return ()

    m1 = re.search(ur'var\s+markers\s*=\s*\[', body)
    if not m1:
        logger.error(unicode.format(u'Error in finding stores for {0}:{1}', brand_id, brand_name))
        return ()

    body = body[m1.end() - 1:]
    m2 = re.search(ur'\]\s*;', body)
    if not m2:
        logger.error(unicode.format(u'Error in finding stores for {0}:{1}', brand_id, brand_name))
        return ()
    raw = json.loads(body[:m2.end() - 1])

    store_list = []
    for s in raw:
        entry = cm.init_store_entry(brand_id, brand_name, data['brandname_c'])
        # try:
        try:
            entry[cm.lat], entry[cm.lng] = (float(s['location'][idx]) for idx in (0, 1))
        except (KeyError, IndexError, ValueError, TypeError):
            pass

        s = s['content']
        try:
            entry[cm.name_e] = cm.html2plain(s['title']).strip()
        except (KeyError, TypeError):
            pass

        tmp_list = s['analytics_label'].split('-')
        entry[cm.country_e] = tmp_list[0]
        entry[cm.city_e] = cm.extract_city(tmp_list[1])[0]

        try:
            entry[cm.addr_e] = cm.reformat_addr(s['address']).strip()
        except (KeyError, TypeError):
            pass

        try:
            entry[cm.fax] = s['fax'].strip()
        except (KeyError, TypeError):
            pass
        try:
            entry[cm.tel] = s['phone'].strip()
        except (KeyError, TypeError):
            pass
        try:
            entry[cm.email] = s['mail'].strip()
        except (KeyError, TypeError):
            pass
        try:
            entry[cm.url] = u'http://en.longchamp.com/store/map' + s['url'].strip()
        except (KeyError, TypeError):
            pass
        try:
            entry[cm.zip_code] = cm.html2plain(s['zipcode_town']).replace(tmp_list[1], '').strip()
        except (KeyError, TypeError):
            pass

        gs.field_sense(entry)
        ret = gs.addr_sense(entry[cm.addr_e], entry[cm.country_e])
        if ret[1] is not None and entry[cm.province_e] == '':
            entry[cm.province_e] = ret[1]
        if ret[2] is not None and entry[cm.city_e] == '':
            entry[cm.city_e] = ret[2]
        gs.field_sense(entry)

        logger.info(
            unicode.format(u'{0}:{1} FOUND STORE: {2}, {3}, ({4}, {5}, {6})', data['brand_id'], data['brandname_e'],
                           *(entry[key] for key in
                             (cm.name_e, cm.addr_e, cm.city_e, cm.country_e, cm.continent_e))))

        cm.insert_record(db, entry, 'spider_stores.stores')
        store_list.append(entry)

    return tuple(store_list)


def fetch(db, data=None, user='root', passwd=''):
    logging.config.fileConfig('longchamp.cfg')
    logger = logging.getLogger('firenzeLogger')
    logger.info(u'Longchamp STARTED')

    def func(data, level):
        """
        :param data:
        :param level: 0：国家；1：城市；2：商店列表
        """
        if level == 0:
            # 商店
            return [{'func': None, 'data': s} for s in fetch_stores(db, data, logger)]
        else:
            return ()

    # Walk from the root node, where level == 1.
    if data is None:
        data = {'url': 'http://en.longchamp.com/store/map',
                'brand_id': 10510, 'brandname_e': u'Longchamp', 'brandname_c': u'Longchamp'}

    db.query(str.format('DELETE FROM spider_stores.stores WHERE brand_id={0}', data['brand_id']))
    results = cm.walk_tree({'func': lambda data: func(data, 0), 'data': data})
    logger.info(u'Done')

    return results


