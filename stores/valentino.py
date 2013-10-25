# coding=utf-8
import hashlib
import logging
import logging.config
import re

from pyquery import PyQuery as pq

import common as cm
from stores import geosense as gs


__author__ = 'Zephyre'


def fetch_countries(db, data, logger=None):
    def func(item):
        d = data.copy()
        d['node_id'] = item.attrib['id']
        d['country_id'] = d['node_id']
        d['country'] = cm.html2plain(item.text).strip().upper()
        d['url'] = d['host'] + item.attrib['href']
        return d

    return tuple(map(func, pq(url=data['url'])('td.country a[id]')))

    #
    # url = data['url']
    # try:
    #     body = cm.get_data(url)
    # except Exception, e:
    #     logger.error(u'Error in fetching countries: %s' % url)
    #     return []
    #
    # results = []
    # for m in re.findall(ur'<td\s+[^<>]*class\s*=\s*"country"[^<>]*><a id="(\d+)" href="([^"]+)"\s*>([^<>]+)', body):
    #     d = data.copy()
    #     d['country_id'] = string.atoi(m[0])
    #     d['country'] = cm.html2plain(m[2]).strip().upper()
    #     d['url'] = m[1]
    #     results.append(d)
    # return tuple(results)


def fetch_stores(db, data, logger=None):
    # url = data['host'] + data['url'] + '.xml'
    url = data['url'] + '.xml'
    try:
        body = cm.get_data(url)
    except Exception, e:
        logger.error(u'Error in fetching stores: %s' % url)
        return []

    start = body.find(ur'<![CDATA')
    if start == -1:
        logger.error(u'Error in fetching countries: %s' % url)
        return []
    body = cm.extract_closure(body[start + 6:], ur'\[', ur'\]')[0]

    # for item in pq(body)('div.store div.storedata'):
    def func(item):
        entry = cm.init_store_entry(data['brand_id'], data['brandname_e'], data['brandname_c'])
        entry[cm.name_e] = cm.html2plain(item('h6')[0].text).strip()
        addr_sub = unicode(pq(item('p')[0]))
        addr_list = [term.strip() for term in cm.reformat_addr(addr_sub).split(',')]
        tel = cm.extract_tel(addr_list[-1])
        if tel != '':
            entry[cm.tel] = tel
            del addr_list[-1]
        entry[cm.addr_e] = ', '.join(addr_list)

        temp = item('a.track_map[href]')
        m = hashlib.md5()
        m.update(url)
        if len(temp) > 0:
            map_ref = temp[0].attrib['href']
            m.update(map_ref)
            m_query = re.search(r'q=([^;]+?)&', cm.html2plain(map_ref))
            if m_query:
                query_parm = m_query.group(1).replace('+', ' ')
                entry['geo_query_param'] = query_parm

        else:
            m.update(entry[cm.addr_e])
        fingerprint = m.hexdigest()
        entry[cm.native_id] = fingerprint
        if entry[cm.native_id] in data['store_list']:
            return

        entry[cm.country_e] = data['country']
        gs.field_sense(entry)
        ret = gs.addr_sense(entry[cm.addr_e])
        if ret[0] is not None and entry[cm.country_e] == '':
            entry[cm.country_e] = ret[0]
        if ret[1] is not None and entry[cm.province_e] == '':
            entry[cm.province_e] = ret[1]
        if ret[2] is not None and entry[cm.city_e] == '':
            entry[cm.city_e] = ret[2]
        gs.field_sense(entry)

        logger.info(('(%s / %d) Found store: %s, %s (%s, %s)' % (data['brandname_e'], data['brand_id'],
                                                                 entry[cm.name_e], entry[cm.addr_e],
                                                                 entry[cm.country_e],
                                                                 entry[cm.continent_e])))
        cm.insert_record(db, entry, data['table'])
        return entry

    store_list = tuple(map(func, (pq(temp) for temp in pq(body)('div.store div.storedata'))))

    m = hashlib.md5()
    m.update(url)
    fingerprint = m.hexdigest()
    # data['node_id'] = fingerprint
    return ()

    # store_list = []
    # for m in re.finditer(ur'<div class="store ', body):
    #     s = cm.extract_closure(body[m.end():], ur'<div\b', ur'</div>')[0]
    #     entry = cm.init_store_entry(data['brand_id'], data['brandname_e'], data['brandname_c'])
    #
    #     m1 = re.search(ur'<h6>([^<>]+)</h6>', s)
    #     if m1 is not None:
    #         entry[cm.name_e] = m1.group(1).strip()
    #
    #     addr_sub = cm.extract_closure(s, ur'<p>', ur'</p>')[0]
    #     addr_list = [term.strip() for term in cm.reformat_addr(addr_sub).split(',')]
    #     tel = cm.extract_tel(addr_list[-1])
    #     if tel != '':
    #         entry[cm.tel] = tel
    #         del addr_list[-1]
    #     entry[cm.addr_e] = ', '.join(addr_list)
    #
    #     m1 = re.search(ur'll=(-?\d+\.\d+),(-?\d+\.\d+)', addr_sub)
    #     if m1 is not None:
    #         entry[cm.lat] = string.atof(m1.group(1))
    #         entry[cm.lng] = string.atof(m1.group(2))
    #
    #     gs.field_sense(entry)
    #     ret = gs.addr_sense(entry[cm.addr_e])
    #     if ret[0] is not None and entry[cm.country_e] == '':
    #         entry[cm.country_e] = ret[0]
    #     if ret[1] is not None and entry[cm.province_e] == '':
    #         entry[cm.province_e] = ret[1]
    #     if ret[2] is not None and entry[cm.city_e] == '':
    #         entry[cm.city_e] = ret[2]
    #     gs.field_sense(entry)
    #
    #     if entry[cm.country_e] == '' or entry[cm.city_e] == '':
    #         ret = None
    #         if entry[cm.lat] != '' and entry[cm.lng] != '':
    #             ret = gs.geocode(latlng='%f,%f' % (entry[cm.lat], entry[cm.lng]))
    #         if ret is None:
    #             ret = gs.geocode(entry[cm.addr_e])
    #
    #         if ret is not None:
    #             city = ''
    #             province = ''
    #             country = ''
    #             zip_code = ''
    #             tmp = ret[0]['address_components']
    #             for v in tmp:
    #                 if 'locality' in v['types']:
    #                     city = v['long_name'].strip().upper()
    #                 elif 'administrative_area_level_1' in v['types']:
    #                     province = v['long_name'].strip().upper()
    #                 elif 'country' in v['types']:
    #                     country = v['long_name'].strip().upper()
    #                 elif 'postal_code' in v['types']:
    #                     zip_code = v['long_name'].strip()
    #             entry[cm.country_e] = country
    #             entry[cm.province_e] = province
    #             entry[cm.city_e] = city
    #             entry[cm.zip_code] = zip_code
    #
    #             gs.field_sense(entry)
    #             ret = gs.addr_sense(entry[cm.addr_e])
    #             if ret[0] is not None and entry[cm.country_e] == '':
    #                 entry[cm.country_e] = ret[0]
    #             if ret[1] is not None and entry[cm.province_e] == '':
    #                 entry[cm.province_e] = ret[1]
    #             if ret[2] is not None and entry[cm.city_e] == '':
    #                 entry[cm.city_e] = ret[2]
    #             gs.field_sense(entry)
    #
    #     logger.info('(%s / %d) Found store: %s, %s (%s, %s)' % (data['brandname_e'], data['brand_id'],
    #                                                             entry[cm.name_e], entry[cm.addr_e],
    #                                                             entry[cm.country_e],
    #                                                             entry[cm.continent_e]))
    #     cm.insert_record(db, entry, data['update_table'] if data['update'] else data['table'])
    #     store_list.append(entry)
    #
    # return ()


def get_data(db, table, logger=None):
    brand_id = 10367

    db.query(str.format('SELECT DISTINCT native_id FROM {0} WHERE brand_id={1}', table, brand_id))
    store_list = tuple(temp[0] for temp in db.store_result().fetch_row(maxrows=0))

    return {'host': 'http://www.valentino.com', 'url': 'http://www.valentino.com/en/home/store_locator/',
            'brand_id': brand_id, 'brandname_e': u'Valentino', 'brandname_c': u'华伦天奴', 'node_id': u'0',
            'store_list': store_list, 'table': table}


def get_func_chain():
    return fetch_countries, fetch_stores


def get_logger():
    logging.config.fileConfig('valentino.cfg')
    logger = logging.getLogger('firenzeLogger')
    return logger


# def fetch(level=1, data=None, user='root', passwd=''):
#     def func(data, level):
#         """
#         :param data:
#         :param level: 0：国家；1：城市；2：商店列表
#         """
#         if level == 0:
#             # 国家列表
#             return [{'func': lambda data: func(data, level + 1), 'data': s} for s in fetch_countries(data)]
#         if level == 1:
#             # 商店
#             return [{'func': None, 'data': s} for s in fetch_stores(data)]
#         else:
#             return []
#
#     # Walk from the root node, where level == 1.
#     if data is None:
#         data = {'host': 'http://www.valentino.com',
#                 'url': 'http://www.valentino.com/en/home/store_locator/',
#                 'brand_id': 10367, 'brandname_e': u'Valentino', 'brandname_c': u'华伦天奴'}
#
#     global db
#     db = cm.StoresDb()
#     db.connect_db(user=user, passwd=passwd)
#     db.execute(u'DELETE FROM %s WHERE brand_id=%d' % ('stores', data['brand_id']))
#
#     results = cm.walk_tree({'func': lambda data: func(data, 0), 'data': data})
#     db.disconnect_db()
#
#     return results


