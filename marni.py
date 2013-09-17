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
    url = data['url']
    try:
        html = cm.get_data(url)
    except Exception:
        print 'Error occured: %s' % url
        dump_data = {'level': 0, 'time': cm.format_time(), 'data': {'url': url}, 'brand_id': data['brand_id']}
        cm.dump(dump_data)
        return []

    country_list = []
    for m in re.findall(ur'<li class="Level4">\s*?<a id="_.+?" href="(.+?)">(.+?)</a>\s*?</li>', html, re.S):
        data = data.copy()
        data['country_e'] = m[1].strip().upper()
        data['url'] = data['host'] + m[0]
        country_list.append(data)
    return country_list


def fetch_stores(data):
    """
    获得商店信息
    :param data:
    """
    url = data['url']
    try:
        info = json.loads(cm.get_data(url, {'tskay': data['key_term']}))
    except Exception:
        print 'Error occured: %s' % url
        dump_data = {'level': 0, 'time': cm.format_time(), 'data': {'url': url}, 'brand_id': data['brand_id']}
        cm.dump(dump_data)
        return []

    raw_list = info['shops']
    store_list = []
    for s in raw_list:
        entry = cm.init_store_entry(data['brand_id'], data['brandname_e'], data['brandname_c'])
        entry[cm.city_e] = s['city'].strip().upper()
        entry[cm.country_e] = data['country_e'].strip().upper()
        entry[cm.name_e] = s['name'].strip()
        addr = s['address']
        entry[cm.addr_e] = addr

        terms = addr.split(',')
        if len(terms) > 1 and entry[cm.city_e] in terms[-1].strip().upper():
            country = entry['country_e']
            tmp = gs.look_up(country, 1)
            if tmp is not None:
                country = tmp['name_e']
            if country == 'JAPAN':
                # 日本邮编
                m = re.search(ur'\d{3,}[ -\.]+?\d{3,}', terms[-1])
                if m is not None:
                    entry[cm.zip_code] = m.group(0)
            else:
                m = re.search(ur'\d{4,}', terms[-1])
                if m is not None:
                    entry[cm.zip_code] = m.group(0)

        entry[cm.tel] = s['tel']
        entry[cm.fax] = s['fax']
        entry[cm.email] = s['email']
        gs.field_sense(entry)

        print '(%s / %d) Found store: %s, %s (%s, %s)' % (data['brandname_e'], data['brand_id'],
                                                          entry[cm.name_e], entry[cm.addr_e], entry[cm.country_e],
                                                          entry[cm.continent_e])
        store_list.append(entry)
        # db.insert_record(entry, 'stores')

    return store_list


def fetch(db, data=None, user='root', passwd=''):
    logging.config.fileConfig('marni.cfg')
    logger = logging.getLogger('firenzeLogger')
    logger.info(u'marni STARTED')

    def func(data, level):
        """
        :param data:
        :param level: 0：国家；1：城市；2：商店列表
        """
        if level == 0:
            # 国家列表
            return [{'func': lambda data: func(data, 1), 'data': c} for c in fetch_countries(data)]
        elif level == 1:
            # 商店列表
            return [{'func': None, 'data': s} for s in fetch_stores(data)]
        else:
            return []

    # Walk from the root node, where level == 1.
    if data is None:
        data = {'url': 'http://www.marni.cn/experience/marni/en/marni_group/shops.asp',
                'host': 'http://www.marni.cn',
                'key_term': 'A444F5AB', 'brand_id': 10241, 'brandname_e': u'Marni', 'brandname_c': u'玛尼'}

    # db.query('DELETE FROM %s WHERE brand_id=%d' % ('spider_stores.stores', data['brand_id']))
    results = cm.walk_tree({'func': lambda data: func(data, 0), 'data': data})
    logging.info(u'DONE')

    return results
