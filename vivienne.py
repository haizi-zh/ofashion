# coding=utf-8
import json
import string
import re
import common as cm
import geosense as gs

__author__ = 'Zephyre'

db = None
log_name = 'vivienne_log.txt'


def fetch_countries(data):
    url = data['url']
    try:
        body = cm.get_data(url)
    except Exception, e:
        cm.dump('Error in fetching countries: %s' % url, log_name)
        return []

    m = re.search(ur'<div class="item store_directory">(.+?)</div>', body, re.S)
    if m is None:
        cm.dump('Error in fetching countries: %s' % url, log_name)
        return []
    body = m.group(1)
    results = []
    for m in re.findall(ur'<a href="([^"]+)" title="([^"]+)"', body):
        d = data.copy()
        d['url'] = m[0]
        d['country'] = m[1].strip().upper()
        results.append(d)
    return results


def fetch_stores(data):
    url = data['url']
    try:
        body = cm.get_data(url)
    except Exception, e:
        cm.dump('Error in fetching stores: %s' % url, log_name)
        return []

    store_list = []
    for s in re.findall(ur'<div class="store_wrapper">(.+?)</div>', body, re.S):
        entry = cm.init_store_entry(data['brand_id'], data['brandname_e'], data['brandname_c'])
        entry[cm.country_e] = data['country']

        m = re.search(ur'<h2>(.+?)</h2>', s)
        if m is not None:
            entry[cm.name_e] = cm.html2plain(m.group(1))

        m = re.search(ur'<p>(.+?)</p>', s, re.S)
        if m is not None:
            addr_list = [tmp.strip() for tmp in cm.reformat_addr(m.group(1)).split(',')]
            tel = cm.extract_tel(re.sub(re.compile('^\s*t\s*(\.|:)\s*', re.I), '', addr_list[-1]))
            if tel != '':
                if entry[cm.country_e] == 'CHINA':
                    if len(re.findall(r'\d', tel)) > 6:
                        entry[cm.tel] = tel
                        del addr_list[-1]
                else:
                    entry[cm.tel] = tel
                    del addr_list[-1]
            entry[cm.addr_e] = ', '.join(addr_list)

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

    return store_list


def fetch(level=1, data=None, user='root', passwd=''):
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
            return [{'func': None, 'data': s} for s in fetch_stores(data)]
        else:
            return []

    # Walk from the root node, where level == 1.
    if data is None:
        data = {'url': 'http://www.viviennewestwood.co.uk/w/store-directory',
                'brand_id': 10378, 'brandname_e': u'Vivienne Westwood', 'brandname_c': u'薇薇恩·韦斯特伍德'}

    global db
    db = cm.StoresDb()
    db.connect_db(user=user, passwd=passwd)
    db.execute(u'DELETE FROM %s WHERE brand_id=%d' % ('stores', data['brand_id']))

    results = cm.walk_tree({'func': lambda data: func(data, 0), 'data': data})
    db.disconnect_db()

    return results


