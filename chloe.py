# coding=utf-8
import json
import string
import re
import common as cm
import geosense as gs

__author__ = 'Zephyre'

db = None
log_name = 'chloe_log.txt'


def fetch_stores(data):
    url = data['url']
    try:
        body = cm.get_data(url)
    except Exception, e:
        cm.dump('Error in fetching stores: %s' % url, log_name)
        return ()

    store_list = []
    for m1 in re.findall(ur'<country id="\d+" name="([^"]+)"[^<>]*>(.+?)<\s*/\s*country\s*>', body, re.S):
        country = cm.html2plain(m1[0]).strip().upper()
        for m2 in re.findall(ur'<city name="([^"]+)"\s*>(.+?)<\s*/\s*city\s*>', m1[1], re.S):
            city = cm.html2plain(m2[0]).strip().upper()
            for m3 in re.findall(ur'<shop id="\d+"[^<>]*>(.+?)<\s*/\s*shop\s*>', m2[1], re.S):
                entry = cm.init_store_entry(data['brand_id'], data['brandname_e'], data['brandname_c'])
                entry[cm.country_e] = country
                entry[cm.city_e] = city

                m4 = re.search(ur'<title>([^<>]+)</title>', m3)
                entry[cm.name_e] = cm.html2plain(m4.group(1)).strip() if m4 else ''
                m4 = re.search(ur'<address>([^<>]+)</address>', m3)
                entry[cm.addr_e] = cm.reformat_addr(m4.group(1)).strip() if m4 else ''
                m4 = re.search(ur'<type>([^<>]+)</type>', m3)
                entry[cm.store_type] = m4.group(1).strip() if m4 else ''

                m4 = re.search(ur'<openings>(.+?)</openings>', m3, re.S)
                if m4:
                    hours_list = ('%s: %s' % (item[0], item[1]) for item in
                                  re.findall(ur'<day>\s*<interval>([^<>]+)</interval>\s*<time>([^<>]+)</time>\s*</day>',
                                             m4.group(1), re.S))
                    entry[cm.hours] = ', '.join(hours_list)

                m4 = re.search(ur'<phone>([^<>]+)</phone>', m3)
                entry[cm.tel] = m4.group(1).strip() if m4 else ''
                m4 = re.search(ur'<fax>([^<>]+)</fax>', m3)
                entry[cm.fax] = m4.group(1).strip() if m4 else ''

                gs.field_sense(entry)
                ret = gs.addr_sense(entry[cm.addr_e], entry[cm.country_e])
                if ret[1] is not None and entry[cm.province_e] == '':
                    entry[cm.province_e] = ret[1]
                if ret[2] is not None and entry[cm.city_e] == '':
                    entry[cm.city_e] = ret[2]
                gs.field_sense(entry)

                cm.dump('(%s / %d) Found store: %s, %s (%s, %s)' % (data['brandname_e'], data['brand_id'],
                                                                    entry[cm.name_e], entry[cm.addr_e],
                                                                    entry[cm.country_e],
                                                                    entry[cm.continent_e]), log_name)
                db.insert_record(entry, 'stores')
                store_list.append(entry)

    return tuple(store_list)


def fetch(level=1, data=None, user='root', passwd=''):
    def func(data, level):
        """
        :param data:
        :param level: 0：国家；1：城市；2：商店列表
        """
        if level == 0:
            # 商店
            return [{'func': None, 'data': s} for s in fetch_stores(data)]
        else:
            return ()

    # Walk from the root node, where level == 1.
    if data is None:
        data = {'url': 'http://www.chloe.com/languages/en/xml/boutiques_data.xml?CacheBuster=1372490251197',
                'brand_id': 10079, 'brandname_e': u'Chloe', 'brandname_c': u'蔻依'}

    global db
    db = cm.StoresDb()
    db.connect_db(user=user, passwd=passwd)
    db.execute(u'DELETE FROM %s WHERE brand_id=%d' % ('stores', data['brand_id']))

    results = cm.walk_tree({'func': lambda data: func(data, 0), 'data': data})
    db.disconnect_db()
    cm.dump('Done!', log_name)

    return results


