# coding=utf-8
import json
import string
import urllib

__author__ = 'Zephyre'

import re
import common

db = None
host = 'http://www.baume-et-mercier.cn'
url_init = 'http://www.baume-et-mercier.cn/ajax/store-locator/'
url_more = 'http://www.baume-et-mercier.cn/ajax/store-locator-pages/'
brand_id = 10032
brandname_e = u'Baume & Mercier'
brandname_c = u'名仕'


def fetch(level=1, data=None, user='root', passwd=''):
    db = common.StoresDb()
    db.connect_db(user=user, passwd=passwd)

    data = {'s': -89, 'w': -179, 'n': 89, 'e': 179, 'chinese': 0, 'repair': 1, 'store': 1}
    try:
        html = common.get_data(url_init, data)
    except Exception:
        print 'Error occured in getting the list of countries: %s' % url_init
        dump_data = {'level': 1, 'time': common.format_time(), 'data': {'data': url_init}, 'brand_id': brand_id}
        common.dump(dump_data)
        return []

    store_list = []

    store_map = json.loads(html)
    tot = 0
    while True:
        # 得到{'uid':entry}的字典
        tmp = store_map['lists']
        # 是否有'more'
        flag = False
        if 'has_key' not in dir(tmp):
            raw_stores = {}
            for item in tmp:
                if 'more' in item:
                    flag = item['more']
                else:
                    raw_stores[item['nid']] = item
        else:
            raw_stores = tmp
            for k in tmp:
                if 'more' in tmp[k]:
                    flag = tmp[k]['more']
                    break

        # 分析raw_stores


        for k in raw_stores:
            s = raw_stores[k]
            if 'more' in s:
                flag = s['more']
            else:
                entry = common.init_store_entry(brand_id)
                common.update_entry(entry, {common.brandname_e: brandname_e,
                                            common.brandname_c: brandname_c})

                country_c = s['country']
                if country_c is not None:
                    term = common.geo_translate(country_c)
                    if len(term) == 0:
                        print 'Error in geo translating: %s' % country_c
                        if common.is_chinese(country_c):
                            entry[common.country_c] = country_c
                        else:
                            entry[common.country_e] = country_c
                    else:
                        common.update_entry(entry, {common.continent_e: term[common.continent_e],
                                                    common.continent_c: term[common.continent_c],
                                                    common.country_e: term[common.country_e],
                                                    common.country_c: term[common.country_c]})

                if s['address'] is not None:
                    addr = common.reformat_addr(s['address'])
                    if common.is_chinese(addr):
                        entry[common.addr_c] = addr
                    else:
                        entry[common.addr_e] = addr

                if s['city'] is not None:
                    if common.is_chinese(s['city']):
                        entry[common.city_c] = s['city']
                    else:
                        entry[common.city_e] = s['city']

                entry[common.email] = s['email']
                entry[common.fax] = s['fax']
                if s['latitude'] is not None:
                    entry[common.lat] = string.atof(s['latitude'])
                if s['longitude'] is not None:
                    entry[common.lng] = string.atof(s['longitude'])
                entry[common.tel] = s['phone']
                entry[common.zip_code] = s['postal_code']

                if s['title'] is not None:
                    name = s['title']
                    if common.is_chinese(name):
                        entry[common.name_c] = name
                    else:
                        entry[common.name_e] = name

                entry[common.hours] = s['operating_hours']
                if s['url'] is not None:
                    entry[common.url] = host + s['url']

                for k in entry:
                    if entry[k] is None:
                        entry[k] = ''
                common.chn_check(entry)
                print '%s: Found store: %s, %s (%s, %s)' % (
                    brandname_e, entry[common.name_e], entry[common.addr_e], entry[common.country_e],
                    entry[common.continent_e])
                db.insert_record(entry, 'stores')
                store_list.append(entry)

        if flag:
            tot += len(store_map['lists']) - 1
            data['offset'] = tot
            store_map = json.loads(common.get_data(url_more, data))
            continue
        else:
            tot += len(store_map['lists'])
            break
    print 'Found a total of %d stores.' % tot
    db.disconnect_db()
    return store_list