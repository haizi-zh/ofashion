# coding=utf-8
import json
import string
import common
from stores import geosense as gs

__author__ = 'Zephyre'

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
    db.execute(u'DELETE FROM %s WHERE brand_id=%d' % ('stores', brand_id))

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
                entry = common.init_store_entry(brand_id, brandname_e, brandname_c)

                if s['country'] is not None:
                    country_c = s['country'].strip().upper()
                    ret = gs.look_up(country_c, 1)
                    if ret is not None:
                        entry[common.country_e] = ret['name_e']
                        entry[common.country_c] = ret['name_c']
                    else:
                        if common.is_chinese(country_c):
                            entry[common.country_c] = country_c
                        else:
                            entry[common.country_e] = country_c

                if s['address'] is not None:
                    addr = common.reformat_addr(s['address'])
                    if common.is_chinese(addr):
                        entry[common.addr_c] = addr
                    else:
                        entry[common.addr_e] = addr

                city = s['city']
                if city is not None:
                    city = city.strip().upper()
                    ret = gs.look_up(city, 3)
                    if ret is not None:
                        entry[common.city_c] = ret['name_c']
                        entry[common.city_e] = ret['name_e']
                    else:
                        if common.is_chinese(city):
                            entry[common.city_c] = city
                        else:
                            entry[common.city_e] = city

                entry[common.city_e] = common.extract_city(entry[common.city_e])[0]

                if s['email'] is not None:
                    entry[common.email] = s['email']
                if s['fax'] is not None:
                    entry[common.fax] = s['fax']
                if s['latitude'] is not None:
                    entry[common.lat] = string.atof(s['latitude'])
                if s['longitude'] is not None:
                    entry[common.lng] = string.atof(s['longitude'])
                if s['phone'] is not None:
                    entry[common.tel] = s['phone']
                if s['postal_code'] is not None:
                    entry[common.zip_code] = s['postal_code']

                if s['title'] is not None:
                    name = s['title']
                    if common.is_chinese(name):
                        entry[common.name_c] = name
                    else:
                        entry[common.name_e] = name

                if s['operating_hours'] is not None:
                    entry[common.hours] = s['operating_hours']
                if s['url'] is not None:
                    entry[common.url] = host + s['url']

                gs.field_sense(entry)

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