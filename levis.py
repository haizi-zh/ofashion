# coding=utf-8
import json
import string
import re
import common as cm
import geosense as gs

__author__ = 'Zephyre'

db = None


def fetch_cn(data):
    url = data['url']
    try:
        html = cm.get_data(url)
    except Exception:
        print 'Error occured: %s' % url
        dump_data = {'level': 0, 'time': cm.format_time(), 'data': {'url': url}, 'brand_id': data['brand_id']}
        cm.dump(dump_data)
        return []

    start = html.find('arrData = [')
    if start == -1:
        return []
    sub, start, end=cm.extract_closure(html[start:], ur'\[', ur'\]')
    raw_list=json.loads(sub)

    store_list=[]
    for v1 in raw_list:
        # 省
        province = v1[0].strip()
        for v2 in v1[1]:
            # 市
            city = v2[0].strip()
            for v3 in v2[1]:
                # 商店
                entry = cm.init_store_entry(data['brand_id'], data['brandname_e'], data['brandname_c'])
                terms=v3.split(';')
                if len(terms)<2:
                    continue
                entry['name_c']=terms[0].strip()
                entry['addr_e']=terms[1].strip()
                cm.update_entry(entry, {cm.city_c:city, cm.province_c:province, cm.country_c:u'中国',
                                        cm.country_e:u'CHINA', cm.continent_c:u'亚洲', cm.continent_e:u'ASIA'})

                print '(%s/%d) Found store: %s, %s (%s, %s)' % (data['brandname_e'], data['brand_id'],
                                                          entry[cm.name_c], entry[cm.addr_e], entry[cm.country_e],
                                                          entry[cm.continent_e])
                store_list.append(entry)
                db.insert_record(entry, 'stores')
                store_list.append(entry)

    return store_list


def fetch(level=0, data=None, user='root', passwd=''):
    if data is None:
        data = {'url': 'http://www.levi.com.cn/lcn/storelocator', 'brand_id': 10215,
                'brandname_e': u"Levi's", 'brandname_c': u'李维斯'}

    global db
    db = cm.StoresDb()
    db.connect_db(user=user, passwd=passwd)
    db.execute(u'DELETE FROM %s WHERE brand_id=%d' % ('stores', data['brand_id']))

    results = fetch_cn(data)
    db.disconnect_db()

    return results