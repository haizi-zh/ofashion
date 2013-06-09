# coding=utf-8
import json
import string
import re
import common as cm
import geosense as gs

__author__ = 'Zephyre'

host = 'http://www.kenzo.com'
url = 'https://www.kenzo.com/en/services/store'
brand_id = 10192
brandname_e = u'KENZO'
brandname_c = u'高田贤三'


def fetch(level=1, data=None, user='root', passwd=''):
    db = cm.StoresDb()
    db.connect_db(user=user, passwd=passwd)
    db.execute(u'DELETE FROM %s WHERE brand_id=%d' % ('stores', brand_id))

    try:
        html = cm.get_data(url)
    except Exception:
        print 'Error occured: %s' % url
        dump_data = {'level': 1, 'time': cm.format_time(), 'data': {'url': url}, 'brand_id': brand_id}
        cm.dump(dump_data)
        return []

    js = json.loads(html)
    store_list = []
    for s in js['data']['list']:
        entry = cm.init_store_entry(brand_id, brandname_e, brandname_c)
        cm.update_entry(entry, {cm.lat: string.atof(s['geo']['lat']),
                                cm.lng: string.atof(s['geo']['lng'])})
        entry[cm.name_e] = s['contact']['title']
        entry[cm.addr_e] = cm.reformat_addr(s['contact']['address'])
        entry[cm.tel] = s['contact']['phone']
        entry[cm.fax] = s['contact']['fax']
        entry[cm.hours] = cm.reformat_addr(s['contact']['hours'])
        entry[cm.store_type]=s['contact']['selling']
        entry[cm.url]=host+s['link']

        gs.update_city_map(s['city'], s['country'], s['continent'])
        cm.update_entry(entry,{cm.continent_e:s['continent'], cm.country_e:s['country'],
                               cm.city_e:s['city']})
        gs.field_sense(entry)

        print '(%s / %d) Found store: %s, %s (%s, %s)' % (
            brandname_e, brand_id, entry[cm.name_e], entry[cm.addr_e], entry[cm.country_e],
            entry[cm.continent_e])
        db.insert_record(entry, 'stores')
        store_list.append(entry)

    db.disconnect_db()
    gs.commit_maps(1)
    gs.commit_maps(3)
    return store_list
