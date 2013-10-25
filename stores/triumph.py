# coding=utf-8
import json
import string
import re
import time
from stores import geosense as gs

__author__ = 'Zephyre'

db = None
url = 'http://storelocator.triumph.com/solr/api/v1/stores'
brand_id = 10359
brandname_e = u'Triumph'
brandname_c = u'黛安芬'


def fetch(level=1, data=None, host='localhost', port=3306, user='root', passwd='123456'):
    tot = 0
    start = 0
    store_list = []
    data = {'q': '*:*', 'pt': '0,0', 'd': 100000, 'start': 0, 'rows': 100}
    # data = {'q': '*:*', 'pt': '36.778261,-119.417932', 'd': 50, 'start': 0, 'rows': 100}

    db = cm.StoresDb()
    db.connect_db(host=host, port=port, user=user, passwd=passwd, db='brand_stores')
    db.execute(u'DELETE FROM %s WHERE brand_id=%d' % ('stores', brand_id))

    while True:
        cm.dump('Fetching from %d' % start, 'triumph_log.txt')
        try:
            data['start'] = start
            html = cm.get_data(url, data)
            raw_list = json.loads(html)
            if tot == 0:
                tot = raw_list['response']['numFound']
                cm.dump('Found: %d' % tot, 'triumph_log.txt')
            raw_list = raw_list['response']['docs']
        except Exception:
            cm.dump('Error occured while fetching from %d' % data['start'], 'triumph_log.txt')
            dump_data = {'level': 1, 'time': cm.format_time(), 'data': {'url': url}, 'brand_id': brand_id}
            cm.dump(dump_data)
            return []

        idx = 0
        if len(raw_list) < data['rows'] and start + len(raw_list) < tot:
            cm.dump('Cooling down...', 'triumph_log.txt')
            time.sleep(5)
            continue

        for v in raw_list:
            entry = cm.init_store_entry(brand_id, brandname_e, brandname_c)
            cm.update_entry(entry, {cm.store_type: v['class'],
                                    cm.zip_code: v['zip'], cm.tel: v['phone'], cm.fax: v['fax'],
                                    cm.url: v['web'], cm.email: v['email'], cm.hours: v['opening_hours']})
            entry[cm.name_e] = cm.reformat_addr(v['name'])

            entry[cm.city_e], tmp = cm.extract_city(v['city'])
            if not re.search(ur'\d', entry[cm.zip_code]) and tmp != '':
                entry[cm.zip_code] = tmp

            if v['location'] != '':
                terms = v['location'].split(',')
                cm.update_entry(entry, {cm.lat: string.atof(terms[0]), cm.lng: string.atof(terms[1])})
            addr = v['address']
            if v['address2'] != '':
                addr += ', ' + v['address2']
            entry[cm.addr_e] = cm.reformat_addr(addr)
            ret = gs.look_up(v['country'], 1)
            if ret is not None:
                entry[cm.country_e] = ret['name_e']
            else:
                cm.dump('Error in looking up country %s' % v['country'], 'triumph_log.txt')
            gs.field_sense(entry)

            cm.dump('(%s / %d) Found store at %d: %s, %s (%s, %s, %s)' % (
                brandname_e, brand_id, start + idx, entry[cm.name_e], entry[cm.addr_e], entry[cm.city_e],
                entry[cm.country_e],
                entry[cm.continent_e]), 'triumph_log.txt')
            store_list.append(entry)
            db.insert_record(entry, 'stores')
            idx += 1

        if tot - start <= len(raw_list):
            break
        else:
            start += len(raw_list)

    return store_list