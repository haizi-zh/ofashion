# coding=utf-8
import json
import string
import re
import time
import common as cm
import geosense as gs

__author__ = 'Zephyre'

db = None
url = 'http://storelocator.triumph.com/solr/api/v1/stores'
# data = '?json.wrf=jQuery18304032145428952145_1370845106875&q=*%3A*&pt=48.856614%2C2.3522219000000177&d=50&start=0&rows=30&_=1370848037839'
brand_id = 10359
brandname_e = u'Triumph'
brandname_c = u'黛安芬'


def fetch(level=1, data=None, user='root', passwd=''):
    tot = 0
    start = 0
    store_list = []
    data = {'q': '*:*', 'pt': '0,0', 'd': 50000, 'start': 0, 'rows': 100}

    db = cm.StoresDb()
    db.connect_db(user=user, passwd=passwd)
    db.execute(u'DELETE FROM %s WHERE brand_id=%d' % ('stores', brand_id))

    while True:
        print 'Fetching from %d' % start
        try:
            data['start'] = start
            html = cm.get_data(url, data)
            raw_list = json.loads(html)
            if tot==0:
                tot = raw_list['response']['numFound']
            raw_list = raw_list['response']['docs']
            # html = cm.post_data(url, {'country': -1, 'city': -1, 'recordit': -1})
        except Exception:
            print 'Error occured: %s' % url
            dump_data = {'level': 1, 'time': cm.format_time(), 'data': {'url': url}, 'brand_id': brand_id}
            cm.dump(dump_data)
            return []

        idx = 0
        if len(raw_list) < data['rows'] and start + len(raw_list) < tot:
            print 'Cooling down...'
            time.sleep(5)
            continue

        for v in raw_list:
            entry = cm.init_store_entry(brand_id, brandname_e, brandname_c)
            cm.update_entry(entry, {cm.store_type: v['class'], cm.name_e: v['name'], cm.city_e: v['city'],
                                    cm.zip_code: v['zip'], cm.tel: v['phone'], cm.fax: v['fax'],
                                    cm.url: v['web'], cm.email: v['email'], cm.hours: v['opening_hours']})
            if v['location'] != '':
                terms = v['location'].split(',')
                cm.update_entry(entry, {cm.lat: string.atof(terms[0]), cm.lng: string.atof(terms[1])})
            addr = v['address']
            if v['address2'] != '':
                addr += ', ' + v['address2']
            entry[cm.addr_e] = addr
            ret = gs.look_up(v['country'], 1)
            if ret is not None:
                entry[cm.country_e] = ret['name_e']
            gs.field_sense(entry)

            print '(%s / %d) Found store at %d: %s, %s (%s, %s)' % (
                brandname_e, brand_id, start + idx, entry[cm.name_e], entry[cm.addr_e], entry[cm.country_e],
                entry[cm.continent_e])
            store_list.append(entry)
            db.insert_record(entry, 'stores')
            idx += 1

        if tot - start <= len(raw_list):
            break
        else:
            start += len(raw_list)

    return store_list