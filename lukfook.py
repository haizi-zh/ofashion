# coding=utf-8
import json
import string
import re
import common as cm
import geosense as gs

__author__ = 'Zephyre'

db = None


def fetch_stores(data):
    url = data['url']
    try:
        html = cm.get_data(url)
    except Exception:
        print 'Error occured: %s' % url
        dump_data = {'level': 0, 'time': cm.format_time(), 'data': {'url': url}, 'brand_id': data['brand_id']}
        cm.dump(dump_data)
        return []

    store_list = []
    for m in re.findall(ur'var markerContent\s*?=\s*?"(.+?)".+?'
                        ur'createMarker\(.+?new google.maps.LatLng\((-?\d+\.\d+),(-?\d+\.\d+)\)', html, re.S):
        entry = cm.init_store_entry(data['brand_id'], data['brandname_e'], data['brandname_c'])
        lat, lng = map(string.atof, [m[1], m[2]])
        cm.update_entry(entry, {cm.lat: lat, cm.lng: lng})

        sub = m[0].strip()
        m1 = re.search(ur'<b>(.+?)</b>', sub)
        if m1 is None:
            continue
        entry[cm.name_c] = m1.group(1)
        sub = sub.replace(m1.group(0), '')
        m1=re.search(ur'聯系電話(?::|：)(.+?)<', sub)
        if m1 is not None:
            entry[cm.tel]=m1.group(1)
            sub=sub.replace(m1.group(0), '<')
        sub = re.sub(ur'<img\b.*?/>', '', sub)
        entry[cm.addr_c] = cm.reformat_addr(sub)

        print '(%s/%d) Found store: %s, %s (%s, %s)' % (data['brandname_e'], data['brand_id'],
                                                        entry[cm.name_c], entry[cm.addr_e], entry[cm.country_e],
                                                        entry[cm.continent_e])
        store_list.append(entry)
        db.insert_record(entry, 'stores')

    return store_list


def fetch(level=0, data=None, user='root', passwd=''):
    if data is None:
        data = {'url': 'http://www1.lukfook.com.hk/googlemaps.asp', 'brand_id': 10227,
                'brandname_e': u'LUKFOOK JEWELLERY', 'brandname_c': u'六福珠宝'}

    global db
    db = cm.StoresDb()
    db.connect_db(user=user, passwd=passwd)
    db.execute(u'DELETE FROM %s WHERE brand_id=%d' % ('stores', data['brand_id']))

    results = fetch_stores(data)

    db.disconnect_db()

    return results
