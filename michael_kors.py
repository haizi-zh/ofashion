# coding=utf-8
import json
import string
import re
import common as cm
import geosense as gs

__author__ = 'Zephyre'

db = None


def fetch(level=1, data=None, user='root', passwd=''):
    # Walk from the root node, where level == 1.
    if data is None:
        data = {'url': 'http://cms.destinationkors.com/store/get',
                'brand_id': 10259, 'brandname_e': u'Michael Kors', 'brandname_c': u'迈克.柯尔'}

    type_desc = ['Collection Boutique', 'Lifestyle', 'Outlet']

    global db
    db = cm.StoresDb()
    db.connect_db(user=user, passwd=passwd)
    db.execute(u'DELETE FROM %s WHERE brand_id=%d' % ('stores', data['brand_id']))

    store_list = []
    url = data['url']
    try:
        html = cm.get_data(url).decode('unicode_escape')
        start = html.find('[')
        if start == -1:
            return []
        js = json.loads(html[start:])
    except Exception:
        print 'Error occured: %s' % url
        dump_data = {'level': 0, 'time': cm.format_time(), 'data': {'url': url}, 'brand_id': data['brand_id']}
        cm.dump(dump_data)
        return []

    for s in js:
        entry = cm.init_store_entry(data['brand_id'], data['brandname_e'], data['brandname_c'])
        entry[cm.store_type] = type_desc[string.atoi(s['store_type']) - 1]
        name = s['name'].strip()
        if s['name2'].strip() != '':
            name += ', ' + s['name2'].strip()
        entry[cm.name_e] = name

        addr = []
        for i in xrange(3):
            tmp = s['address%d' % (i + 1)].strip()
            if tmp != '':
                addr.append(tmp)
        entry[cm.addr_e] = ', '.join(addr)
        entry[cm.city_e] = s['city']

        country = s['country']
        ret =  gs.look_up(country, 1)
        if ret is not None:
            country=ret['name_e']
        entry[cm.country_e] = country

        state = s['state'].strip().upper()
        if country=='UNITED STATES' and state != '':
            ret = gs.look_up(state, 2)
            if ret is not None:
                entry[cm.province_e] = ret['name_e']
        else:
            entry[cm.province_e] = state

        entry[cm.zip_code] = s['zip']
        entry[cm.tel] = s['phone']
        entry[cm.hours] = s['hours']
        entry[cm.lat] = string.atof(s['latitude'])
        entry[cm.lng] = string.atof(s['longitude'])
        gs.field_sense(entry)

        print '(%s / %d) Found store: %s, %s (%s, %s)' % (data['brandname_e'], data['brand_id'],
                                                          entry[cm.name_e], entry[cm.addr_e], entry[cm.country_e],
                                                          entry[cm.continent_e])
        store_list.append(entry)
        db.insert_record(entry, 'stores')

    db.disconnect_db()
    return store_list