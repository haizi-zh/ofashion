# coding=utf-8
import json
import string
import re
import common as cm
import geosense as gs

__author__ = 'Zephyre'

db = None


def fetch_stores(data):
    url = data['home_url']
    try:
        body = cm.get_data(url)
    except Exception:
        print 'Error occured: %s' % url
        dump_data = {'level': 0, 'time': cm.format_time(), 'data': {'url': url}, 'brand_id': data['brand_id']}
        cm.dump(dump_data)
        return []

    raw = json.loads(body)['results']
    store_list = []
    for key in raw:
        store = raw[key]
        entry = cm.init_store_entry(data['brand_id'], data['brandname_e'], data['brandname_c'])
        entry[cm.name_e] = store['post_title']
        entry[cm.url] = store['post_permalink'].replace(u'\\', '')
        entry[cm.country_e] = store['country'].strip().upper()
        entry[cm.city_e] = store['city'].strip().upper()

        if '_yoox_store_latlong' in store:
            m = re.findall(ur'-?\d+\.\d+', store['_yoox_store_latlong'])
            if len(m) == 2:
                entry[cm.lat] = string.atof(m[0])
                entry[cm.lng] = string.atof(m[1])

        if 'store_phone' in store:
            entry[cm.tel] = store['store_phone'].replace('P:', '').replace('T:', '') \
                .replace('P', '').replace('T', '').strip()
        if 'store_email' in store:
            entry[cm.email] = store['store_email']
        if 'store_fax' in store:
            entry[cm.fax] = store['store_fax'].replace('F:', '').replace('F', '').strip()
        if 'store_hours' in store:
            entry[cm.hours] = cm.reformat_addr(store['store_hours'])
        if 'store_address' in store:
            entry[cm.addr_e] = cm.reformat_addr(store['store_address'])
        if 'women' in store and 'men' in store:
            entry[cm.store_type] = 'Women: %s, men: %s' % (', '.join(store['women']), ', '.join(store['men']))

        gs.field_sense(entry)
        print '(%s / %d) Found store: %s, %s (%s, %s)' % (data['brandname_e'], data['brand_id'],
                                                          entry[cm.name_e], entry[cm.addr_e], entry[cm.country_e],
                                                          entry[cm.continent_e])
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
            # 商店列表
            return [{'func': None, 'data': s} for s in fetch_stores(data)]
        else:
            return []

    # Walk from the root node, where level == 1.
    if data is None:
        data = {
            'home_url': 'http://www.balenciaga.com/experience/us?yoox_storelocator_action=true&action=yoox_storelocator_get_stores',
            'brand_id': 10029, 'brandname_e': u'Balenciaga', 'brandname_c': u'巴黎世家'}

    global db
    db = cm.StoresDb()
    db.connect_db(user=user, passwd=passwd)
    db.execute(u'DELETE FROM %s WHERE brand_id=%d' % ('stores', data['brand_id']))

    results = cm.walk_tree({'func': lambda data: func(data, 0), 'data': data})
    db.disconnect_db()

    return results
