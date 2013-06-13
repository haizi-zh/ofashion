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

    page = 1
    tot = -1
    tot_page = -1
    store_ids = set([])
    store_list = []

    f = open('err_log_%s.log' % data['brandname_e'], 'w')

    while True:
        msg = 'Fetching page %d...' % page
        print msg
        f.write('%s\n' % msg)
        try:
            html = cm.get_data(url, {'brand': 'adidas', 'geoengine': 'google', 'method': 'get',
                                     'category': 'store', 'latlng': '31.22434895,121.47675279999999, 10000',
                                     'page': '%d' % page, 'pagesize': 400,
                                     'fields': 'name,street1,street2,addressline,buildingname,postal_code,city,'
                                               'state,store_owner,country,storetype,longitude_google,'
                                               'latitude_google,store_owner,state,performance,brand_store,'
                                               'factory_outlet,originals,neo_label,y3,slvr,children,woman,'
                                               'footwear,football,basketball,outdoor,porsche_design,miadidas,'
                                               'miteam,stella_mccartney,eyewear,micoach,opening_ceremony',
                                     'format': 'json', 'storetype': ''})
        except Exception:
            msg = 'Error occured: %s' % url
            print msg
            f.write('%s\n' % msg)
            dump_data = {'level': 0, 'time': cm.format_time(), 'data': {'url': url}, 'brand_id': data['brand_id']}
            cm.dump(dump_data)
            page += 1
            if page > tot_page:
                break
            else:
                continue

        try:
            start = html.find('{')
            if start != -1:
                html = html[start:]
            raw = json.loads(html)['wsResponse']
            if tot == -1:
                tot = string.atoi(raw['results'])
                tot_page = (tot - 1) / 500 + 1
            raw = raw['result']

            def addr_func(addr_list, addr_map, key):
                if key in addr_map:
                    addr_list.append(addr_map[key].strip())

            for s in raw:
                try:
                    entry = cm.init_store_entry(data['brand_id'], data['brandname_e'], data['brandname_c'])
                    if s['id'] in store_ids:
                        continue
                    store_ids.add(s['id'])
                    entry[cm.name_e] = s['name']

                    addr_list = []
                    map(lambda key: addr_func(addr_list, s, key), ['addressline', 'buildingname', 'street1',
                                                                   'street2'])
                    entry[cm.addr_e] = ', '.join(addr_list)
                    entry[cm.city_e] = s['city'].split('-')[0].strip().upper()
                    entry[cm.country_e] = s['country'].strip().upper()
                    entry[cm.store_type] = s['storetype']
                    entry[cm.lat] = string.atof(s['latitude_google'])
                    entry[cm.lng] = string.atof(s['longitude_google'])
                    entry[cm.store_class] = 'adidas'

                    gs.field_sense(entry)
                    msg = '(%s / %d) Found store: %s, %s (%s, %s)' % (data['brandname_e'], data['brand_id'],
                                                                      entry[cm.name_e], entry[cm.addr_e],
                                                                      entry[cm.country_e],
                                                                      entry[cm.continent_e])
                    print msg
                    f.write('%s\n' % msg.encode('utf-8'))
                    store_list.append(entry)
                    db.insert_record(entry, 'stores')
                except Exception, e:
                    msg = 'Error processing. Reason: %s, content: %s' % (str(e), s)
                    print msg
                    f.write('%s\n' % msg.encode('utf-8'))
        except Exception, e:
            msg = 'Error processing page %d, reason: %s' % (page, str(e))
            print msg
            f.write('%s\n' % msg)
        finally:
            page += 1
            if page > tot_page:
                break
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
        data = {'url': 'http://placesws.adidas-group.com/API/search',
                'brand_id': 10004, 'brandname_e': u'Adidas SLVR Label', 'brandname_c': u'阿迪达斯'}

    global db
    db = cm.StoresDb()
    db.connect_db(user=user, passwd=passwd)
    db.execute(u'DELETE FROM %s WHERE brand_id=%d' % ('stores', data['brand_id']))

    results = cm.walk_tree({'func': lambda data: func(data, 0), 'data': data})
    db.disconnect_db()

    return results