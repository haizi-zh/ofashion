# coding=utf-8
import json
import string
import re
import traceback
import urllib
import urlparse
import common as cm
import geosense as gs
from pyquery import PyQuery as pq
import xml.etree.ElementTree as et

__author__ = 'Zephyre'

db = None
log_name = 'louis_vuitton_log.txt'


def fetch_stores(data):
    url = data['url']
    try:
        body = cm.get_data(url, client='iPad')
    except Exception, e:
        cm.dump('Error in fetching stores: %s' % url, log_name)
        return ()

    store_list = []
    for s in json.loads(body)['stores']['storeTab']:
        try:
            entry = cm.init_store_entry(data['brand_id'], data['brandname_e'], data['brandname_c'])
            entry[cm.country_e] = cm.html2plain(s['country']).strip().upper() if s['country'] else ''
            entry[cm.city_e] = cm.extract_city(s['city'])[0] if s['city'] else ''
            entry[cm.name_e] = cm.html2plain(s['name']).strip().upper() if s['name'] else ''
            entry[cm.province_e] = cm.html2plain(s['state']).strip().upper() if s['state'] else ''
            entry[cm.addr_e] = cm.html2plain(s['street']) if s['street'] else ''
            entry[cm.zip_code] = s['zip'] if s['zip'] else ''

            try:
                entry[cm.lat] = string.atof(s['latitude']) if s['latitude'] != '' else ''
            except (ValueError, KeyError, TypeError) as e:
                cm.dump('Error in fetching lat: %s' % str(e), log_name)
            try:
                entry[cm.lng] = string.atof(s['longitude']) if s['longitude'] != '' else ''
            except (ValueError, KeyError, TypeError) as e:
                cm.dump('Error in fetching lng: %s' % str(e), log_name)

            entry[cm.tel] = s['phone'] if s['phone'] else ''
            entry[cm.url] = (data['host'] + s['storeDetailUrl']) if s['storeDetailUrl'] else ''

            hour_list = []
            try:
                body = cm.get_data(entry[cm.url], client='iPad')
                html = pq(body)
                for sub in (pq(tmp) for tmp in html('table.storeDetailed-horaires-content tr')):
                    tmp = sub('td[class!="hours"]')
                    if len(tmp) == 0:
                        continue
                    val1 = cm.reformat_addr(tmp[0].text).strip()
                    tmp = sub('td.hours')
                    if len(tmp) == 0:
                        continue
                    val2 = cm.reformat_addr(tmp[0].text).strip()
                    if val1 == '' or val2 == '':
                        continue
                    hour_list.append('%s %s' % (val1, val2))

                tmp = html('div.storeDetailed-horaires-content')
                if len(tmp) > 0:
                    hour_list.append('Closing days: ' + cm.reformat_addr(tmp[0].text).strip())
                entry[cm.hours] = ', '.join(hour_list)
            except Exception as e:
                print traceback.format_exc()

            entry[cm.store_type] = ', '.join(item.replace('_', ' ') for item in s['categories']) if s[
                'categories'] else ''
            val = {'callCenter': 'Call center', 'flagship': 'Flagship'}
            class_list = []
            for key in val:
                if s[key] == 'true':
                    class_list.append(val[key])
            entry[cm.store_class] = ', '.join(class_list)

            gs.field_sense(entry)
            ret = gs.addr_sense(entry[cm.addr_e], entry[cm.country_e])
            if ret[1] is not None and entry[cm.province_e] == '':
                entry[cm.province_e] = ret[1]
            if ret[2] is not None and entry[cm.city_e] == '':
                entry[cm.city_e] = ret[2]
            gs.field_sense(entry)

            cm.dump('(%s / %d) Found store: %s, %s (%s, %s, %s)' % (data['brandname_e'], data['brand_id'],
                                                                    entry[cm.name_e], entry[cm.addr_e],
                                                                    entry[cm.city_e],
                                                                    entry[cm.country_e], entry[cm.continent_e]),
                    log_name)
            db.insert_record(entry, 'stores')
            store_list.append(entry)
        except (IndexError, TypeError) as e:
            print(traceback.format_exc())
            continue

    return tuple(store_list)


def fetch(level=1, data=None, user='root', passwd=''):
    def func(data, level):
        """
        :param data:
        :param level: 0：国家；1：城市；2：商店列表
        """
        if level == 0:
            # 商店
            return [{'func': None, 'data': s} for s in fetch_stores(data)]
        else:
            return ()

    # Walk from the root node, where level == 1.
    if data is None:
        data = {'host': 'http://m.louisvuitton.eu',
                'url': 'http://a.louisvuitton.com/mobile/ajax/getStoreJson.jsp?storeLang=eng_E1&module=storelocator&category=index&latitudeA=89&longitudeA=-179&latitudeB=-89&longitudeB=179&doClosestSearch=false&zoomLevel=3&country=undefined&categories=',
                'brand_id': 10226, 'brandname_e': u'Louis Vuitton', 'brandname_c': u'路易威登'}

    global db
    db = cm.StoresDb()
    db.connect_db(user=user, passwd=passwd)
    db.execute(u'DELETE FROM %s WHERE brand_id=%d' % ('stores', data['brand_id']))

    results = cm.walk_tree({'func': lambda data: func(data, 0), 'data': data})
    db.disconnect_db()
    cm.dump('Done!', log_name)

    return results


