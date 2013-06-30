# coding=utf-8
import json
import string
import re
import urllib
import common as cm
import geosense as gs
import xml.etree.ElementTree as et

__author__ = 'Zephyre'

db = None
log_name = 'dolce_gabbana_log.txt'


def fetch_countries(data):
    param = {'lang': 'en', 'entity': 'Boutique', 'select': 'Country_ID|Country_Nation'}
    tail = '&'.join(('%s=%s' % (item[0], urllib.quote(item[1]))) for item in
                    param.items()) + '&condition=brandDolceeGabbanaDolceeGabbanavalue:True'
    url = '%s?%s' % (data['url'], tail)
    try:
        body = cm.get_data(url)
    except Exception, e:
        cm.dump('Error in fetching countries: %s' % url, log_name)
        return ()

    tree = et.fromstring(body.encode('utf-8'))
    results = []
    for item in tree.iter('Item'):
        d = data.copy()
        d['country_id'] = string.atoi(item.attrib['Country_ID'])
        d['country'] = cm.html2plain(item.getiterator('value')[0].text).strip().upper()
        results.append(d)
    return tuple(results)


def fetch_stores(data):
    param = {'lang': 'en', 'entity': 'Boutique'}
    tail = '&'.join(('%s=%s' % (item[0], urllib.quote(item[1]))) for item in
                    param.items()) + '&condition=brandDolceeGabbanaDolceeGabbanavalue:True%%7CCity:%d' % data[
               'city_id']
    url = '%s?%s' % (data['url'], tail)
    try:
        body = cm.get_data(url)
    except Exception, e:
        cm.dump('Error in fetching cities: %s' % url, log_name)
        return ()

    tree = et.fromstring(body.encode('utf-8'))
    store_list = []
    for s in tree.getchildren():
        entry = cm.init_store_entry(data['brand_id'], data['brandname_e'], data['brandname_c'])
        entry[cm.country_e] = data['country']

        for ele in s.getchildren():
            if ele.tag == 'name':
                val = ele.getiterator('value')[0].text
                entry[cm.name_e] = cm.reformat_addr(val).strip() if val else ''
            elif ele.tag == 'address':
                val = ele.getiterator('value')[0].text
                entry[cm.addr_e] = cm.reformat_addr(val).strip() if val else ''
            elif ele.tag == 'zipcode':
                val = ele.getiterator('value')[0].text
                entry[cm.zip_code] = val.strip() if val else ''
            elif ele.tag == 'coordinateslat':
                try:
                    val = ele.getiterator('value')[0].text
                    entry[cm.lat] = string.atof(val) if val and val != '' else ''
                except (ValueError, KeyError, TypeError) as e:
                    cm.dump('Error in fetching lat: %s' % str(e), log_name)
            elif ele.tag == 'coordinateslong':
                try:
                    val = ele.getiterator('value')[0].text
                    entry[cm.lng] = string.atof(val) if val and val != '' else ''
                except (ValueError, KeyError, TypeError) as e:
                    cm.dump('Error in fetching lng: %s' % str(e), log_name)
            elif ele.tag == 'city':
                val = ele.getiterator('value')[0].text
                entry[cm.city_e] = cm.html2plain(val).strip().upper() if val else ''
            elif ele.tag == 'province':
                val = ele.getiterator('value')[0].text
                entry[cm.province_e] = cm.html2plain(val).strip().upper() if val else ''
            elif ele.tag == 'phone':
                val = ele.getiterator('value')[0].text
                entry[cm.tel] = val.strip() if val else ''
            elif ele.tag == 'fax':
                val = ele.getiterator('value')[0].text
                entry[cm.fax] = val.strip() if val else ''
            elif ele.tag == 'email':
                val = ele.getiterator('value')[0].text
                entry[cm.email] = val.strip() if val else ''

        gs.field_sense(entry)
        ret = gs.addr_sense(entry[cm.addr_e], entry[cm.country_e])
        if ret[1] is not None and entry[cm.province_e] == '':
            entry[cm.province_e] = ret[1]
        if ret[2] is not None and entry[cm.city_e] == '':
            entry[cm.city_e] = ret[2]
        gs.field_sense(entry)

        cm.dump('(%s / %d) Found store: %s, %s (%s, %s)' % (data['brandname_e'], data['brand_id'],
                                                            entry[cm.name_e], entry[cm.addr_e], entry[cm.country_e],
                                                            entry[cm.continent_e]), log_name)
        db.insert_record(entry, 'stores')
        store_list.append(entry)

    return tuple(store_list)


def fetch_cities(data):
    param = {'lang': 'en', 'entity': 'Boutique', 'select': 'City_ID|city'}
    tail = '&'.join(('%s=%s' % (item[0], urllib.quote(item[1]))) for item in
                    param.items()) + '&condition=brandDolceeGabbanaDolceeGabbanavalue:True%%7CCountry:%d' % data[
               'country_id']
    url = '%s?%s' % (data['url'], tail)
    try:
        body = cm.get_data(url)
    except Exception, e:
        cm.dump('Error in fetching cities: %s' % url, log_name)
        return ()

    tree = et.fromstring(body.encode('utf-8'))
    results = []
    for item in tree.iter('Item'):
        d = data.copy()
        d['city_id'] = string.atoi(item.attrib['City_ID'])
        d['city'] = cm.html2plain(item.getiterator('value')[0].text).strip().upper()
        results.append(d)
    return tuple(results)


def fetch(level=1, data=None, user='root', passwd=''):
    def func(data, level):
        """
        :param data:
        :param level: 0：国家；1：城市；2：商店列表
        """
        if level == 0:
            # 国家列表
            return [{'func': lambda data: func(data, level + 1), 'data': s} for s in fetch_countries(data)]
        if level == 1:
            # 城市列表
            return [{'func': lambda data: func(data, level + 1), 'data': s} for s in fetch_cities(data)]
        if level == 2:
            # 商店
            return [{'func': None, 'data': s} for s in fetch_stores(data)]
        else:
            return ()

    # Walk from the root node, where level == 1.
    if data is None:
        data = {'url': 'http://www.dolcegabbana.com/dgstorelocator/serv/search',
                'brand_id': 10109, 'brandname_e': u'Dolce & Gabbana', 'brandname_c': u'杜嘉班纳'}

    global db
    db = cm.StoresDb()
    db.connect_db(user=user, passwd=passwd)
    db.execute(u'DELETE FROM %s WHERE brand_id=%d' % ('stores', data['brand_id']))

    results = cm.walk_tree({'func': lambda data: func(data, 0), 'data': data})
    db.disconnect_db()
    cm.dump('Done!', log_name)

    return results


