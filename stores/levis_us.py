# coding=utf-8
import json
import string
import re
import traceback

from pyquery import PyQuery as pq

import common as cm
from stores import geosense as gs


__author__ = 'Zephyre'

db = None
log_name = 'levis_log.txt'


def fetch_countries(data):
    url = data['url']
    try:
        body = cm.get_data(url)
    except Exception, e:
        cm.dump('Error in fetching countries: %s' % url, log_name)
        return ()


def fetch_stores(data):
    url = data['url']
    param = {'operation': 'coSearch', 'numResults': 999999, 'mnlt': -89, 'mxlt': 89, 'mnln': -179, 'mxln': 179,
             'token': 'LEVI', 'heavy': 'true'}
    try:
        body = cm.get_data(url, param)
    except Exception, e:
        cm.dump('Error in fetching stores: %s, %s' % (url, param), log_name)
        return ()

    store_list = []
    for s in json.loads(body)['RESULTS']:
        s = s['store']
        entry = cm.init_store_entry(data['brand_id'], data['brandname_e'], data['brandname_c'])

        entry[cm.country_e] = s['countryCode']
        entry[cm.city_e] = cm.extract_city(s['city'])[0]
        entry[cm.province_e] = s['stateCode']

        addr_list = []
        if 'address1' in s:
            val = s['address1']
            val = cm.html2plain(val).strip() if val else ''
            if val != '':
                addr_list.append(val)
        if 'address2' in s:
            val = s['address2']
            val = cm.html2plain(val).strip() if val else ''
            if val != '':
                addr_list.append(val)
        entry[cm.addr_e] = ', '.join(addr_list)

        try:
            val = s['latitude']
            entry[cm.lat] = string.atof(val) if val and val != '' else ''
        except (ValueError, KeyError, TypeError) as e:
            cm.dump('Error in fetching lat: %s' % str(e), log_name)
        try:
            val = s['longitude']
            entry[cm.lng] = string.atof(val) if val and val != '' else ''
        except (ValueError, KeyError, TypeError) as e:
            cm.dump('Error in fetching lng: %s' % str(e), log_name)

        entry[cm.tel] = s['phoneNumber'].strip() if s['phoneNumber'] else ''
        entry[cm.tel] = s['postalCode'].strip() if s['postalCode'] else ''
        entry[cm.name_e] = s['locationName'].strip() if s['locationName'] else ''
        entry[cm.hours] = cm.reformat_addr(s['storeHours']) if s['storeHours'] else ''

        val = s['storeType']
        entry[cm.store_class] = val['typeName'] if val else ''

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


def fetch_jp(data):
    url = 'http://www.levi.jp/storefinder/hokuriku/'
    data = data.copy()
    data['country'] = u'JAPAN'
    try:
        body = cm.get_data(url)
    except Exception, e:
        cm.dump('Error in fetching states: %s' % url, log_name)
        return ()

    store_list = []
    for region in pq(body)("#area li a"):
        region_url = '%s%s' % (url, region.attrib['href'])
        try:
            region_body = cm.get_data(region_url)
        except Exception, e:
            cm.dump('Error in fetching store list: %s' % url, log_name)
            return ()

        for item in pq(region_body)('ul.store li a'):
            try:
                store_url = '%s%s' % (region_url, item.attrib['href'])
                try:
                    store = pq(cm.get_data(store_url))
                except Exception, e:
                    cm.dump('Error in fetching store details: %s' % url, log_name)
                    return ()

                entry = cm.init_store_entry(data['brand_id'], data['brandname_e'], data['brandname_c'])
                entry[cm.country_e] = data['country']

                tmp = store('p.mapLink a[href!=""]')
                if len(tmp) > 0:
                    map_link = tmp[0].attrib['href']
                    m = re.search(ur'll=(-?\d+\.\d+),(-?\d+\.\d+)', map_link)
                    if m:
                        entry[cm.lat] = string.atof(m.group(1))
                        entry[cm.lng] = string.atof(m.group(2))
                    else:
                        for item in pq(url=map_link)('meta[content!=""]'):
                            m = re.search(ur'maps.+ll=(-?\d+\.\d+),(-?\d+\.\d+)', item.attrib['content'])
                            if m:
                                entry[cm.lat] = string.atof(m.group(1))
                                entry[cm.lng] = string.atof(m.group(2))

                tmp = store('#storeDetail h1')
                entry[cm.name_e] = cm.html2plain(tmp[0].text).strip() if len(tmp) > 0 and tmp.text else ''

                idx_map = {}
                idx = 0
                for item in store('dl.data dt img[alt!=""]'):
                    if 'address' in item.attrib['alt'].lower():
                        idx_map['address'] = idx
                    elif 'open' in item.attrib['alt'].lower():
                        idx_map['hours'] = idx
                    idx += 1

                tmp_list = [cm.reformat_addr(unicode(pq(tmp))) for tmp in store('dl.data dd')]
                if 'address' in idx_map:
                    addr_sub = tmp_list[idx_map['address']]
                    pat_tel = re.compile(ur'(tel|telephone|phone)\s*[:\.]?\s*(.+)')
                    m = re.search(pat_tel, addr_sub)
                    if m:
                        entry[cm.tel] = m.group(2)
                        addr_sub = re.sub(pat_tel, '', addr_sub)
                    entry[cm.addr_e] = cm.reformat_addr(addr_sub)
                if 'hours' in idx_map:
                    entry[cm.hours] = cm.reformat_addr(tmp_list[idx_map['hours']])

                addr = entry[cm.addr_e]
                pat = re.compile(ur'(.+?)県')
                m = re.search(pat, addr)
                if m:
                    entry[cm.province_e] = cm.html2plain(m.group(1)).strip().upper()
                    addr = re.sub(pat, '', addr)
                pat = re.compile(ur'(.+?)市')
                m = re.search(pat, addr)
                if m:
                    entry[cm.city_e] = cm.html2plain(m.group(1)).strip().upper()

                gs.field_sense(entry)
                ret = gs.addr_sense(entry[cm.addr_e])
                if ret[0] is not None and entry[cm.country_e] == '':
                    entry[cm.country_e] = ret[0]
                if ret[1] is not None and entry[cm.province_e] == '':
                    entry[cm.province_e] = ret[1]
                if ret[2] is not None and entry[cm.city_e] == '':
                    entry[cm.city_e] = ret[2]
                gs.field_sense(entry)

                entry[cm.city_e] = cm.extract_city(entry[cm.city_e])[0]
                cm.dump('(%s / %d) Found store: %s, %s (%s, %s)' % (data['brandname_e'], data['brand_id'],
                                                                    entry[cm.name_e], entry[cm.addr_e],
                                                                    entry[cm.country_e],
                                                                    entry[cm.continent_e]), log_name)
                db.insert_record(entry, 'stores')
                store_list.append(entry)
            except (IndexError, TypeError) as e:
                print traceback.format_exc()
                continue
    return tuple(store_list)


def fetch_hk(data):
    loc_list = ('Hong Kong', 'Kowloon', 'Macau', 'New Territories')
    url = 'http://levi.com.hk/hk/storelocator'
    store_list = []
    for loc in loc_list:
        param = {'loc': loc}
        try:
            body = cm.get_data(url, param)
        except Exception, e:
            cm.dump('Error in fetching stores: %s' % param, log_name)
            continue

        start = body.find(ur'<div id="addWrapper">')
        if start == -1:
            cm.dump('Error in fetching stores: %s' % param, log_name)
            continue
        sub = cm.extract_closure(body[start:], ur'<ul>', ur'</ul>')[0]
        for s in re.findall(ur'<li>(.+?)</li>', sub, re.S):
            entry = cm.init_store_entry(data['brand_id'], data['brandname_e'], data['brandname_c'])
            entry[cm.country_e] = 'MACAU' if loc == 'Macau' else 'HONG KONG'
            entry[cm.city_e] = entry[cm.country_e]

            m = re.search(ur'<div id="addStore">([^<>]+)', s)
            entry[cm.addr_e] = cm.html2plain(m.group(1)) if m else ''

            m = re.search(ur'<div id="addAddress">([^<>]+)', s)
            tmp = cm.html2plain(m.group(1))
            pat = re.compile(ur'business hours?\s*[:\.]?\s*', re.I)
            if re.search(pat, tmp):
                entry[cm.hours] = re.sub(pat, '', tmp).strip()

            m = re.search(ur'<div id="addPhone">([^<>]+)', s)
            tmp = cm.html2plain(m.group(1))
            pat = re.compile(ur'(tel|phone|telephone)?\s*[:\.]?\s*', re.I)
            if re.search(pat, tmp):
                entry[cm.tel] = re.sub(pat, '', tmp).strip()

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

    return store_list


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
        data = {'data_url': 'xxxxxxxxxx',
                'url': 'http://us.levi.com/storeLocServ',
                'brand_id': 10215, 'brandname_e': u"Levi's", 'brandname_c': u"Levi's"}

    global db
    db = cm.StoresDb()
    db.connect_db(user=user, passwd=passwd)
    # db.execute(u'DELETE FROM %s WHERE brand_id=%d' % ('stores', data['brand_id']))

    # results = cm.walk_tree({'func': lambda data: func(data, 0), 'data': data})
    # results.extend(fetch_hk(data))

    results = fetch_jp(data)

    db.disconnect_db()
    cm.dump('Done!', log_name)

    return results


