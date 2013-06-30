# coding=utf-8
import json
import string
import re
import common as cm
import geosense as gs
import xml.etree.ElementTree as et

__author__ = 'Zephyre'

db = None
log_name = 'columbia_log.txt'
store_map = {}


def gen_city_map():
    with open('city_lite.dat', 'r') as f:
        sub = f.readlines()
    return json.loads(sub[0])


def fetch_countries(data):
    url = data['url']
    try:
        body, data['cookie'] = cm.get_data_cookie(url)
    except Exception, e:
        cm.dump('Error in fetching AppKey: %s' % url, log_name)
        return ()
    app_key = data['cookie']['AppKey']
    url = data['data_url']
    param = {
        'xml_request': '<request><appkey>%s</appkey><formdata id="getlist"><objectname>Account::Country</objectname><where></where></formdata></request>' % app_key}
    try:
        body, data['cookie'] = cm.get_data_cookie(url, param, cookie=data['cookie'])
    except Exception, e:
        cm.dump('Error in fetching countries: %s, %s' % (url, param), log_name)
        return ()

    tree = et.fromstring(body.encode('utf-8'))
    results = []
    for ele in tree.iter('account_country'):
        d = data.copy()
        val = ele.getiterator('name')[0].text
        if not val:
            continue
        d['country_code'] = val.strip()
        results.append(d)
    return tuple(results)


def fetch_cities(data):
    ret = gs.look_up(data['country_code'], 1)
    if ret is None:
        return ()

    country = ret['name_e']
    city_map = data['city_map']
    results = []
    if country in city_map:
        for city in city_map[country]:
            d = data.copy()
            d['country'] = country
            d['city'] = city
            d['city_lat'] = city_map[country][city]['lat']
            d['city_lng'] = city_map[country][city]['lng']
            results.append(d)
    return tuple(results)


def fetch_stores(data):
    url = data['data_url']
    c_code = data['country_code']
    param = {
        'xml_request': '<request><appkey>%s</appkey><formdata id="locatorsearch"><order>rank,_distance</order><geolocs><geoloc><longitude>%f</longitude><latitude>%f</latitude><country>%s</country></geoloc></geolocs><searchradius>100</searchradius><where><country><eq>%s</eq></country><or><columbia><eq>1</eq></columbia><outlet><eq>1</eq></outlet><store><eq>1</eq></store></or></where></formdata></request>' % (
            data['cookie']['AppKey'], data['city_lat'], data['city_lng'], c_code, c_code)}
    param = {
        'xml_request': '<request><appkey>%s</appkey><formdata id="locatorsearch"><order>rank,_distance</order><geolocs><geoloc><addressline></addressline><city></city><state></state><province></province><postalcode></postalcode><longitude>%f</longitude><latitude>%f</latitude><country>%s</country></geoloc></geolocs><searchradius>100</searchradius><where><country><eq>%s</eq></country><or><columbia><eq>1</eq></columbia><outlet><eq>1</eq></outlet><store><eq>1</eq></store></or></where></formdata></request>' % (
            data['cookie']['AppKey'], data['city_lng'], data['city_lat'], c_code, c_code)}
    try:
        body = cm.get_data(url, param)
    except Exception, e:
        cm.dump('Error in fetching stores: %s, %s' % (url, param), log_name)
        return ()

    tree = et.fromstring(body.encode('utf-8'))
    store_list = []
    for store in tree.iter('poi'):
        entry = cm.init_store_entry(data['brand_id'], data['brandname_e'], data['brandname_c'])
        val = store.getiterator('uid')[0].text
        if val in store_map:
            continue
        store_map[val] = entry

        val = store.getiterator('name')[0].text
        entry[cm.name_e] = cm.html2plain(val).strip() if val else ''

        addr_list = []
        for idx in xrange(1, 3):
            val = store.getiterator('address%d' % idx)[0].text
            if val:
                val = cm.reformat_addr(val)
                if val != '':
                    addr_list.append(val)
        entry[cm.addr_e] = ', '.join(addr_list)

        val = store.getiterator('city')[0].text
        entry[cm.city_e] = cm.html2plain(val).strip().upper() if val else ''
        val = store.getiterator('province')[0].text
        entry[cm.province_e] = cm.html2plain(val).strip().upper() if val else ''
        if entry[cm.province_e] == '':
            val = store.getiterator('state')[0].text
            entry[cm.province_e] = cm.html2plain(val).strip().upper() if val else ''
        val = store.getiterator('country')[0].text
        entry[cm.country_e] = val.strip().upper() if val else ''

        val = store.getiterator('email')[0].text
        entry[cm.email] = val if val else ''
        val = store.getiterator('phone')[0].text
        entry[cm.tel] = val if val else ''
        val = store.getiterator('postalcode')[0].text
        entry[cm.zip_code] = val if val else ''

        week_days = (
            ('mon', 'Monday'), ('tue', 'Tuesday'), ('wed', 'Wednesday'), ('thu', 'Thursday'), ('fri', 'Friday'),
            ('sat', 'Saturday'), ('sun', 'Sunday'))
        hours_list = []
        for day in week_days:
            val = store.getiterator(day[0])[0].text
            if not val:
                continue
            hours_list.append('%s: %s' % (day[1], val))
        entry[cm.hours] = ', '.join(hours_list)

        type_term = (('accessories', 'Accessories'), ('fishing', 'Fishing'), ('footwear', 'Footwear'),
                     ('hunting', 'Hunting'), ('outerwear', 'Outwear'), ('skispecialty', 'Ski'),
                     ('sportswear', 'Sportswear'))
        type_list = []
        for term in type_term:
            val = store.getiterator(term[0])[0].text
            if not val or str(val) == '0':
                continue
            type_list.append(term[1])
        entry[cm.store_type] = ', '.join(type_list)

        try:
            val = store.getiterator('latitude')[0].text
            entry[cm.lat] = string.atof(str(val)) if val else ''
        except (ValueError, KeyError, TypeError) as e:
            cm.dump('Error in fetching lat: %s' % str(e), log_name)
        try:
            val = store.getiterator('longitude')[0].text
            entry[cm.lng] = string.atof(str(val)) if val else ''
        except (ValueError, KeyError, TypeError) as e:
            cm.dump('Error in fetching lat: %s' % str(e), log_name)

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
            # 国家列表
            return [{'func': lambda data: func(data, level + 1), 'data': s} for s in fetch_cities(data)]
        if level == 2:
            # 商店
            return [{'func': None, 'data': s} for s in fetch_stores(data)]
        else:
            return ()

    # Walk from the root node, where level == 1.
    if data is None:
        data = {'data_url': 'http://hosted.where2getit.com/columbia/ajax',
                'url': 'http://hosted.where2getit.com/columbia/findstore.html',
                'brand_id': 10095, 'brandname_e': u'Columbia', 'brandname_c': u'哥伦比亚',
                'city_map': gen_city_map()}

    global db
    db = cm.StoresDb()
    db.connect_db(user=user, passwd=passwd)
    db.execute(u'DELETE FROM %s WHERE brand_id=%d' % ('stores', data['brand_id']))

    results = cm.walk_tree({'func': lambda data: func(data, 0), 'data': data})
    db.disconnect_db()
    cm.dump('Done!', log_name)

    return results


