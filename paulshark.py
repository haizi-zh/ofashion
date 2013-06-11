# coding=utf-8
import json
import random
import string
import re
import urllib
import common as cm
import geosense as gs

__author__ = 'Zephyre'

db = None


def fetch_countries(data):
    url = data['home_url']
    try:
        html = cm.get_data(url)
    except Exception:
        print 'Error occured: %s' % url
        dump_data = {'level': 0, 'time': cm.format_time(), 'data': {'url': url}, 'brand_id': data['brand_id']}
        cm.dump(dump_data)
        return []

    country_list = []
    for m in re.findall(ur'<option value="[\w ]+?">(.+?)</option>', html):
        d = data.copy()
        d['country_e'] = m
        country_list.append(d)
    return country_list


def fetch_cities(data):
    # http://www.paulshark.it/StoreLocator/ajaxGetCitiesByCountry?country=UNITED%20STATES&lang=en&rnd=0.33586437113497525
    url = data['data_url']
    try:
        html = cm.get_data(url, {'country': urllib.quote(data['country_e']), 'lang':'en',
                                 'rnd':str(random.random())})
    except Exception:
        print 'Error occured: %s' % url
        dump_data = {'level': 0, 'time': cm.format_time(), 'data': {'url': url}, 'brand_id': data['brand_id']}
        cm.dump(dump_data)
        return []

    city_list = []
    for m in re.findall(ur'<option value="([\w ]+?)">', html):
        d = data.copy()
        d['city_e'] = m
        city_list.append(d)
    return city_list


def fetch_stores(data):
    url = data['store_url']
    try:
        html = cm.get_data(url, {'nazione': urllib.quote(data['country_e']),
                                 'citta': urllib.quote(data['city_e']),
                                 'tipo':'tutti'})
    except Exception:
        print 'Error occured: %s' % url
        dump_data = {'level': 0, 'time': cm.format_time(), 'data': {'url': url}, 'brand_id': data['brand_id']}
        cm.dump(dump_data)
        return []

    store_list=[]
    for m in re.finditer(ur'<marker\b', html):
        sub, start, end=cm.extract_closure(html[m.start():], ur'<marker\b', ur'</marker>')
        if end==0:
            continue

        entry = cm.init_store_entry(data['brand_id'], data['brandname_e'], data['brandname_c'])
        m1 = re.search(ur'name\s*=\s*"(.+?)"', sub)
        if m1 is not None:
            entry[cm.name_e]= cm.html2plain(m1.group(1)).strip()

        addr_list=[]
        m1 = re.search(ur'address\s*=\s*"(.+?)"', sub)
        if m1 is not None and cm.html2plain(m1.group(1)).strip()!='':
            addr_list.append(cm.html2plain(m1.group(1)).strip())
        m1 = re.search(ur'addr2\s*=\s*"(.+?)"', sub)
        if m1 is not None and cm.html2plain(m1.group(1)).strip()!='':
            addr_list.append(cm.html2plain(m1.group(1)).strip())
        entry[cm.addr_e]= ', '.join(addr_list)

        m1 = re.search(ur'city\s*=\s*"(.+?)"', sub)
        if m1 is not None:
            entry[cm.city_e]= cm.html2plain(m1.group(1)).strip().upper()

        m1 = re.search(ur'country\s*=\s*"(.+?)"', sub)
        if m1 is not None:
            entry[cm.country_e]= cm.html2plain(m1.group(1)).strip().upper()

        m1 = re.search(ur'zipcode\s*=\s*"(.+?)"', sub)
        if m1 is not None:
            entry[cm.name_e]= cm.html2plain(m1.group(1)).strip()

        m1 = re.search(ur'phone\s*=\s*"(.+?)"', sub)
        if m1 is not None:
            entry[cm.tel]= cm.html2plain(m1.group(1)).strip()

        m1 = re.search(ur'email\s*=\s*"(.+?)"', sub)
        if m1 is not None:
            entry[cm.email]= cm.html2plain(m1.group(1)).strip()

        m1 = re.search(ur'website\s*=\s*"(.+?)"', sub)
        if m1 is not None:
            entry[cm.url]= cm.html2plain(m1.group(1)).strip()

        m1 = re.search(ur'lat\s*=\s*"(.+?)"', sub)
        if m1 is not None:
            entry[cm.lat]= string.atof(m1.group(1))

        m1 = re.search(ur'lng\s*=\s*"(.+?)"', sub)
        if m1 is not None:
            entry[cm.lng]= string.atof(m1.group(1))

        gs.field_sense(entry)
        print '(%s / %d) Found store: %s, %s (%s, %s)' % (data['brandname_e'], data['brand_id'],
                                                          entry[cm.name_e], entry[cm.addr_e], entry[cm.country_e],
                                                          entry[cm.continent_e])
        store_list.append(entry)
        db.insert_record(entry, 'stores')

    return store_list


def fetch(level=1, data=None, user='root', passwd=''):
    def func(data, level):
        """
        :param data:
        :param level: 0：国家；1：城市；2：商店列表
        """
        if level == 0:
            # 国家列表
            return [{'func': lambda data: func(data, 1), 'data': c} for c in fetch_countries(data)]
        elif level == 1:
            # 城市信息
            return [{'func': lambda data: func(data, 2), 'data': c} for c in fetch_cities(data)]
        elif level==2:
            # 商店信息
            return [{'func': None, 'data': s} for s in fetch_stores(data)]
        else:
            return []

    # Walk from the root node, where level == 1.
    if data is None:
        data = {'home_url': 'http://www.paulshark.it/en/storelocator',
                'data_url': 'http://www.paulshark.it/StoreLocator/ajaxGetCitiesByCountry',
                'store_url': 'http://www.paulshark.it/en/StoreLocator/getResultsMarkersFisso',
                'brand_id': 10298, 'brandname_e': u'Paul & Shark', 'brandname_c': u'保罗与鲨鱼'}

    global db
    db = cm.StoresDb()
    db.connect_db(user=user, passwd=passwd)
    db.execute(u'DELETE FROM %s WHERE brand_id=%d' % ('stores', data['brand_id']))

    results = cm.walk_tree({'func': lambda data: func(data, 0), 'data': data})
    db.disconnect_db()

    return results