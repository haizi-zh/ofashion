# coding=utf-8
import json
import urllib
import urllib2
import re
from stores import geosense as gs

__author__ = 'Zephyre'

__brand__ = 'dkny'
__brand__ = 'donnakaran'

db = None
brand_id = 10110
brandname_e = 'Donna Karan'
brandname_c = u'唐娜·凯伦'


def get_district(url, opt):
    """
    获得国家或城市列表

    """
    opener = urllib2.build_opener()
    opener.addheaders = [("User-Agent",
                          "Mozilla/5.0 (Windows NT 6.1) AppleWebKit/537.36 (KHTML, like Gecko)"
                          "Chrome/27.0.1453.94 Safari/537.36"),
                         ('Accept', '*/*'), ('X-Requested-With', 'XMLHttpRequest'), ('Connection', 'keep-alive')]
    response = opener.open(url)
    html = response.read()
    # 开始解析
    start = html.find(opt[0])
    if start == -1:
        return []
    end = html.find('</select>', start)
    if end == -1:
        return []
    html = html[start:end]
    # <option value="">Choose a Country</option>
    countries = []
    for m in re.finditer(opt[1], html, re.S):
        if m.group(1).__len__() > 4:
            # 这是一个网址，不用保存
            countries.append({'name': m.group(2)})
        else:
            countries.append(
                {'name': m.group(2), 'code': m.group(1)})

    return countries


def get_stores(url, data):
    """
    从json对象中获得商店信息
    """
    opener = urllib2.build_opener()
    opener.addheaders = [("User-Agent",
                          "Mozilla/5.0 (Windows NT 6.1) AppleWebKit/537.36 (KHTML, like Gecko)"
                          "Chrome/27.0.1453.94 Safari/537.36"),
                         ('Accept', '*/*'), ('X-Requested-With', 'XMLHttpRequest'), ('Connection', 'keep-alive')]
    response = opener.open(url)
    html = response.read().encode('utf-8')
    jsonobj = json.loads(html)
    stores = jsonobj[u'Stores'][u'Items']
    region_list = jsonobj['Regions']
    region_id = jsonobj['Region']
    region = ''
    if len(region_list) > 0 and region_id != 0:
        for val in region_list:
            if val['RegionId'] == region_id:
                region = val['Name']
                break

    country = jsonobj['CurrentCountry']['Name']
    store_list = []
    for s in stores:
        # print('Found store: %s, %s. Tel: %s, lat=%s, lng=%s' % (
        #     s['Name'], s['Address'], s['Phone'], s['Latitude'], s['Longitude']))

        store_type = ['']
        # Some stores may have varioius store types
        if len(s['StoreTypes']) > 0:
            store_type = list(val['Name'] for val in s['StoreTypes'])
        if s['Url'] is not None:
            url = s['Url']
        else:
            url = ''
        if s['ZipCode'] is not None and not s['ZipCode'].__eq__(''):
            zip = s['ZipCode']
        else:
            zip = ''
        local_addr = s['Address']
        if local_addr[-1] == '.':
            local_addr = local_addr[:-1]
        if not zip.__eq__(''):
            addr = u'%s, %s, %s' % (local_addr, s['City'], zip)
        else:
            addr = u'%s, %s' % (local_addr, s['City'])
        if region.__eq__(''):
            addr = u'%s, %s' % (addr, country)
        else:
            addr = u'%s, %s, %s' % (addr, region, country)

        for t in store_type:
            entry = cm.init_store_entry(brand_id, brandname_e, brandname_c)
            cm.update_entry(entry, {'addr_e': addr, 'country_e': country, 'city_e': s['City'],
                                    'comments': s['Comments'],
                                    'province_e': region.strip().upper(), 'zip': zip,
                                    'email': s['Email'], 'fax': s['Fax'], 'lat': s['Latitude'], 'lng': s['Longitude'],
                                    'name_e': s['Name'], 'tel': s['Phone'], 'store_type': t, 'url': url})
            gs.field_sense(entry)

            print '%s Found store: %s, %s (%s, %s)' % (
                brandname_e, entry[cm.name_e], entry[cm.addr_e], entry[cm.country_e],
                entry[cm.continent_e])
            db.insert_record(entry, 'stores')
            store_list.append(entry)

    return store_list


def fetch_stores(url, type, data):
    """
    type: 0: 国家, 1: 地区，2:城市
    """
    if type == 0:
        url = 'http://www.donnakaran.com/store'
        opt = ['param param-country', r'<option\s+value="/store\?country=(\w+)\s*".*?>([\w\s]+)</option>',
               'http://www.donnakaran.com/store?country=%s']
        countries = get_district(url, opt)
        stores = []
        for c in countries:
            # if c['code']!='us':
            #     continue

            url = 'http://www.donnakaran.com/store/formpartial?' + urllib.urlencode({'country': c['code']})
            print('Fetching for %s...' % c['name'])
            if c['code'].__eq__('us'):
                col = fetch_stores(url, 1, {'country_name': c['name'], 'code': c['code']})
                if col is not None:
                    stores.extend(col)
            else:
                col = fetch_stores(url, 2, {'country_name': c['name'], 'code': c['code'], 'region': 0})
                if col is not None:
                    stores.extend(col)
        return stores
    elif type == 1:
        # 获得洲列表
        opt = ['param param-region', r'<option value="/store/list\?country=us&region=(\d+).*?"\s*>([\w\s]+)</option>']
        states = get_district(url, opt)
        stores = []
        for s in states:
            url = 'http://www.donnakaran.com/store/formpartial?' + \
                  urllib.urlencode({'country': 'us', 'region': s['code'], 'p': 1})
            print('Fetching for %s...' % s['name'])
            d = dict(data)
            d['region'] = s['code']
            d['province_name'] = s['name'].strip().upper()
            col = fetch_stores(url, 2, d)
            if col is not None:
                stores.extend(col)

    elif type == 2:
        # 获得城市列表
        opt = ['param param-city', r'<option\s+value="([^\s]+)"\s*>([\w\s]+)</option>', 'http://www.donnakaran.com%s']
        cities = get_district(url, opt)
        stores = []
        country_code = data['code']
        region = data['region']
        for c in cities:
            # country=ca&region=0&city=burlington&zip=&brand=dkny&p=1&output=json
            url = 'http://www.donnakaran.com/store/listpartial?' + \
                  urllib.urlencode({'output': 'json', 'country': country_code, 'region': region,
                                    'city': c['name'],
                                    'zip': '', 'brand': __brand__, 'p': 1})
            print('\tFetching for %s...' % c['name'])
            d = dict(data)
            d['city_name'] = c['name']
            col = fetch_stores(url, 3, d)
            if col is not None:
                stores.extend(col)
        return stores
    elif type == 3:
        # 获得城市中的商店信息
        if 'province_name' in data:
            gs.update_city_map(data['city_name'], data['country_name'], province_name=data['province_name'])
        else:
            gs.update_city_map(data['city_name'], data['country_name'])
        return get_stores(url, data)


def fetch(user='root', passwd=''):
    global db
    db = cm.StoresDb()
    db.connect_db(user=user, passwd=passwd)
    db.execute(u'DELETE FROM %s WHERE brand_id=%d' % ('stores', brand_id))

    # Walk from the root node, where level == 1.
    results = fetch_stores(None, 0, None)

    db.disconnect_db()
    for i in xrange(4):
        gs.commit_maps(i)

    return results