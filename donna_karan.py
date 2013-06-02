# coding=utf-8
import json
import string
import urllib
import urllib2
import re
import common

__author__ = 'Zephyre'

__brand__ = 'dkny'
__brand__ = 'donnakaran'


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


def get_stores(url):
    """
    从json对象中获得商店信息
    """
    opener = urllib2.build_opener()
    opener.addheaders = [("User-Agent",
                          "Mozilla/5.0 (Windows NT 6.1) AppleWebKit/537.36 (KHTML, like Gecko)"
                          "Chrome/27.0.1453.94 Safari/537.36"),
                         ('Accept', '*/*'), ('X-Requested-With', 'XMLHttpRequest'), ('Connection', 'keep-alive')]
    response = opener.open(url)
    html = response.read()
    stores = json.loads(html)[u'Stores'][u'Items']
    for s in stores:
        print('Found store: %s, %s. Tel: %s, lat=%s, lng=%s' % (
            s['Name'], s['Address'], s['Phone'], s['Latitude'], s['Longitude']))
    return stores


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
            if not c['code'].__eq__('us'):
                continue

            url = 'http://www.donnakaran.com/store/formpartial?' + urllib.urlencode({'country': c['code']})
            print('Fetching for %s...' % c['name'])
            if c['code'].__eq__('us'):
                col = fetch_stores(url, 1, None)
                if col is not None:
                    stores.extend(col)
            else:
                col = fetch_stores(url, 2, {'country': c['code'], 'region': 0})
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
            col = fetch_stores(url, 2, {'country': 'us', 'region': s['code']})
            if col is not None:
                stores.extend(col)

    elif type == 2:
        # 获得城市列表
        opt = ['param param-city', r'<option\s+value="([^\s]+)"\s*>([\w\s]+)</option>', 'http://www.donnakaran.com%s']
        cities = get_district(url, opt)
        stores = []
        country = data['country']
        region = data['region']
        for c in cities:
            # country=ca&region=0&city=burlington&zip=&brand=dkny&p=1&output=json
            url = 'http://www.donnakaran.com/store/listpartial?' + \
                  urllib.urlencode({'output': 'json', 'country': country, 'region': region,
                                    'city': c['name'],
                                    'zip': '', 'brand': __brand__, 'p': 1})
            print('\tFetching for %s...' % c['name'])
            col = fetch_stores(url, 3, None)
            if col is not None:
                stores.extend(col)
        return stores
    elif type == 3:
    # 获得城市中的商店信息
        return get_stores(url)


def fetch(brand):
    __brand__ = brand
    return fetch_stores(None, 0, None)
