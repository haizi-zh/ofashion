# coding=utf-8

__author__ = 'Zephyre'

import string
import re
import common

url = 'http://www.shanghaitang.com/stores.html'


def get_district(html):
    """
    Return: [continents, countries, cities]
    """
    continents = {}
    countries = {}
    cities = {}

    # continents
    start = html.find(u'<div class="store-col store-col1')
    if start != -1:
        end = html.find(u'</div>', start)
        sub_html = html[start:end]

        # <a href="/stores/asia.html" rel="74" title="Asia">Asia</a>
        for m in re.findall(ur'<a href=".*?" rel="(\d+)" title=".*?">([\w\s]+)</a>', sub_html):
            continent_id = string.atoi(m[0])
            continents[continent_id] = m[1].strip()

    # countries
    start = html.find(u'<div class="store-col store-col2')
    if start != -1:
        end = html.find(u'</div>', start)
        sub_html = html[start:end]

        # <li class="country74 hide"><a href="/stores/asia/china.html" rel="75" title="China">China</a></li>
        for m in re.findall(ur'<li class="country(\d+) hide"><a href=".*?" rel="(\d+)" title=".*?">([\w\s]+)</a></li>',
                            sub_html):
            continent_id = string.atoi(m[0])
            country_id = string.atoi(m[1])
            countries[country_id] = {'name': m[2].strip(), 'continent': continents[continent_id]}

    # cities
    start = html.find(u'<div class="store-col store-col3')
    if start != -1:
        end = html.find(u'</div>', start)
        sub_html = html[start:end]

        # <li class="city75 hide"><a href="/stores/asia/china/beijing.html" rel="76" title="Beijing">Beijing</a></li>
        for m in re.findall(ur'<li class="city(\d+) hide"><a href=".*?" rel="(\d+)" title=".*?">([\w\s]+)</a></li>',
                            sub_html):
            country_id = string.atoi(m[0])
            city_id = string.atoi(m[1])
            cities[city_id] = {'name': m[2].strip(), 'country': countries[country_id]}

    return [continents, countries, cities]


def get_stores(html, cities):
    start = None
    stores = []

    def proc(html, start, end, city_id):
        sub_html = html[start:end]
        store_name, store_addr, store_tel, store_opening = [''] * 4
        for m1 in re.findall(ur'<div class="store-desc">(.+?)</div>', sub_html, re.S):
            store_name = m1.strip()
            break
        if store_name.__eq__(''):
            return None

        for m1 in re.findall(ur'<div class="store-terminal">(.+?)</div>', sub_html, re.S):
            store_addr = common.reformat_addr(m1)
            break

        for m1 in re.findall(ur'<div class="store-tel">\s*(Tel:)?\s*(.+?)\s*</div>', sub_html, re.S):
            store_tel = m1[1].strip()
            break;

        for m1 in re.findall(ur'<div class="store-opening-hour">\s*?(?:Opening Hours:)?\s*?(.+?)\s*?</div>', sub_html,
                             re.S):
            store_opening = m1.strip()
            break

        return {'name': store_name, 'addr': store_addr, 'tel': store_tel, 'opening': store_opening,
                'city': cities[city_id]['name'], 'country': cities[city_id]['country']['name'],
                'continent': cities[city_id]['country']['continent']}

    for m in re.finditer(ur'<div class="store-row city(\d+) hide">', html):
        val = string.atoi(m.group(1))
        if start is None:
            start = m.start()
            cid = val
            continue

        end = m.start()
        store = proc(html, start, end, cid)
        if store is not None:
            stores.append(store)
            start = end
            cid = val
            print('Found store: %s, %s (%s, %s, %s)' % (
                store['name'], store['addr'], store['city'], store['country'], store['continent']))

    return stores


def fetch():
    def func(url):
        html = common.get_data(url)
        c3 = get_district(html)[2]
        stores=get_stores(html,c3)
        return [{'func':None,'data':s} for s in stores]

    node = {'func': func, 'data': url}
    return common.walk_tree(node)

