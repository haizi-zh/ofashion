# coding=utf-8
import common
from stores import geosense as gs

__author__ = 'Zephyre'

import string
import re

url = 'http://www.shanghaitang.com/stores.html'
host = 'http://www.shanghaitang.com'
brand_id = 10371
brandname_e = u'Shanghai Tang'
brandname_c = u'上海滩'


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


# def get_stores1(html, cities):
#     start = None
#     stores = []
#
#     def proc(html, start, end, city_id):
#         sub_html = html[start:end]
#         store_name, store_addr, store_tel, store_opening = [''] * 4
#         for m1 in re.findall(ur'<div class="store-desc">(.+?)</div>', sub_html, re.S):
#             store_name = m1.strip()
#             break
#         if store_name.__eq__(''):
#             return None
#
#         for m1 in re.findall(ur'<div class="store-terminal">(.+?)</div>', sub_html, re.S):
#             store_addr = common.reformat_addr(m1)
#             break
#
#         for m1 in re.findall(ur'<div class="store-tel">\s*(Tel:)?\s*(.+?)\s*</div>', sub_html, re.S):
#             store_tel = m1[1].strip()
#             break;
#
#         for m1 in re.findall(ur'<div class="store-opening-hour">\s*?(?:Opening Hours:)?\s*?(.+?)\s*?</div>', sub_html,
#                              re.S):
#             store_opening = m1.strip()
#             break
#
#         return {'name': store_name, 'addr': store_addr, 'tel': store_tel, 'opening': store_opening,
#                 'city': cities[city_id]['name'], 'country': cities[city_id]['country']['name'],
#                 'continent': cities[city_id]['country']['continent']}
#
#     for m in re.finditer(ur'<div class="store-row city(\d+) hide">', html):
#         val = string.atoi(m.group(1))
#         if start is None:
#             start = m.start()
#             cid = val
#             continue
#         end = m.start()
#         store = proc(html, start, end, cid)
#         if store is not None:
#             stores.append(store)
#             start = end
#             cid = val
#             print('Found store: %s, %s (%s, %s, %s)' % (
#                 store['name'], store['addr'], store['city'], store['country'], store['continent']))
#
#     return stores
#

def get_coordinates(url):
    try:
        html = common.get_data(url)
    except Exception:
        print 'Error occured in retrieving the coordinates: %s' % url
        dump_data = {'level': 2, 'time': common.format_time(), 'data': {'data': url}, 'brand_id': brand_id}
        common.dump(dump_data)
        return []

    m = re.findall(ur'new google.maps.LatLng\(\s*?(-?\d+\.\d+)\s*?,\s*?(-?\d+\.\d+)\s*?\)', html)
    if len(m) > 0:
        return [string.atof(m[0][0]), string.atof(m[0][1])]
    else:
        return ['', '']


def get_stores(html, cities):
    """
    获得门店信息
    :param html:
    :param cities: 已取得的城市信息
    """
    sub_list = []
    start = 0
    start_city_id = 0
    for m in re.finditer(ur'<div class="store-row city(\d+).*?">', html):
        if start == 0:
            start = m.start()
            start_city_id = string.atoi(m.group(1))
            continue
        end = m.start()
        # sub_list.append(html[start:end])
        sub_list.append({'city_id': start_city_id, 'html': html[start:end]})
        start = end
        start_city_id = string.atoi(m.group(1))
    sub_list.append({'city_id': string.atoi(m.group(1)), 'html': html[start:]})

    store_list = []
    for m in sub_list:
        city_id = m['city_id']
        sub_html = m['html']
        entry = common.init_store_entry(brand_id, brandname_e, brandname_c)
        for m1 in re.findall(ur'<div class="store-desc">(.+?)</div>', sub_html, re.S):
            entry[common.name_e] = common.reformat_addr(m1)
            break

        for m1 in re.findall(ur'<div class="store-terminal">(.+?)</div>', sub_html, re.S):
            entry[common.addr_e] = common.reformat_addr(m1)
            break

        for m1 in re.findall(ur'<div class="store-tel">(.+?)</div>', sub_html, re.S):
            entry[common.tel] = common.extract_tel(m1)
            break

        for m1 in re.findall(ur'<div class="store-opening-hour">\s*?(?:Opening Hours:)?(.+?)</div>', sub_html,
                             re.S):
            entry[common.hours] = common.reformat_addr(m1)
            break

        m1 = re.findall(ur'href="/(.+?)" title="View on map"', sub_html)
        if len(m1) > 0:
            entry[common.url] = host + '/' + m1[0]
            lat, lng = get_coordinates(entry[common.url])
            common.update_entry(entry, {common.lat: lat, common.lng: lng})

        # geo
        city_e = cities[city_id]['name'].strip()
        country_e = cities[city_id]['country']['name'].strip().upper()
        continent_e = cities[city_id]['country']['continent'].strip().upper()
        common.update_entry(entry,
                            {common.city_e: common.extract_city(city_e)[0], common.country_e: country_e, common.continent_e: continent_e})
        gs.field_sense(entry)

        # ret = common.geo_translate(country_e.strip())
        # if len(ret) > 0:
        #     common.update_entry(entry, {common.continent_c: ret[common.continent_c],
        #                                 common.continent_e: ret[common.continent_e],
        #                                 common.country_c: ret[common.country_c],
        #                                 common.country_e: ret[common.country_e]})
        # common.update_entry(entry, {common.brandname_c: brandname_c, common.brandname_e: brandname_e})
        # common.chn_check(entry)

        print '%s Found store: %s, %s (%s, %s)' % (
            brandname_e, entry[common.name_e], entry[common.addr_e], entry[common.country_e],
            entry[common.continent_e])
        db.insert_record(entry, 'stores')
        store_list.append(entry)

    return store_list


def fetch(data=None, user='root', passwd=''):
    try:
        html = common.get_data(url)
    except Exception:
        print 'Error occured: %s' % url
        dump_data = {'level': 1, 'time': common.format_time(), 'data': {'data': url}, 'brand_id': brand_id}
        common.dump(dump_data)
        return []

    global db
    db = common.StoresDb()
    db.connect_db(user=user, passwd=passwd)
    db.execute(u'DELETE FROM %s WHERE brand_id=%d' % ('stores', brand_id))
    cities = get_district(html)[2]
    stores = get_stores(html, cities)
    db.disconnect_db()
    return stores