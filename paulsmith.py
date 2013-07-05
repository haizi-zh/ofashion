# coding=utf-8
import json
import string
import re
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

    start = html.find(u'<div id="block-ps-shop-locator-shop-locator-filters"')
    if start == -1:
        return []
    html, start, end = cm.extract_closure(html[start:], ur'<div\b', ur'</div>')
    if end == 0:
        return []

    country_list = []

    for m in re.finditer(ur'<h3>(.+?)</h3>', html):
        continent_e = m.group(1).strip().upper()
        if continent_e == u'UK' and False:
            d = data.copy()
            d[cm.continent_e] = u'EUROPE'
            d[cm.country_e] = u'UNITED KINGDOM'
            d[cm.url] = data['host'] + '/uk-en/shop-locator/gb/all'
            country_list.append(d)
        else:
            sub, start, end = cm.extract_closure(html[m.end():], ur'<ul\b', ur'</ul>')
            if end == 0:
                continue
                #<a href="/uk-en/shop-locator/fr/all">France</a>
            for m1 in re.findall(ur'<a href="(.+?)">(.+?)</a>', sub):
                d = data.copy()
                d[cm.continent_e] = continent_e
                d[cm.country_e] = m1[1].strip().upper()
                d[cm.url] = data['host'] + m1[0]
                if d[cm.country_e]=='SINGAPORE':
                    country_list.append(d)

    return country_list


def fetch_stores(data):
    # <h2 property="dc:title"
    url = data[cm.url]
    try:
        html = cm.get_data(url)
    except Exception:
        print 'Error occured: %s' % url
        dump_data = {'level': 0, 'time': cm.format_time(), 'data': {'url': url}, 'brand_id': data['brand_id']}
        cm.dump(dump_data)
        return []

    store_list = []
    for m in re.finditer(ur'<h2 property="dc:title"', html):
        end = html.find('</header>', m.start())
        if end == -1:
            continue
        sub = html[m.start():end]
        m1 = re.search(ur'<a href="(.+?)">(.+?)</a></h2>', sub)
        if m1 is None:
            print 'Error: no more details for %s' % url
            continue
        d = data.copy()
        d[cm.url] = data['host'] + m1.group(1)
        d[cm.name_e] = cm.html2plain(m1.group(2)).strip()
        store_list.append(d)
    return store_list


def fetch_details(data):
    url = data[cm.url]
    try:
        html = cm.get_data(url)
    except Exception:
        print 'Error occured: %s' % url
        dump_data = {'level': 0, 'time': cm.format_time(), 'data': {'url': url}, 'brand_id': data['brand_id']}
        cm.dump(dump_data)
        return []

    entry = cm.init_store_entry(data['brand_id'], data['brandname_e'], data['brandname_c'])
    entry[cm.name_e] = data[cm.name_e]
    start = html.find(ur'<div class="field-address">')
    if start == -1:
        return []
    sub, start, end = cm.extract_closure(html[start:], ur'<div\b', ur'</div>')
    if end == 0:
        return []
    m1 = re.search(ur'<div  class="locality">(.+?)</div>', sub)
    if m1 is not None:
        entry[cm.city_e] = cm.extract_city(m1.group(1))[0]
    m1 = re.search(ur'<div  class="postal-code">(.+?)</div>', sub)
    if m1 is not None:
        entry[cm.zip_code] = m1.group(1).strip()
    entry[cm.country_e] = data[cm.country_e]
    pat = re.compile(ur'<[^<>]+?>', re.S)
    entry[cm.addr_e] = cm.reformat_addr(re.sub(pat, u'\r\n', sub))

    m1 = re.search(ur'<div class="field-telephone"><a href=".+?" class="tel">(.+?)</a></div>', html)
    if m1 is not None:
        entry[cm.tel] = m1.group(1).strip()

    m1 = re.search(ur'<div class="field-opening-hours">\s*<p>(.+?)</p>\s*</div>', html, re.S)
    if m1 is not None:
        entry[cm.hours] = cm.reformat_addr(m1.group(1))

    m1 = re.search(ur'"coordinates":\[(-?\d+\.\d{4,})\s*,\s*(-?\d+\.\d{4,})\]', html)
    if m1 is not None:
        lat = string.atof(m1.group(1))
        lng = string.atof(m1.group(2))
        cm.update_entry(entry, {cm.lat: lat, cm.lng: lng})

    entry[cm.continent_e] = data[cm.continent_e]
    gs.field_sense(entry)
    print '(%s / %d) Found store: %s, %s (%s, %s)' % (data['brandname_e'], data['brand_id'],
                                                      entry[cm.name_e], entry[cm.addr_e], entry[cm.country_e],
                                                      entry[cm.continent_e])
    db.insert_record(entry, 'stores')
    return [entry]


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
            # 商店信息
            return [{'func': lambda data: func(data, 2), 'data': s} for s in fetch_stores(data)]
        elif level == 2:
            return [{'func': None, 'data': s} for s in fetch_details(data)]

        # elif level == 2:
        #     # 城市列表
        #     return [{'func': lambda data: func(data, 3), 'data': s} for s in fetch_cities(data)]
        # elif level == 3:
        #     # 商店的具体信息
        #     return [{'func': None, 'data': s} for s in fetch_stores(data)]
        else:
            return []

    # Walk from the root node, where level == 1.
    if data is None:
        data = {'home_url': 'http://www.paulsmith.co.uk/uk-en/information/featured-shop',
                'host': 'http://www.paulsmith.co.uk',
                # 'post_url': 'http://www.mauricelacroix.com/RetailAndService/FinderJson.sls',
                'brand_id': 10299, 'brandname_e': u'Paul Smith', 'brandname_c': u'保罗·史密斯'}

    global db
    db = cm.StoresDb()
    db.connect_db(user=user, passwd=passwd)
    db.execute(u'DELETE FROM %s WHERE brand_id=%d' % ('stores', data['brand_id']))

    results = cm.walk_tree({'func': lambda data: func(data, 0), 'data': data})
    db.disconnect_db()

    return results