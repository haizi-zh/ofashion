# coding=utf-8
import json
import string
import re
import urllib
import common as cm
import geosense as gs

__author__ = 'Zephyre'

db = None
log_name = 'celine_log.txt'


def fetch_countries(data):
    url = data['url']
    try:
        body = cm.get_data(url)
    except Exception, e:
        cm.dump('Error in fetching countries: %s' % url, log_name)
        return ()

    start = body.find(ur'<div id="block-aam-store-localisation-store-pays"')
    if start == -1:
        cm.dump('Error in fetching countries: %s' % url, log_name)
        return ()
    sub = cm.extract_closure(body[start:], ur'<ul>', ur'</ul>')[0]
    results = []
    for m in re.findall(ur'<a href="([^"]+)" class="store-link"\s*>([^<>]+)', sub):
        d = data.copy()
        d['url'] = data['host'] + m[0]
        d['country'] = cm.html2plain(m[1]).strip().upper()
        results.append(d)

    if data['continent'] == 'Asia':
        d = data.copy()
        d['url'] = 'http://www.celine.com/en/informations/celine-stores/japan'
        d['country'] = 'JAPAN'
        results.append(d)

    return tuple(results)


def fetch_stores(data):
    url = data['url']
    try:
        body = cm.get_data(url)
    except Exception, e:
        cm.dump('Error in fetching stores: %s' % url, log_name)
        return ()

    start = body.find(ur'<div id="block-aam-store-localisation-store-store"')
    if start == -1:
        cm.dump('Error in fetching cities: %s' % url, log_name)
        return ()
    sub = cm.extract_closure(body[start:], ur'<div\b', ur'</div>')[0]

    store_list = []
    for s in re.findall(ur'<div class="views-row-\d+"\s*>(.+?)</div>', sub):
        entry = cm.init_store_entry(data['brand_id'], data['brandname_e'], data['brandname_c'])
        entry[cm.country_e] = data['country']
        entry[cm.city_e] = data['city']

        pat_voir = re.compile(ur'<a class="voir_link" href="([^"]+)"\s*>Voir</a>')
        m = re.search(pat_voir, s)
        entry[cm.url] = data['host'] + urllib.quote(m.group(1).encode('utf-8')) if m else ''
        store_sub = re.sub(pat_voir, '', s)

        pat_title = re.compile(ur'([^<>]*)<\s*br\s*>')
        m = re.search(pat_title, store_sub)
        entry[cm.name_e] = cm.html2plain(m.group(1)).strip() if m else ''
        store_sub = re.sub(pat_title, '', store_sub, 1)

        addr_list = [tmp.strip() for tmp in cm.reformat_addr(store_sub).split(',')]
        while True:
            tel = cm.extract_tel(addr_list[-1])
            if tel != '':
                entry[cm.tel] = tel
                del addr_list[-1]
            else:
                break
        entry[cm.addr_e] = ', '.join(addr_list)

        url = entry[cm.url]
        if url != '':
            try:
                details = cm.get_data(url)
            except Exception, e:
                cm.dump('Error in fetching store details: %s' % url, log_name)
                return ()

            m = re.search(ur'maps\.googleapis\.com/maps/api/staticmap\?center=(-?\d+\.\d+)\s*,\s*(-?\d+\.\d+)', details)
            if m:
                entry[cm.lat] = string.atof(m.group(1))
                entry[cm.lng] = string.atof(m.group(2))

            start = details.find(ur'<div id="fixed_map">')
            if start != -1:
                details = cm.extract_closure(details[start:], ur'<div\b', ur'</div>')[0]
                start = details.find(ur'<div class="aam-adresse-field fin-bloc horaires">')
                if start != -1:
                    details = cm.extract_closure(details[start:], ur'<div\b', ur'</div>')[0]
                    entry[cm.hours] = cm.reformat_addr(details)

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


def fetch_continents(data):
    vals = {'Asia': 'http://www.celine.com/en/informations/celine-stores/asia',
            'America': 'http://www.celine.com/en/informations/celine-stores/america',
            'Europe': 'http://www.celine.com/en/informations/celine-stores/europe',
            'Middle East': 'http://www.celine.com/en/informations/celine-stores/middle_east'}
    results = []
    for item in vals.items():
        d = data.copy()
        d['continent'] = item[0]
        d['url'] = item[1]
        results.append(d)
    return tuple(results)


def fetch_cities(data):
    url = data['url']
    try:
        body = cm.get_data(url)
    except Exception, e:
        cm.dump('Error in fetching cities: %s' % url, log_name)
        return ()

    start = body.find(ur'<div id="block-aam-store-localisation-store-ville"')
    if start == -1:
        cm.dump('Error in fetching cities: %s' % url, log_name)
        return ()
    sub = cm.extract_closure(body[start:], ur'<ul>', ur'</ul>')[0]
    results = []
    for m in re.findall(ur'<a href="([^"]+)" class="store-link"\s*>([^<>]+)', sub):
        d = data.copy()
        d['url'] = data['host'] + m[0]
        d['city'] = cm.html2plain(m[1]).strip().upper()
        results.append(d)
    return tuple(results)


def fetch(level=1, data=None, user='root', passwd=''):
    def func(data, level):
        """
        :param data:
        :param level: 0：国家；1：城市；2：商店列表
        """
        if level == 0:
            # 洲列表
            return [{'func': lambda data: func(data, level + 1), 'data': s} for s in fetch_continents(data)]
        if level == 1:
            # 国家列表
            return [{'func': lambda data: func(data, level + 1), 'data': s} for s in fetch_countries(data)]
        if level == 2:
            # 城市列表
            return [{'func': lambda data: func(data, level + 1), 'data': s} for s in fetch_cities(data)]
        if level == 3:
            # 商店
            return [{'func': None, 'data': s} for s in fetch_stores(data)]
        else:
            return ()

    # Walk from the root node, where level == 1.
    if data is None:
        data = {'host': 'http://www.celine.com',
                'url': 'http://www.celine.com/en/informations/celine-stores',
                'brand_id': 10070, 'brandname_e': u'Celine', 'brandname_c': u'思琳'}

    global db
    db = cm.StoresDb()
    db.connect_db(user=user, passwd=passwd)
    db.execute(u'DELETE FROM %s WHERE brand_id=%d' % ('stores', data['brand_id']))

    results = cm.walk_tree({'func': lambda data: func(data, 0), 'data': data})
    db.disconnect_db()
    cm.dump('Done!', log_name)

    return results


