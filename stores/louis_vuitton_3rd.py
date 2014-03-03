# coding=utf-8
import re

from pyquery import PyQuery as pq

import common as cm
from stores import geosense as gs


__author__ = 'Zephyre'

db = None
log_name = 'louis_vuitton_log.txt'
id_set = None


def fetch_countries(data):
    url = data['url']
    try:
        body = cm.get_data(url)
    except Exception, e:
        cm.dump('Error in fetching countries: %s' % url, log_name)
        return ()

    results = []
    for item in pq(body)('#slim_sidebar')('li')('a'):
        d = data.copy()
        d['country'] = cm.html2plain(item.text).strip().upper()
        d['url'] = data['host'] + item.attrib['href'] if 'href' in item.attrib else None
        if d['url']:
            results.append(d)
    return tuple(results)


def fetch_store_details(data):
    url = data['url']
    try:
        body = cm.get_data(url)
    except Exception, e:
        cm.dump('Error in fetching store details: %s' % url, log_name)
        return ()

    try:
        details = pq(pq(body)('.store-details')[0])

        entry = cm.init_store_entry(data['brand_id'], data['brandname_e'], data['brandname_c'])
        entry[cm.country_e] = data['country']
        entry[cm.province_e] = data['state'] if data['state'] else ''
        entry[cm.url] = url
        entry[cm.name_e] = data['store_name']
        entry[cm.city_e] = data['city'] if data['city'] else ''

        if data['addr']:
            entry[cm.addr_e] = data['addr']
        else:
            entry[cm.addr_e] = cm.reformat_addr(unicode(pq(details('p')[0])))

        if data['tel']:
            entry[cm.tel] = data['tel']
        else:
            tmp = details('p')[1].text
            pat = re.compile(ur'(phone|tel|telephone)\s*[\.: ]?\s*', re.I)
            if re.search(pat, tmp):
                entry[cm.tel] = re.sub(pat, '', tmp).strip()

        sub = unicode(details)
        start = sub.find(u'Regular Store Hours')
        if start != -1:
            m = re.search(ur'<p>(.+?)<ul', sub[start:], re.S)
            if m:
                entry[cm.hours] = cm.reformat_addr(m.group(1))

        type_list = []
        for item in pq(body)('#map-panel ul li'):
            if item.text:
                val = cm.html2plain(item.text).strip()
                if val != '':
                    type_list.append(val)
        entry[cm.store_type] = ', '.join(type_list)

        tmp = pq(body)('#map-panel iframe[src!=""]')
        if len(tmp) > 0:
            # map_url = tmp[0].attrib['src']
            m = re.search(ur'daddr=([^&]+)', tmp[0].attrib['src'])
            if m:
                map_url = 'http://maps.googleapis.com/maps/api/geocode/json?address=%s&sensor=false' % m.group(1)
                ret = gs.geocode(url=map_url)
                if ret:
                    city = ''
                    province = ''
                    country = ''
                    zip_code = ''
                    tmp = ret[0]['address_components']
                    for v in tmp:
                        if 'locality' in v['types']:
                            city = v['long_name'].strip().upper()
                        elif 'administrative_area_level_1' in v['types']:
                            province = v['long_name'].strip().upper()
                        elif 'country' in v['types']:
                            country = v['long_name'].strip().upper()
                        elif 'postal_code' in v['types']:
                            zip_code = v['long_name'].strip()
                    if entry[cm.country_e] == '':
                        entry[cm.country_e] = country
                    entry[cm.province_e] = province
                    if entry[cm.city_e] == '':
                        entry[cm.city_e] = cm.extract_city(city)[0]
                    entry[cm.zip_code] = zip_code
                    entry[cm.lat] = ret[0]['geometry']['location']['lat']
                    entry[cm.lng] = ret[0]['geometry']['location']['lng']
                    entry['is_geocoded'] = 1
                else:
                    cm.dump(u'Failed to geocode: %s' % map_url, log_name)

        gs.field_sense(entry)
        ret = gs.addr_sense(entry[cm.addr_e])
        if ret[0] is not None and entry[cm.country_e] == '':
            entry[cm.country_e] = ret[0]
        if ret[1] is not None and entry[cm.province_e] == '':
            entry[cm.province_e] = ret[1]
        if ret[2] is not None and entry[cm.city_e] == '':
            entry[cm.city_e] = ret[2]
        gs.field_sense(entry)
    except (IndexError, KeyError) as e:
        cm.dump('Error in parsing store details: %s' % url, log_name)
        return ()

    entry[cm.city_e] = cm.extract_city(entry[cm.city_e])[0]
    cm.dump('(%s / %d) Found store: %s, %s (%s, %s, %s)' % (data['brandname_e'], data['brand_id'],
                                                            entry[cm.name_e], entry[cm.addr_e], entry[cm.country_e],
                                                            entry[cm.city_e], entry[cm.continent_e]), log_name)
    db.insert_record(entry, 'stores')
    return (entry,)


def fetch_states(data):
    if data['country'] != 'US':
        d = data.copy()
        d['state'] = None
        return (d,)
        # return ()

    url = data['url']
    try:
        body = cm.get_data(url)
    except Exception, e:
        cm.dump('Error in fetching states: %s' % url, log_name)
        return ()

    results = []
    try:
        for item in pq(pq(body)('.table1')[0])('td')('a'):
            d = data.copy()
            d['state'] = item.text.strip().upper()
            # d['url'] = data['host'] + item.attrib['href']
            d['url'] = '%s/store/list_state/187/%s/Louis-Vuitton-(LV)-store-locations' % (data['host'], item.text)
            results.append(d)
        return tuple(results)
    except (IndexError, KeyError) as e:
        cm.dump('Error in fetching states: %s' % url, log_name)
        return ()


def fetch_store_list(data):
    url = data['url']
    try:
        body = cm.get_data(url)
    except Exception, e:
        cm.dump('Error in fetching store list: %s' % url, log_name)
        return ()

    results = []
    try:
        for city_sub in pq(body)('.table1'):
            for store in pq(city_sub)('tr'):
                d = data.copy()
                cols = pq(store)('td')
                if len(cols) == 0:
                    continue
                tmp = pq(cols[0])('a')
                if len(tmp) > 0:
                    tmp = tmp[0]
                else:
                    continue
                d['store_name'] = cm.html2plain(tmp.text).strip() if tmp.text else ''
                if not tmp.attrib['href']:
                    continue
                d['url'] = data['host'] + tmp.attrib['href']
                d['city'] = cm.extract_city(cols[1].text)[0] if len(cols) >= 2 and cols[1].text else None
                d['addr'] = cm.reformat_addr(cols[2].text) if len(cols) >= 3 and cols[2].text else None
                d['tel'] = cols[3].text.strip() if len(cols) >= 4 and cols[3].text else None

                if d['url'] not in id_set:
                    results.append(d)
                else:
                    cm.dump('%s already exists.' % d['store_name'], log_name)

        return tuple(results)
    except (IndexError, KeyError) as e:
        cm.dump('Error in fetching store list: %s' % url, log_name)
        return ()


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
            # 州列表
            return [{'func': lambda data: func(data, level + 1), 'data': s} for s in fetch_states(data)]
        if level == 2:
            # 商店列表
            return [{'func': lambda data: func(data, level + 1), 'data': s} for s in fetch_store_list(data)]
        if level == 3:
            # 商店
            return [{'func': None, 'data': s} for s in fetch_store_details(data)]
        else:
            return ()

    # Walk from the root node, where level == 1.
    if data is None:
        data = {
        'url': 'http://ec2-204-236-232-240.compute-1.amazonaws.com/store/listing/187/Louis-Vuitton-%28LV%29-store-locations',
        'host': 'http://ec2-204-236-232-240.compute-1.amazonaws.com',
        'brand_id': 10226, 'brandname_e': u'Louis Vuitton', 'brandname_c': u'路易威登'}

    global db, id_set
    db = cm.StoresDb()
    db.connect_db(user=user, passwd=passwd)
    db.execute(u'DELETE FROM %s WHERE brand_id=%d' % ('stores', data['brand_id']))
    rs = db.query_all(
        'SELECT distinct url FROM stores WHERE brand_id=%d' % data['brand_id'])
    id_set = [tmp[0] for tmp in ([tmp[0] for tmp in rs])]

    results = cm.walk_tree({'func': lambda data: func(data, 0), 'data': data})
    db.disconnect_db()
    cm.dump('Done!', log_name)

    return results


