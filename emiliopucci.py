# coding=utf-8
import hashlib
import json
import random
import string
import sys

__author__ = 'Zephyre'

import re
import common as cm
import geosense as gs

db = None
url = 'http://home.emiliopucci.com/boutiques'
brand_id = 10117
brandname_e = u'Emilio Pucci'
brandname_c = u'璞琪'


def get_countries(url):
    """
    获得洲和国家列表
    :rtype : {'Asia':[{'country_id':884,'country':'brazil'}]}
    """
    try:
        html = cm.get_data(url)
    except Exception:
        print 'Error occured: %s' % url
        dump_data = {'level': 1, 'time': cm.format_time(), 'data': {'data': url}, 'brand_id': brand_id}
        cm.dump(dump_data)
        return {}

    # 开始解析
    start = html.find('<select name="country_id" id="country_id">')
    if start == -1:
        return []
    end = html.find('</select>', start)
    html = html[start:end]

    districts = {}
    start = 0
    while True:
        # 获得洲信息
        start = html.find('<optgroup', start)
        if start == -1:
            break
        end = html.find('</optgroup>', start) + '</optgroup>'.__len__()
        con = html[start:end]
        start = end

        m = re.findall(r'<optgroup label="([\w\s]+)">', con, re.S)
        if m is None:
            continue
        continent = m[0]

        itor = re.finditer(r'<option value="(\d+)">([\w\s]+)</option>', con, re.S)
        countries = []
        for m in itor:
            country_id = string.atoi(m.group(1))
            country = m.group(2)
            countries.append({'country': country, 'country_id': country_id})
        districts[continent] = countries
    return districts


def fetch_cities(data):
    country_id = data['country_id']
    try:
        html = cm.post_data(url, {'country_id': country_id})
    except Exception:
        print 'Error occured: %s' % url
        dump_data = {'level': 2, 'time': cm.format_time(), 'data': data, 'brand_id': brand_id}
        cm.dump(dump_data)
        return []

    start = html.find('<select name="city_id" id="city_id">')
    if start == -1:
        return []
    start += len('<select name="city_id" id="city_id">')
    end = html.find('</select>', start)
    html = html[start:end]
    city_list = []
    for m in re.findall(ur'<option\s.*?value="(\d+).*?">(.*?)</option>', html):
        entry = {'city': m[1].strip().upper(), 'city_id': string.atoi(m[0])}
        entry['country'] = data['country']
        entry['country_id'] = data['country_id']
        city_list.append(entry)

        # # 副产品
        # cname=entry['city']
        # country = gs.look_up(data['country'], 1)['name_e']
        # country_guid = gs.country_map['lookup'][country]
        # if cname not in gs.city_map['lookup']:
        #     citem = {'country':country_guid, 'province':'', 'name_e':cname, 'name_c':''}
        #     sha = hashlib.sha1()
        #     while True:
        #         sha.update(str(random.randint(0, sys.maxint)))
        #         guid = ''.join(['%x' % ord(v) for v in sha.digest()])
        #         if guid not in gs.city_map['data']:
        #             gs.city_map['data'][guid] = citem
        #             gs.city_map['lookup'][cname] = [guid]
        #             print 'Added: %s in %s' % (citem['name_e'], country)
        #             break
    return city_list


def fetch_stores(data):
    """
    country_id: country_id
    """
    country = data['country']
    country_id = data['country_id']
    city = data['city']
    city_id = data['city_id']

    try:
        html = cm.post_data(url, {'country_id': country_id, 'city_id': city_id})
    except Exception:
        print 'Error occured: %s' % url
        dump_data = {'level': 2, 'time': cm.format_time(), 'data': data, 'brand_id': brand_id}
        cm.dump(dump_data)
        return []

    start = html.find('class="boutique_store"')
    if start == -1:
        return []
    end = html.find('</ul>', start)
    html = html[start:end]

    # <li><h6>Paris</h6><p>36 Avenue Montaigne<br />+33 1 47 20 04 45<br />France</p></li>
    stores = []
    for m in re.finditer(r'<li><h6>([^<>]+)</h6><p>(.*?)</p></li>', html):
        store_item = cm.init_store_entry(brand_id, brandname_e, brandname_c)
        # city = m.group(1)
        content = m.group(2) + r'<br />'
        addr = ''
        idx = 0
        for m1 in re.finditer(r'(.*?)<br\s*?/>', content):
            idx += 1
            # 第一个为门店名称
            if idx == 1 and re.match(ur'.*?\d+', m1.group(1)) is None:
                store_item[cm.name_e] = cm.reformat_addr(m1.group(1))
                addr += m1.group(1) + '\r\n'
            else:
                # 是否为电话？
                tel_str = cm.extract_tel(m1.group(1))
                if tel_str != '':
                    store_item[cm.tel] = tel_str
                else:
                    addr += m1.group(1) + '\r\n'

        store_item[cm.addr_e] = cm.reformat_addr(addr)
        store_item[cm.city_e] = city
        store_item[cm.country_e] = country
        gs.field_sense(store_item)

        # term = cm.geo_translate(country)
        # if len(term) == 0:
        #     print 'Error in geo translating: %s' % country
        # else:
        #     store_item[cm.continent_c] = term[cm.continent_c]
        #     store_item[cm.continent_e] = term[cm.continent_e]
        #     store_item[cm.country_c] = term[cm.country_c]
        #     store_item[cm.country_e] = term[cm.country_e]
        # store_item[cm.brandname_e] = brandname_e
        # store_item[cm.brandname_c] = brandname_c
        # cm.chn_check(store_item)
        print '%s: Found store: %s, %s (%s, %s)' % (
            brandname_e, store_item[cm.name_e], store_item[cm.addr_e], store_item[cm.country_e],
            store_item[cm.continent_e])
        db.insert_record(store_item, 'stores')
        stores.append(store_item)
    return stores


def fetch(level=1, data=None, user='root', passwd=''):
    def func(data, level):
        """
        :param data:
        :param level: 1: 国家和地区列表；2：获得单独的门店信息
        """
        if level == 1:
            district_l = get_countries(data['url'])
            siblings = []
            for c1 in district_l.keys():
                country_l = district_l[c1]
                for c2 in country_l:
                    c2['continent'] = c1
                    siblings.append({'func': lambda data: func(data, 2), 'data': c2})
            return siblings
        elif level == 2:
            city_list = fetch_cities(data)
            return [{'func': lambda data: func(data, 3), 'data': c} for c in city_list]
            # store_l = fetch_stores(data)
            # return [{'func': None, 'data': s} for s in store_l]
        elif level == 3:
            store_list = fetch_stores(data)
            return [{'func': None, 'data': s} for s in store_list]
        else:
            return []

    global db
    db = cm.StoresDb()
    db.connect_db(user=user, passwd=passwd)
    db.execute(u'DELETE FROM %s WHERE brand_id=%d' % ('stores', brand_id))

    if data is None:
        data = {'url': url}
    results = cm.walk_tree({'func': lambda data: func(data, level), 'data': data})
    db.disconnect_db()

    return results
