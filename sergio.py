# coding=utf-8
import json
import string
import re
import common as cm
import geosense as gs

__author__ = 'Zephyre'

db = None


def fetch_continents(data):
    """
    获得洲列表
    :param data:
    :return:
    """
    url = data['home_url']
    try:
        html = cm.get_data(url)
    except Exception:
        print 'Error occured: %s' % url
        dump_data = {'level': 0, 'time': cm.format_time(), 'data': {'url': url}, 'brand_id': data['brand_id']}
        cm.dump(dump_data)
        return []

    continents_list = []
    for m in re.findall(ur'<option value="(\d+).*?">(.+?)</option>', html):
        if m[0] != '0':
            d = data.copy()
            d['continent_id'] = string.atoi(m[0])
            d['continent_e'] = m[1].strip().upper()
            continents_list.append(d)
    return continents_list


def fetch_countries(data):
    """
    获得国家列表
    :param data:
    """
    url = data['post_url']
    try:
        html = cm.post_data(url, {'pid': data['continent_id'], 'lang': 'en', 'action': 'popola_select'})
    except Exception:
        print 'Error occured in getting country list: %s' % url
        dump_data = {'level': 1, 'time': cm.format_time(), 'data': {'url': url}, 'brand_id': data['brand_id']}
        cm.dump(dump_data)
        return []

    country_list = []
    for m in re.findall(ur'<option value="(\d+)".*?>(.+?)</option>', html):
        if m[0]!='0':
            d = data.copy()
            d['country_id'] = string.atoi(m[0])
            d['country_e'] = m[1].strip().upper()
            country_list.append(d)
    return country_list


def fetch_cities(data):
    """
    获得城市列表
    :param data:
    """
    url = data['post_url']
    try:
        html = cm.post_data(url, {'pid': data['country_id'], 'lang': 'en', 'action': 'popola_select_city'})
    except Exception:
        print 'Error occured in getting city list: %s' % url
        dump_data = {'level': 2, 'time': cm.format_time(), 'data': {'url': url}, 'brand_id': data['brand_id']}
        cm.dump(dump_data)
        return []

    city_list = []
    for m in re.findall(ur'<option value="(\d+)".*?>(.+?)</option>', html):
        if m[0]!='0':
            d = data.copy()
            d['city_id'] = string.atoi(m[0])
            d['city_e'] = m[1].strip().upper()
            city_list.append(d)
    return city_list


def fetch_stores(data):
    """
    获得商店信息
    :param data:
    """
    url = data['post_url']
    try:
        html = cm.post_data(url, {'pid': data['city_id'], 'lang': 'en', 'action': 'popola_box_DX'})
    except Exception:
        print 'Error occured in getting city list: %s' % url
        dump_data = {'level': 2, 'time': cm.format_time(), 'data': {'url': url}, 'brand_id': data['brand_id']}
        cm.dump(dump_data)
        return []

    store_list = []
    for m in re.finditer(ur'<a href="(.+?)".*?>', html):
        entry = cm.init_store_entry(data['brand_id'], data['brandname_e'], data['brandname_c'])
        entry[cm.url] = m.group(1)
        store_html, start, end = cm.extract_closure(html[m.start():], ur'<a href', ur'</a>')
        if end == 0:
            continue
        m1 = re.findall(ur'<h3 class="titleShop">(.+?)</h3>', store_html, re.S)
        if len(m1) > 0:
            entry[cm.name_e] = m1[0].strip()
        m1 = re.findall(ur'<p\b.*?>(.+?)(?:</p>|</div>)', store_html, re.S)
        if len(m1) > 0:
            terms = cm.reformat_addr(m1[0]).split(',')
            tel = cm.extract_tel(terms[-1])
            if tel != '':
                terms = terms[:-1]
                entry[cm.tel] = tel
            entry[cm.addr_e] = ', '.join([v.strip() for v in terms])

        entry['country_e'] = data['country_e']
        entry['city_e'] = data['city_e']
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
            # 国家
            return [{'func': lambda data: func(data, 1), 'data': c} for c in fetch_continents(data)]
        elif level == 1:
            # 国家列表
            return [{'func': lambda data: func(data, 2), 'data': s} for s in fetch_countries(data)]
        elif level == 2:
            # 城市列表
            return [{'func': lambda data: func(data, 3), 'data': s} for s in fetch_cities(data)]
        elif level == 3:
            # 商店的具体信息
            return [{'func': None, 'data': s} for s in fetch_stores(data)]
        else:
            return []

    # Walk from the root node, where level == 1.
    if data is None:
        data = {'home_url': 'http://www.sergiorossi.com/experience/en/pages/stores/',
                'post_url': 'http://www.sergiorossi.com/experience/en/wpapi/store-services/',
                'brand_id': 10316, 'brandname_e': u'Sergio Rossi', 'brandname_c': u'塞乔·罗西'}

    global db
    db = cm.StoresDb()
    db.connect_db(user=user, passwd=passwd)
    db.execute(u'DELETE FROM %s WHERE brand_id=%d' % ('stores', data['brand_id']))

    results = cm.walk_tree({'func': lambda data: func(data, 0), 'data': data})
    db.disconnect_db()

    return results