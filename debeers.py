# coding=utf-8
import json
import string
import re
import common as cm
import geosense as gs

__author__ = 'Zephyre'

db = None


def get_stores(url, type, opt):
    """
    获得洲，城市等信息
    """
    host = 'http://www.debeers.com.cn'
    if type == 0:
        url = host + '/stores'
    html = common.get_data(url)

    if type == 0:
        # 洲列表
        pat_s = '<ul class="tab-link-set">'
        pat_e = '</ul>'
        pat_entry = r'<a class="tab-link-a" href="/stores/(\w+)" title="([^\s]+?)">'
        entries = []

        start = html.find(pat_s)
        if start == -1:
            return []
        end = html.find(pat_e, start) + pat_e.__len__()
        html = html[start:end]

        for m in re.finditer(pat_entry, html, re.U):
            c_code = m.group(1)
            c_name = m.group(2)
            c_url = url + '/%s' % c_code
            entries.append({'type': 0, 'code': c_code, 'name': c_name, 'url': c_url})

        stores = []
        for e in entries:
            stores.extend(get_stores(e['url'], 1, {'continent': e['name']}))
        return stores

    elif type == 1:
        # 店铺列表
        pat_s = '<ul class="store-link-set">'
        pat_e = '</ul>'
        pat_entry = r'<a href="([^\s]+?)" title="(.+?)" class="store-link-a">'

        entries = []
        start = html.find(pat_s)
        if start == -1:
            return []
        end = html.find(pat_e, start) + pat_e.__len__()
        html = html[start:end]

        for m in re.finditer(pat_entry, html, re.U):
            c_url = host + m.group(1)
            c_name = m.group(2)
            entries.append({'type': 1, 'name': c_name, 'url': c_url})

        stores = []
        for e in entries:
            opt['url'] = e['url']
            stores.extend(get_stores(e['url'], 2, opt))
        return stores
    elif type == 2:
        # 店铺信息
        pat_s = '<div class="store-details">'
        pat_e = '<div class="share">'
        start = html.find(pat_s)
        if start == -1:
            return []
        end = html.find(pat_e, start)
        html = html[start:end]

        store = {'type': 2}
        store['url'] = opt['url']
        store['continent'] = opt['continent']

        m = re.findall(r'<h2 class="store-name">(.+?)</h2>', html, re.U)
        if m is not None:
            store['name'] = m[0].strip()

        start = html.find(u'<h3>营业时间</h3>')
        if not start == -1:
            start += u'<h3>营业时间</h3>'.__len__()
            end = html.find('</div>', start)
            hour_str = html[start:end].strip()
            store['hours'] = hour_str

        start = html.find('<div class="store-address">')
        if not start == -1:
            end = html.find('</div>', start)
            addr_src = html[start:end].strip()

            m = re.findall(r'<p class="store-phone">\s*(.*?)\s*</p>', addr_src, re.S)
            if m is not None:
                store['tel'] = m[0]
            m = re.findall(r'<p class="store-fax">\s*(.*?)\s*</p>', addr_src, re.S)
            if m is not None:
                store['fax'] = m[0]
            m = re.findall(r'<p class="store-email">\s*(.*?)\s*</p>', addr_src, re.S)
            if m is not None:
                store['email'] = m[0]

            start = addr_src.find(u'<h3>地址</h3>')
            if not start == -1:
                start += u'<h3>地址</h3>'.__len__()
                end = addr_src.find('<p', start)
                addr_src = common.reformat_addr(addr_src[start:end])
                store['addr'] = addr_src

        print('Found store: %s, %s, %s, (%s)' % (store['name'], store['addr'], store['tel'], store['continent']))
        return [store]


def fetch1():
    stores = get_stores(None, 0, None)
    return stores


def fetch_continents(data):
    values = [{'continent': 'ASIA', 'url': 'http://www.debeers.com.cn/stores/as'},
              {'continent': 'EUROPE', 'url': 'http://www.debeers.com.cn/stores/eu'},
              {'continent': 'MIDDLE EAST', 'url': 'http://www.debeers.com.cn/stores/me'},
              {'continent': 'NORTH AMERICA', 'url': 'http://www.debeers.com.cn/stores/na'}]
    results = []
    for v in values:
        d = data.copy()
        d['continent'] = v['continent']
        d['url'] = v['url']
        results.append(d)
    return results


def fetch_store_list(data):
    url = data['url']
    try:
        body = cm.get_data(url)
    except Exception:
        cm.dump('Error in fetching store list: %s' % url, 'debeers_log.txt')
        return []

    start = body.find(u'<ul class="store-link-set">')
    if start == -1:
        cm.dump('Error in fetching store list: %s' % url, 'debeers_log.txt')
    body = cm.extract_closure(body[start:], ur'<ul\b', ur'</ul>')[0]

    results = []
    for m in re.findall(ur'<li .*?>\s*<a href="(.+?)" title="(.+?)"', body):
        d = data.copy()
        d['url'] = data['host'] + m[0]
        d['name'] = m[1].strip()
        results.append(d)
    return results


def fetch_store_details(data):
    url = data['url']
    try:
        body = cm.get_data(url)
    except Exception:
        cm.dump('Error in fetching store details: %s' % url, 'debeers_log.txt')
        return []

    start = body.find(u'<div class="store-details">')
    if start == -1:
        cm.dump('Error in fetching store details: %s' % url, 'debeers_log.txt')
    body = cm.extract_closure(body[start:], ur'<div\b', ur'</div>')[0]

    entry = cm.init_store_entry(data['brand_id'], data['brandname_e'], data['brandname_c'])

    m = re.search(ur'<h2 class="store-name">(.+?)</h2>', body)
    if m is not None:
        entry[cm.name_e] = m.group(1).strip()

    m_addr = re.search(ur'<div class="store-address">(.+?)</div>', body, re.S)
    if m_addr is not None:
        addr = m_addr.group(1).strip()
        pat_tel = re.compile(ur'<p class="store-phone">(.+?)</p>', re.S)
        pat_fax = re.compile(ur'<p class="store-fax">(.+?)</p>', re.S)
        pat_email = re.compile(ur'<p class="store-email">(.+?)</p>', re.S)

        m = re.search(pat_tel, addr)
        if m is not None:
            entry[cm.tel] = m.group(1).strip()

        m = re.search(pat_fax, addr)
        if m is not None:
            entry[cm.fax] = m.group(1).strip()

        m = re.search(pat_email, addr)
        if m is not None:
            entry[cm.email] = m.group(1).strip()

        addr = re.sub(pat_tel, '', addr)
        addr = re.sub(pat_fax, '', addr)
        addr = re.sub(pat_email, '', addr)
        addr = re.sub(u'<h3>.+?</h3>', '', addr)
        addr = cm.reformat_addr(addr)
        entry[cm.addr_e] = addr
        country, province, city = gs.addr_sense(addr)
        if country is not None:
            entry[cm.country_e] = country
        if province is not None:
            entry[cm.province_e] = province
        if city is not None:
            entry[cm.city_e] = city

    m = re.search(ur'<div class="store-hours">(.+?)</div>', body, re.S)
    if m is not None:
        entry[cm.hours] = cm.reformat_addr(m.group(1))

    gs.field_sense(entry)
    cm.dump('(%s / %d) Found store: %s, %s (%s, %s)' % (data['brandname_e'], data['brand_id'],
                                                        entry[cm.name_e], entry[cm.addr_e], entry[cm.country_e],
                                                        entry[cm.continent_e]), 'benetton_log.txt', False)
    db.insert_record(entry, 'stores')

    return [entry]


def fetch(level=1, data=None, user='root', passwd=''):
    def func(data, level):
        """
        :param data:
        :param level: 0：国家；1：城市；2：商店列表
        """
        if level == 0:
            # 洲列表
            return [{'func': lambda data: func(data, 1), 'data': s} for s in fetch_continents(data)]
        if level == 1:
            # 商店列表
            return [{'func': lambda data: func(data, 2), 'data': s} for s in fetch_store_list(data)]
        if level == 2:
            # 商店列表
            return [{'func': None, 'data': s} for s in fetch_store_details(data)]
        else:
            return []

    # Walk from the root node, where level == 1.
    if data is None:
        data = {'home_url': 'http://www.debeers.com/stores',
                'host': 'http://www.debeers.com',
                'brand_id': 10100, 'brandname_e': u'De Beers', 'brandname_c': u'戴比尔斯'}

    global db
    db = cm.StoresDb()
    db.connect_db(user=user, passwd=passwd)
    db.execute(u'DELETE FROM %s WHERE brand_id=%d' % ('stores', data['brand_id']))

    results = cm.walk_tree({'func': lambda data: func(data, 0), 'data': data})
    db.disconnect_db()

    return results