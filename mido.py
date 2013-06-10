# coding=utf-8
import json
import string
import re
import common as cm
import geosense as gs

__author__ = 'Zephyre'

db = None


def fetch_stores(data):
    """
    商店列表
    :param data:
    """
    html = data['html']

    store_list = []
    while True:
        entry = cm.init_store_entry(data['brand_id'], data['brandname_e'], data['brandname_c'])
        m = re.search(ur'<li class="leaf end"><div><u>(.+?)</u>', html)
        if m is None:
            break
        html = html[m.start():]
        entry[cm.name_e] = m.group(1)

        sub, start, end = cm.extract_closure(html, ur'<li\b', '</li>')
        html = html[end:]

        # 单个商店的页面
        sub = sub[len(m.group(0)):-len('</li>')]

        m = re.search(ur'<a href="(http.+?)"', sub)
        if m is not None:
            entry[cm.url] = m.group(1)
        m = re.search(ur'<a href="mailto:(.+?)"', sub)
        if m is not None:
            entry[cm.email] = m.group(1)
        m = re.search(ur'(?:<a\b|</div>)', sub)
        if m is not None:
            addr = sub[:m.start()]
        else:
            addr = sub
            # 解析地址栏
        addr = cm.reformat_addr(addr)
        terms = addr.split(',')
        new_terms = []
        for t in terms:
            if re.search(ur'phone', t, re.IGNORECASE) is not None:
                entry[cm.tel] = cm.extract_tel(t)
            elif re.search(ur'fax', t, re.IGNORECASE) is not None:
                entry[cm.fax] = cm.extract_tel(t)
            elif data['city_e'] in t.strip().upper():
                # 邮编
                m = re.search(ur'\d+', t)
                if m is not None:
                    entry[cm.zip_code] = m.group(0)
            else:
                new_terms.append(t)
        entry[cm.addr_e] = ', '.join(new_terms)
        if cm.city_e in data:
            entry[cm.city_e] = data['city_e']
        if cm.city_c in data:
            entry[cm.city_c] = data['city_c']
        if cm.country_e in data:
            entry[cm.country_e] = data['country_e']
        if cm.country_c in data:
            entry[cm.country_e] = data['country_c']
        gs.field_sense(entry)

        print '(%s / %d) Found store: %s, %s (%s, %s)' % (data['brandname_e'], data['brand_id'],
                                                          entry[cm.name_e], entry[cm.addr_e], entry[cm.country_e],
                                                          entry[cm.continent_e])
        store_list.append(entry)
        db.insert_record(entry, 'stores')

    return store_list


def fetch_cities(data):
    """
    城市列表
    :param data:
    """
    html = data['html']

    store_list = []
    while True:
        m = re.search(ur'<li class="expanded"><a href=".*?">(.+?)</a><br\s*?/>', html)
        if m is None:
            break
        html = html[m.start():]

        sub, start, end = cm.extract_closure(html, ur'<li\b', '</li>')
        html = html[end:]

        d = data.copy()
        d['html'] = sub[len(m.group(0)):-len('</li>')]
        terms = m.group(1).strip().upper().split(' ')
        if len(terms) > 1 and cm.is_chinese(terms[-1]):
            d['city_c'] = terms[-1].strip()
            terms = terms[:-1]
        d['city_e'] = ' '.join(terms)
        print 'Processing %s' % d['city_e']
        store_list.extend(fetch_stores(d))

    return store_list


def fetch_countries(data):
    """
    国家列表
    :param data:
    """
    html = data['html']

    store_list = []
    while True:
        m = re.search(ur'<li class="expanded"><a href=".*?">(.+?)</a><br\s*?/>', html)
        if m is None:
            break
        html = html[m.start():]

        sub, start, end = cm.extract_closure(html, ur'<li\b', '</li>')
        html = html[end:]

        d = data.copy()
        d['html'] = sub[len(m.group(0)):-len('</li>')]
        d['country_e'] = m.group(1).strip().upper()
        print 'Processing %s' % d['country_e']
        store_list.extend(fetch_cities(d))

    return store_list


def fetch(level=1, data=None, user='root', passwd=''):
    # Walk from the root node, where level == 1.
    if data is None:
        data = {'url': 'http://www.mido.cn/zh/retailer_li/POS',
                'brand_id': 10260, 'brandname_e': u'MIDO', 'brandname_c': u'美度'}

    global db
    db = cm.StoresDb()
    db.connect_db(user=user, passwd=passwd)
    db.execute(u'DELETE FROM %s WHERE brand_id=%d' % ('stores', data['brand_id']))

    url = data['url']
    try:
        data['html'] = cm.get_data(url)
    except Exception:
        print 'Error occured: %s' % url
        dump_data = {'level': 0, 'time': cm.format_time(), 'data': {'url': url}, 'brand_id': data['brand_id']}
        cm.dump(dump_data)
        return []

    store_list = fetch_countries(data)

    db.disconnect_db()