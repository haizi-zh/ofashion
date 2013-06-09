# coding=utf-8
import re
import string
import common
import geosense as gs

__author__ = 'Zephyre'

brand_id = 10377
brandname_e = 'Viktor & Rolf'
brandname_c = u'维果罗夫'
url_fashion = 'http://www.viktor-rolf.com/storelocator/fashion/'
url_fragrance = 'http://www.viktor-rolf.com/storelocator/fragrances/'


def fetch_stores(data):
    """
    获得门店信息
    :param data:
    :return:
    """
    url = data['url']
    try:
        html = common.get_data(data['url'])
    except Exception:
        print 'Error occured: %s' % url
        dump_data = {'level': 1, 'time': common.format_time(), 'data': {'url': url}, 'brand_id': brand_id}
        common.dump(dump_data)
        return []

    # 第二个<ul>...</ul>
    start = 0
    for i in xrange(2):
        start = html.find('<ul>', start)
        if start == -1:
            return []
        start += len('<ul>')
    end = html.find('</ul>', start)
    html = html[start:end]

    store_list = []
    for m in re.findall(ur'<li>(.+?)</li>', html, re.S):
        entry = common.init_store_entry(brand_id, brandname_e, brandname_c)
        entry[common.store_type] = 'FASHION'
        m1 = re.findall(ur'<h2>(.+?)</h2>', m)
        if len(m1) > 0:
            entry[common.name_e] = common.reformat_addr(m1[0])

        # Google Maps网址
        m1 = re.findall(ur'href="(https://maps.google.com/maps[^\s]+?)"', m)
        if len(m1) > 0:
            entry[common.url] = m1[0]

        addr = common.reformat_addr('\n\r'.join([m1 for m1 in re.findall(ur'<p>(.+?)</p>', m)]))
        entry[common.addr_e] = addr
        terms = addr.split(',')

        # 是否所有的geosensing都未命中？
        hit_flag = False

        # 最后一项是否为国家
        country = ''
        ret = gs.look_up(terms[-1], 1)
        if ret is not None:
            entry[common.country_e] = ret['name_e']
            country = ret['name_e']
            terms = terms[:-1]
            hit_flag = True

        # 查找州和城市
        m = re.match(ur'.*(\d{5,})', terms[-1])
        zip_cdt = ''
        if m is not None:
            zip_cdt = m.group(1)
        tmp = re.sub(ur'\d{5,}', '', terms[-1]).strip().upper()
        ret = gs.look_up(terms[-1], 2)
        if ret is not None:
            entry[common.province_e] = ret['name_e']
            entry[common.zip_code] = zip_cdt
            terms = terms[:-1]
            hit_flag = True

        ret = gs.look_up(terms[-1], 3)
        if ret is not None:
            entry[common.city_e] = ret['name_e']
            entry[common.zip_code] = zip_cdt
            hit_flag = True

        if not hit_flag:
            # 所有都未命中，输出：
            common.write_log('Failed in geosensing: %s' % addr)

        gs.field_sense(entry)

        print '%s Found store: %s, %s (%s, %s)' % (
            brandname_e, entry[common.name_e], entry[common.addr_e], entry[common.country_e],
            entry[common.continent_e])
        db.insert_record(entry, 'stores')
        store_list.append(entry)

    return store_list


def fetch(level=1, data=None, user='root', passwd=''):
    global db
    db = common.StoresDb()
    db.connect_db(user=user, passwd=passwd)
    db.execute(u'DELETE FROM %s WHERE brand_id=%d' % ('stores', brand_id))

    fetch_fashion(level, data, user, passwd)
    fetch_fragrance(level, data, user, passwd)

    db.disconnect_db()


def get_frag_countries(url):
    # 获得国家代码
    """
    获得国家的名字和代码
    :rtype : [{'id':**, 'country':**}, ...]
    :param url:
    :return:
    """
    try:
        html = common.get_data(url)
    except Exception:
        print 'Error occured: %s' % url_fragrance
        dump_data = {'level': 1, 'time': common.format_time(), 'data': {'url': url_fragrance},
                     'brand_id': brand_id}
        common.dump(dump_data)
        return [], False

    start = html.find('<select name="country" id="id_country">')
    if start == -1:
        return [], False
    sub, s, e = common.extract_closure(html[start:], ur'<select\b', ur'</select>')
    if e == 0:
        return [], False
    return [{'id': string.atoi(m[0]), 'country': m[1].strip().upper()}
            for m in re.findall(ur'<option value="(\d+)".*?>(.+?)</option>', sub)]


def get_frag_stores(data):
    try:
        html = common.get_data(data['url'], {'country': data['country'], 'city_postal': '', 'page': data['page']})
    except Exception:
        print 'Error occured: %s' % url_fragrance
        dump_data = {'level': 1, 'time': common.format_time(), 'data': {'url': url_fragrance},
                     'brand_id': brand_id}
        common.dump(dump_data)
        return [], False

    print 'PARSING PAGE: %d' % data['page']
    start = html.find('<section id="content" class="content">')
    if start == -1:
        return [], False
    html, start, end = common.extract_closure(html[start:], ur'<section\b', ur'</section>')
    if end == 0:
        return [], False

    # 找到总页面数量
    tot_page = 0
    start = html.find('<div class="pagination">')
    if start != -1:
        pagination, start, end = common.extract_closure(html[start:], ur'<div\b', ur'</div>')
        m = re.findall(ur'<a href=".*?" class="page">(\d+)</a>', pagination)
        if len(m) > 0:
            tot_page = string.atoi(m[-1])

    # 开始寻找门店
    store_list = []
    for m in re.findall(ur'<li>(.*?)</li>', html, re.S):
        entry = common.init_store_entry(brand_id, brandname_e, brandname_c)
        entry[common.store_type] = 'FRAGRANCE'
        m1 = re.findall(ur'<h2>(.+?)</h2>', m)
        if len(m1) > 0:
            entry[common.name_e] = common.html2plain(m1[0].strip())

        m1 = re.findall(ur'href="(.+?)"', m)
        if len(m1) > 0:
            entry[common.url] = m1[0]

        addr = common.reformat_addr(','.join(re.findall(ur'<p>(.+?)</p>', m)))
        entry[common.addr_e] = addr
        terms = addr.split(', ')
        ret = gs.look_up(terms[-1], 1)
        if ret is not None:
            entry[common.country_e] = ret['name_e']

        if len(terms)>=2:
            m1 = re.match(ur'.*?(\d+)\s+(.*)', terms[-2])
            if m1 is not None:
                ret = gs.look_up(m1.group(2).strip().upper(), 3)
                if ret is not None:
                    entry[common.city_e] = ret['name_e']
                else:
                    if len(re.findall('(\S+)', m1.group(2).strip().upper()))<3 and \
                                    len(re.findall('(\d+)', m1.group(2).strip().upper()))==0:
                        entry[common.city_e] = m1.group(2).strip().upper()
                        entry[common.zip_code] = m1.group(1).strip()

        gs.field_sense(entry)
        print '(%s / %d) Found store: %s, %s (%s, %s)' % (
            brandname_e, brand_id, entry[common.name_e], entry[common.addr_e], entry[common.country_e],
            entry[common.continent_e])
        db.insert_record(entry, 'stores')
        store_list.append(entry)

    if tot_page > data['page']:
        # more
        d = dict(data)
        d['page'] = data['page'] + 1
        store_list.extend(get_frag_stores(d))
        return store_list
    else:
        return store_list


def fetch_fragrance(level=1, data=None, user='root', passwd=''):
    country_list = get_frag_countries(url_fragrance)
    # country_list = [{'id': 2, 'country': 'Netherlands'},
    #                 {'id':4,'country':'Luxembourg'}]
    store_list = []
    for c in country_list:
        print 'Fetching %s' % c['country']
        stores = get_frag_stores({'page': 1, 'country': c['id'], 'url': url_fragrance})
        store_list.extend(stores)

    return store_list


def fetch_fashion(level=1, data=None, user='root', passwd=''):
    def func(data, level):
        """
        :param data:
        :param level: 1: 洲；2：国家；3：城市；4：商店
        """
        stores = fetch_stores(data)
        return [{'func': None, 'data': s} for s in stores]

    # Walk from the root node, where level == 1.
    if data is None:
        data = {'url': url_fashion}
    results = common.walk_tree({'func': lambda data: func(data, 1), 'data': data})

    return results