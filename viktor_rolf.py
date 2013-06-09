# coding=utf-8
import re
import common
import geosense as gs

__author__ = 'Zephyre'

brand_id = 10377
brandname_e = 'Viktor & Rolf'
brandname_c = u'维果罗夫'
url = 'http://www.viktor-rolf.com/storelocator/fashion/'


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
    def func(data, level):
        """
        :param data:
        :param level: 1: 洲；2：国家；3：城市；4：商店
        """
        stores = fetch_stores(data)
        return [{'func': None, 'data': s} for s in stores]

    global db
    db = common.StoresDb()
    db.connect_db(user=user, passwd=passwd)
    db.execute(u'DELETE FROM %s WHERE brand_id=%d' % ('stores', brand_id))
    # Walk from the root node, where level == 1.
    if data is None:
        data = {'url': url}
    results = common.walk_tree({'func': lambda data: func(data, 1), 'data': data})
    db.disconnect_db()
    return results