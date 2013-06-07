# coding=utf-8
import re
import common

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
        entry = common.init_store_entry(brand_id)
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
        # 最后一项是否为国家
        country_cdt = ''
        city_cdt = ''
        zip_cdt = ''
        if ',' in terms[-1]:
            # 可能为TOKYO, JAPAN这种形式
            tmp1 = terms[-1].split(',')
            tmp2 = re.findall(ur'([\w ]+)', common.reformat_addr(tmp1[0]))
            if len(tmp2) > 0:
                city_cdt = tmp2[0].strip()
                country_cdt = common.reformat_addr(tmp1[1])
        else:
            country_cdt = terms[-1]

        # 可能为 12345 Paris这种形式
        tmp1 = re.findall(ur'(\d{4,})\s+([\w ]+)', common.reformat_addr(terms[-2]))
        if len(tmp1) > 0:
            zip_cdt = tmp1[0][0]
            city_cdt = tmp1[0][1].strip()

        ret = common.geo_translate(country_cdt.strip())
        if len(ret) > 0:
            common.update_entry(entry, {common.continent_c: ret[common.continent_c],
                                        common.continent_e: ret[common.continent_e],
                                        common.country_c: ret[common.country_c],
                                        common.country_e: ret[common.country_e]})
        common.update_entry(entry, {common.brandname_c: brandname_c, common.brandname_e: brandname_e,
                                    common.zip_code: zip_cdt})
        # common.city_e: city_cdt})
        common.chn_check(entry)

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
    # Walk from the root node, where level == 1.
    if data is None:
        data = {'url': url}
    results = common.walk_tree({'func': lambda data: func(data, 1), 'data': data})
    db.disconnect_db()
    return results