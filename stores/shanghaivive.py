# coding=utf-8
import re
from stores import geosense as gs

__author__ = 'Zephyre'

db = None


def fetch_cities(data):
    url = data['url']
    try:
        body = cm.get_data(url)
    except Exception:
        cm.dump('Error in fetching cities: %s' % url, 'shanghaivive_log.txt')
        dump_data = {'level': 0, 'time': cm.format_time(), 'data': {'url': url}, 'brand_id': data['brand_id']}
        cm.dump(dump_data)
        return []

    values = [{'city': u'上海', 'code': '021'}, {'city': u'北京', 'code': '010'}, {'city': u'成都', 'code': '028'}]
    results = []
    for v in values:
        d = data.copy()
        d['city'] = v['city']
        d['code'] = v['code']
        d['body'] = body
        results.append(d)
    return results


def fetch_stores(data):
    body = data['body']
    start = body.find(u'<ul class="storelist storelist_%s' % data['code'])
    if start == -1:
        cm.dump('Error in finding stores for %s' % data['code'])
        return []
    body = cm.extract_closure(body[start:], ur'<ul\b', ur'</ul>')[0]

    store_list = []
    for m in re.findall(ur'<li class="sitem">(.+?)</li>', body, re.S):
        entry = cm.init_store_entry(data['brand_id'], data['brandname_e'], data['brandname_c'])
        m1 = re.search(ur'<h3>(.+?)</h3>', m)
        if m1 is not None:
            entry[cm.name_c] = m1.group(1).strip()
        m1 = re.search(ur'<div class="addr">(.+?)</div>', m)
        if m1 is not None:
            entry[cm.addr_e] = m1.group(1).replace(u'地址:', '').replace(u'地址：', '').strip()
        m1 = re.search(ur'<div class="tel">(.+?)</div>', m)
        if m1 is not None:
            entry[cm.tel] = m1.group(1).replace(u'电话:', '').replace(u'电话：', '').strip()
        entry[cm.city_c] = data['city']
        ret = gs.look_up(data['city'], 3)
        if ret is not None:
            entry[cm.city_e] = ret['name_e']
            entry[cm.city_c] = ret['name_c']
            if ret['province'] != '':
                entry[cm.province_e] = ret['province']['name_e']
        entry[cm.country_e] = u'CHINA'
        gs.field_sense(entry)
        cm.dump('(%s / %d) Found store: %s, %s (%s, %s)' % (data['brandname_e'], data['brand_id'],
                                                            entry[cm.name_e], entry[cm.addr_e], entry[cm.country_e],
                                                            entry[cm.continent_e]), 'canali_log.txt')
        db.insert_record(entry, 'stores')
        store_list.append(entry)

    return store_list


def fetch(level=1, data=None, user='root', passwd=''):
    def func(data, level):
        """
        :param data:
        :param level: 0：国家；1：城市；2：商店列表
        """
        if level == 0:
            # 城市列表
            return [{'func': lambda data: func(data, 1), 'data': s} for s in fetch_cities(data)]
        if level == 1:
            # 商店列表
            return [{'func': None, 'data': s} for s in fetch_stores(data)]
        else:
            return []

    # Walk from the root node, where level == 1.
    if data is None:
        data = {'url': 'http://www.shanghaivive.com.cn/store.jsp',
                'brand_id': 10318, 'brandname_e': u'ShanghaiVIVE', 'brandname_c': u'双妹'}

    global db
    db = cm.StoresDb()
    db.connect_db(user=user, passwd=passwd)
    db.execute(u'DELETE FROM %s WHERE brand_id=%d' % ('stores', data['brand_id']))

    results = cm.walk_tree({'func': lambda data: func(data, 0), 'data': data})
    db.disconnect_db()

    return results