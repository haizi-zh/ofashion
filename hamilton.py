# coding=utf-8
import json
import string
import re
import common as cm
import geosense as gs

__author__ = 'Zephyre'

db = None


def fetch_stores(data):
    url = data['url']
    try:
        html = cm.get_data(url)
    except Exception:
        print 'Error occured: %s' % url
        dump_data = {'level': 0, 'time': cm.format_time(), 'data': {'url': url}, 'brand_id': data['brand_id']}
        cm.dump(dump_data)
        return []

    province_list = [{cm.province_c: m[1].strip().upper(), cm.url: m[0].strip()}
                     for m in re.findall(ur'<li><a href="#(fragment-\d+)"><span>(.+?)</span></a></li>', html)]

    comment_pat = re.compile(ur'<!--.*?-->', re.S)
    store_list = []

    for p in province_list:
        start = html.find('<div id="%s">' % p[cm.url])
        if start == -1:
            continue
        p_sub, start, end = cm.extract_closure(html[start:], ur'<tbody>', ur'</tbody>')
        p_sub = re.sub(comment_pat, '', p_sub)

        city_c = ''
        city_e = ''
        while True:
            s_sub, start, end = cm.extract_closure(p_sub, ur'<tr>', ur'</tr>')
            if end == 0:
                break
            p_sub = p_sub[end:]
            if u'城市' in s_sub and u'店铺名称' in s_sub:
                continue

            term_list = re.findall(ur'<td.*?>(.+?)</td>', s_sub)
            if len(term_list) < 3:
                continue

            entry = cm.init_store_entry(data['brand_id'], data['brandname_e'], data['brandname_c'])

            if len(term_list) == 4:
                city_c = term_list[0].strip()
                ret = gs.look_up(city_c, 3)
                if ret is not None:
                    city_e = ret['name_e']
                    city_c = ret['name_c']
                offset = 1
            else:
                offset = 0

            entry[cm.name_c] = cm.html2plain(term_list[offset + 0]).strip()
            entry[cm.tel] = cm.html2plain(term_list[offset + 1]).strip()
            entry[cm.addr_e] = cm.reformat_addr(term_list[offset + 2]).strip()
            entry[cm.country_e] = 'CHINA'
            entry[cm.continent_e] = 'ASIA'

            p_name_c = p[cm.province_c]
            p_name_e = ''
            ret = gs.look_up(p_name_c, 2)
            if ret is not None:
                p_name_c = ret['name_c']
                p_name_e = ret['name_e']
            cm.update_entry(entry, {cm.province_e: p_name_e, cm.province_c: p_name_c,
                                    cm.city_e: city_e, cm.city_c: city_c})
            entry[cm.city_e] = cm.extract_city(entry[cm.city_e])[0]

            gs.field_sense(entry)
            ret = gs.addr_sense(entry[cm.addr_e], entry[cm.country_e])
            if ret[1] is not None and entry[cm.province_e] == '':
                entry[cm.province_e] = ret[1]
            if ret[2] is not None and entry[cm.city_e] == '':
                entry[cm.city_e] = ret[2]
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
            # 商店列表
            return [{'func': None, 'data': s} for s in fetch_stores(data)]
        else:
            return []

    # Walk from the root node, where level == 1.
    if data is None:
        data = {'url': 'http://www.hamilton.com.cn/store_locator/',
                'brand_id': 10157, 'brandname_e': u'Hamilton', 'brandname_c': u'汉密尔顿'}

    global db
    db = cm.StoresDb()
    db.connect_db(user=user, passwd=passwd)
    db.execute(u'DELETE FROM %s WHERE brand_id=%d' % ('stores', data['brand_id']))

    results = cm.walk_tree({'func': lambda data: func(data, 0), 'data': data})
    db.disconnect_db()

    return results