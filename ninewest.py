# coding=utf-8
import json
import string
import re
import time
import common as cm
import geosense as gs

__author__ = 'Zephyre'

db = None


def fetch_stores(data):
    url = data['url']
    try:
        html, cookie_map = cm.get_data_cookie(url)
    except Exception:
        print 'Error occured in getting country list: %s' % url
        dump_data = {'level': 1, 'time': cm.format_time(), 'data': {'url': url}, 'brand_id': data['brand_id']}
        cm.dump(dump_data)
        return []

    print 'SLEEPING>>>>'
    time.sleep(5)

    m = re.search('http://www.ninewest.com/on/demandware.store/Sites-ninewest-Site/default/Stores-Find/C\d{10}', html)
    if m is None:
        return []
    url = m.group(0)

    cookie_map_new = {}
    for key in cookie_map:
        if 'dwpersonalization_' in key or key == 'sr_token':
            continue
        cookie_map_new[key] = cookie_map[key]
    cookie_map_new['invited_visitor_22225'] = '1'
    cookie_map = cookie_map_new


    try:
        html = cm.post_data(url, {'dwfrm_storelocator_startaddress': 'dallas',
                                         'dwfrm_storelocator_maxDistance': 30000.00,
                                         'dwfrm_storelocator_outlet': 'true',
                                         'dwfrm_storelocator_retail': 'true',
                                         'dwfrm_storelocator_optical': 'true',
                                         'dwfrm_storelocator_eyewear': 'true',
                                         'dwfrm_storelocator_apparel': 'true',
                                         'dwfrm_storelocator_attire': 'true',
                                         'dwfrm_storelocator_department': 'true',
                                         'dwfrm_storelocator_IsMensFootwear': 'true',
                                         'dwfrm_storelocator_IsRRR': 'true',
                                         'dwfrm_storelocator_IsRRNY': 'true',
                                         'dwfrm_storelocator_IsRRS': 'true',
                                         'dwfrm_storelocator_wholesale': 'true',
                                         'dwfrm_storelocator_bba': 'true',
                                         'dwfrm_storelocator_ba': 'true',
                                         'dwfrm_storelocator_search.x': 0,
                                         'dwfrm_storelocator_search.y': 0,
                                         'dwfrm_storelocator_countryCode': 'US',
                                         'dwfrm_storelocator_postalCode': '75202',
                                         'dwfrm_storelocator_distanceUnit': 'mi',
                                         'dwfrm_storelocator_long': -96.80045109999998,
                                         'dwfrm_storelocator_lat': 32.7801399}, cookie=cookie_map)
    except Exception:
        print 'Error occured in getting country list: %s' % url
        dump_data = {'level': 1, 'time': cm.format_time(), 'data': {'url': url}, 'brand_id': data['brand_id']}
        cm.dump(dump_data)
        return []

    store_list = []
    for m1 in re.finditer(ur'<div class="storeColumnOne">', html):
        sub, start, end = cm.extract_closure(html[m1.start():], ur'<div\b', ur'</div>')
        if end == 0:
            continue

        entry = cm.init_store_entry(data['brand_id'], data['brandname_e'], data['brandname_c'])
        m2 = re.search(ur'<div class="storename">(.+?)</div>', sub)
        if m2 is not None:
            entry[cm.name_e] = m2.group(1).strip()

        addr_list = [m2 for m2 in re.findall(ur'<div class="adddressline">(.+?)</div>', sub)]
        entry[cm.addr_e] = ', '.join(addr_list)

        m2 = re.search(ur'<div class="citystatezip">(.+?)</div>', sub)
        if m2 is not None:
            tmp = cm.reformat_addr(m2.group(1))
            terms = re.split('[, ]+', tmp)
            if len(terms) < 3:
                entry[cm.addr_e] = tmp
            else:
                ret = gs.look_up(terms[0], 3)
                if ret is not None:
                    entry[cm.city_e] = ret['name_e']
                else:
                    entry[cm.city_e] = terms[0].strip().upper()

                ret = gs.look_up(terms[1], 2)
                if ret is not None:
                    entry[cm.province_e] = ret['name_e']
                else:
                    entry[cm.province_e] = terms[0].strip().upper()

                if re.match('\s*\d{5,}\s*', terms[2]) is not None:
                    entry[cm.zip_code] = terms[2].strip()

        m2 = re.search(ur'<div class="storephone">(.+?)</div>', sub)
        if m2 is not None:
            entry[cm.tel] = m2.group(1)

        cm.update_entry(entry, {'country_e': 'UNITED STATES', 'continent_e': 'NORTH AMERICA'})
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
            # 国家列表
            return [{'func': lambda data: func(data, 1), 'data': c} for c in fetch_stores(data)]
        # elif level == 1:
        #     # 商店信息
        #     retails = [{'func': None, 'data': s} for s in fetch_retails(data)]
        #     services = [{'func': None, 'data': s} for s in fetch_service(data)]
        #     retails.extend(services)
        #     return retails
        # elif level == 2:
        #     # 城市列表
        #     return [{'func': lambda data: func(data, 3), 'data': s} for s in fetch_cities(data)]
        # elif level == 3:
        #     # 商店的具体信息
        #     return [{'func': None, 'data': s} for s in fetch_stores(data)]
        else:
            return []

    # Walk from the root node, where level == 1.
    if data is None:
        data = {'url': 'http://www.ninewest.com/on/demandware.store/Sites-ninewest-Site/default/Stores-Find',
                'post_term': '/C40179436',
                'brand_id': 10279, 'brandname_e': u'Nine West', 'brandname_c': u'玖熙'}

    global db
    db = cm.StoresDb()
    db.connect_db(user=user, passwd=passwd)
    db.execute(u'DELETE FROM %s WHERE brand_id=%d' % ('stores', data['brand_id']))

    results = cm.walk_tree({'func': lambda data: func(data, 0), 'data': data})
    db.disconnect_db()

    return results