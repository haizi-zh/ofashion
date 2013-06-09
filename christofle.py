# coding=utf-8
import json
import string
import re
import common as cm
import geosense as gs

__author__ = 'Zephyre'

db = None
url = 'http://www.christofle.com/oc-en/10-store-locations'
brand_id = 10085
brandname_e = u'Christofle'
brandname_c = u'法国昆庭'


def get_countries(data):
    """
    返回国家列表
    :rtype : [{'country_code':**, 'country':**}, ...]
    :param data:
    :return:
    """
    url = data['url']
    try:
        html = cm.get_data(url)
    except Exception:
        print 'Error occured: %s' % url
        dump_data = {'level': 1, 'time': cm.format_time(), 'data': {'url': url}, 'brand_id': brand_id}
        cm.dump(dump_data)
        return []

    pat = '<option value="0">Choose a country</option>'
    splits = [m.start() for m in re.finditer(pat, html)]
    splits.append(-1)
    sub_html = []
    for i in xrange(len(splits) - 1):
        sub_html.append(html[splits[i]:splits[i + 1]])

    # 1：州信息
    # s_map = [{'state_code':m[0], 'state':m[1].strip}
    state_list = []
    for m in re.findall(ur'<option value="(.+?)"\s*?>(.+?)</option>', sub_html[0][len(pat):]):
        code = m[0].strip().upper()
        state = m[1].strip().upper()
        ret = gs.look_up(state, 2)
        if ret is not None:
            # state_list.append({'state': ret[0]['province_e'], 'state_code': ret[0]['state_code']})
            state_list.append({'state': ret['name_e'], 'state_code': ret['code']})
        else:
            # state其实是写成是代码
            for key in gs.province_map['data']:
                state = gs.province_map['data'][key]
                if state['code'] == code:
                    state = state['name_e']
                    state_list.append({'state': state, 'state_code': code})
                    break

                    # flag = True
                    # for s in gs.province_map:
                    #     if 'state_code' in gs.province_map[s] and gs.province_map[s]['state_code'] == code:
                    #         state = gs.province_map[s]['province_e']
                    #         state_list.append({'state': state, 'state_code': code})
                    #         flag = False
                    #         break
                    # if flag:
                    #     state_list.append({'state': 'HZZZZ', 'state_code': code})

    # 2：国家信息
    c_list = [{'country_code': m[0], 'country': m[1].strip(), 'url': url}
              for m in re.findall(ur'<option value="([A-Z]{2})"\s*?>(.+?)</option>', sub_html[1])]

    return c_list


def get_stores(data):
    # data[StoreLocator][pays]=BO
    url = data['url']
    try:
        html = cm.post_data(url, {'data[StoreLocator][pays]': data['country_code'],
                                  'data[StoreLocator][ville]': '',
                                  'data[StoreLocator][etat]': 0})
    except Exception, e:
        print 'Error occured: %s, %s' % (url, str(e))
        dump_data = {'level': 1, 'time': cm.format_time(), 'data': {'url': url}, 'brand_id': brand_id}
        cm.dump(dump_data)
        return []

    start = html.find('var markers_affiches')
    if start == -1:
        return []
    start = html.find('[', start)
    if start == -1:
        return []
    end = html.find(']', start)
    html = html[start:end]

    store_list = []
    while True:
        entry = cm.init_store_entry(brand_id, brandname_e, brandname_c)
        sub, start, end = cm.extract_closure(html, ur'\{', ur'\}')
        if end == 0:
            break
        js = json.loads(sub)
        start = end
        html = html[start:]

        raw = js['StoreLocator']
        entry[cm.name_e] = raw['name']
        addr1 = raw['adress1']
        addr2 = raw['adress2']
        entry[cm.addr_e] = cm.reformat_addr(', '.join([addr1, addr2]))
        entry[cm.zip_code] = raw['postcode']
        entry[cm.city_e] = raw['city']
        entry[cm.province_e] = raw['region']
        entry[cm.tel] = raw['phone']
        entry[cm.fax] = raw['fax']
        entry[cm.email] = raw['email']
        entry[cm.hours] = cm.reformat_addr(raw['opening'])
        entry[cm.lat] = string.atof(raw['latitude'])
        entry[cm.lng] = string.atof(raw['longitude'])
        entry[cm.url] = raw['link'].replace('\\', '')

        raw = js['Country']
        entry[cm.country_e] = raw['name']
        raw = js['StoreLocatorType']
        entry[cm.store_type] = raw['name']
        gs.field_sense(entry)
        # # Geo
        # country = entry[cm.country_e]
        # city = entry[cm.city_e]
        # ret = gs.look_up(city, 3)
        # if ret is not None:
        #     if cm.city_e in ret[0]:
        #         entry[cm.city_e] = ret[0][cm.city_e]
        #     if cm.city_c in ret[0]:
        #         entry[cm.city_c] = ret[0][cm.city_c]
        #     if 'province' in ret[0]:
        #         ret1 = gs.look_up(ret[0]['province'], 2)
        #         if ret1 is not None:
        #             ret1 = ret1[0]
        #             if cm.province_e in ret1:
        #                 entry[cm.province_e] = ret1[cm.province_e]
        #             if cm.province_c in ret1:
        #                 entry[cm.province_c] = ret1[cm.province_c]
        # ret = gs.look_up(country, 1)
        # if ret is not None:
        #     cm.update_entry(entry, {cm.country_e: ret[0][cm.country_e], cm.country_c: ret[0][cm.country_c]})
        #     ret1 = gs.look_up(ret[0]['continent'], 0)[0]
        #     cm.update_entry(entry, {cm.continent_e: ret1[cm.continent_e], cm.continent_c: ret1[cm.continent_c]})
        # else:
        #     print 'Error in looking up %s' % country

        print '(%s / %d) Found store: %s, %s (%s, %s)' % (
            brandname_e, brand_id, entry[cm.name_e], entry[cm.addr_e], entry[cm.country_e],
            entry[cm.continent_e])
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
            # 国家
            return [{'func': lambda data: func(data, 1), 'data': c} for c in get_countries(data)]
        elif level == 1:
            store_list = get_stores(data)
            return [{'func': None, 'data': s} for s in store_list]
        #     # 城市列表
        #     return [{'func': lambda data: func(data, 2), 'data': s} for s in get_cities(data)]
        # elif level == 2:
        #     # 商店列表
        #     return [{'func': lambda data: func(data, 3), 'data': s} for s in get_store_list(data)]
        # elif level == 3:
        #     # 商店的具体信息
        #     store = get_store_details(data)
        #     return [{'func': None, 'data': store}]
        else:
            return []

    global db
    db = cm.StoresDb()
    db.connect_db(user=user, passwd=passwd)
    db.execute(u'DELETE FROM %s WHERE brand_id=%d' % ('stores', brand_id))
    # Walk from the root node, where level == 1.
    if data is None:
        data = {'url': url}
    results = cm.walk_tree({'func': lambda data: func(data, 0), 'data': data})
    db.disconnect_db()
    return results