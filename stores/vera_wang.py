# coding=utf-8
import string
import re
from stores import geosense as gs

__author__ = 'Zephyre'

db = None
log_name = 'vera_wang_log.txt'


def fetch_countries(data):
    url = data['url']
    try:
        body = cm.get_data(url)
    except Exception, e:
        cm.dump('Error in fetching countries: %s' % url, log_name)
        return []

    m = re.search(ur'<div id="country-container" class="select-wrapper">(.+?)</div>', body, re.S)
    if m is None:
        cm.dump('Error in fetching countries: %s' % url, log_name)
        return []
    sub = m.group(1)

    m = re.search(ur'<div id="state-container" class="select-wrapper">(.+?)</div>', body, re.S)
    if m is None:
        cm.dump('Error in fetching countries: %s' % url, log_name)
        return []
    state_sub = m.group(1)

    results = []
    val_set = set([])
    for m in re.findall(ur'<option value="(\d+)"[^<>]*>\s*([^<>]+?)\s*</option>', sub):
        d = data.copy()
        id = string.atoi(m[0])
        if id in val_set:
            continue
        val_set.add(id)
        d['country_id'] = id
        d['country'] = m[1].strip().upper()
        d['state_sub'] = state_sub
        results.append(d)
    return tuple(results)


def fetch_stores(data):
    url = data['data_url']
    state_id = data['state_id']
    if state_id is None:
        state_id = 0
    param = {'request': 'setStores', 'c': data['country_id'], 's': state_id, 'l': data['city_id'], 'ca': ''}
    try:
        body = cm.post_data(url, param)
    except Exception, e:
        cm.dump('Error in fetching stores: %s' % url, log_name)
        return []

    store_list = []
    for s in re.findall(ur'mapLocations\[\d+\]\s*=\s*new\s+Array\s*\((.+?)\);', body, re.S):
        entry = cm.init_store_entry(data['brand_id'], data['brandname_e'], data['brandname_c'])
        entry[cm.country_e] = data['country']
        entry[cm.city_e] = data['city']
        state = data['state']
        if state is not None:
            entry[cm.province_e] = state

        arg_list = cm.argument_parse(s)

        entry[cm.name_e] = re.sub(ur"^\s*'(.+?)'\s*$", ur'\1', arg_list[0]).strip()
        entry[cm.lat] = string.atof(arg_list[1])
        entry[cm.lng] = string.atof(arg_list[2])

        addr_list = [tmp.strip() for tmp in
                     cm.reformat_addr(re.sub(ur"^\s*'(.+?)'\s*$", ur'\1', arg_list[6])).split(',')]
        if '.com' in addr_list[-1]:
            entry[cm.url] = addr_list[-1]
            del addr_list[-1]
        tel = cm.extract_tel(addr_list[-1])
        if tel != '':
            entry[cm.tel] = tel
            del addr_list[-1]
        entry[cm.addr_e] = ', '.join(addr_list)
        entry[cm.store_type] = arg_list[7].replace("'", '').strip()

        gs.field_sense(entry)
        ret = gs.addr_sense(entry[cm.addr_e], entry[cm.country_e])
        if ret[1] is not None and entry[cm.province_e] == '':
            entry[cm.province_e] = ret[1]
        if ret[2] is not None and entry[cm.city_e] == '':
            entry[cm.city_e] = ret[2]
        gs.field_sense(entry)
        cm.dump('(%s / %d) Found store: %s, %s (%s, %s)' % (data['brandname_e'], data['brand_id'],
                                                            entry[cm.name_e], entry[cm.addr_e], entry[cm.country_e],
                                                            entry[cm.continent_e]), log_name)
        db.insert_record(entry, 'stores')
        store_list.append(entry)

    return store_list


def fetch_states(data):
    if data['country_id'] != 1:
        data['state_id'] = None
        data['state'] = None
        return [data]

    results = []
    val_set = set([])
    for m in re.findall(ur'<option value="(\d+)"[^<>]*>\s*([^<>]+?)\s*</option>', data['state_sub']):
        d = data.copy()
        id = string.atoi(m[0])
        if id in val_set:
            continue
        val_set.add(id)
        d['state_id'] = id
        d['state'] = m[1].strip().upper()
        results.append(d)
    return tuple(results)


def fetch_cities(data):
    url = data['data_url']
    state_id = data['state_id']
    if state_id is None:
        param = {'request': 'countryChange', 'c': data['country_id']}
    else:
        param = {'request': 'stateChange', 'c': data['country_id'], 's': state_id}
    try:
        body = cm.post_data(url, param)
    except Exception, e:
        cm.dump('Error in fetching stores: %s, %s' % (url, param), log_name)
        return []

    results = []
    val_set = set([])
    for m in re.findall(ur'<option value="(\d+)"[^<>]*>\s*([^<>]+?)\s*</option>', body):
        d = data.copy()
        id = string.atoi(m[0])
        if id in val_set:
            continue
        val_set.add(id)
        d['city_id'] = id
        d['city'] = cm.html2plain(m[1]).strip().upper()
        results.append(d)
    return tuple(results)


def fetch(level=1, data=None, user='root', passwd=''):
    def func(data, level):
        """
        :param data:
        :param level: 0：国家；1：城市；2：商店列表
        """
        if level == 0:
            # 国家列表
            return [{'func': lambda data: func(data, level + 1), 'data': s} for s in fetch_countries(data)]
        if level == 1:
            # 州列表
            return [{'func': lambda data: func(data, level + 1), 'data': s} for s in fetch_states(data)]
        if level == 2:
            # 城市列表
            return [{'func': lambda data: func(data, level + 1), 'data': s} for s in fetch_cities(data)]
        if level == 3:
            # 商店
            return [{'func': None, 'data': s} for s in fetch_stores(data)]
        else:
            return []

    # Walk from the root node, where level == 1.
    if data is None:
        data = {'data_url': 'http://www.verawang.com/EN/store-locator/do_query.php',
                'url': 'http://www.verawang.com/EN/store-locator/',
                'brand_id': 10372, 'brandname_e': u'Vera Wang', 'brandname_c': u'王维拉'}

    global db
    db = cm.StoresDb()
    db.connect_db(user=user, passwd=passwd)
    db.execute(u'DELETE FROM %s WHERE brand_id=%d' % ('stores', data['brand_id']))

    results = cm.walk_tree({'func': lambda data: func(data, 0), 'data': data})
    db.disconnect_db()

    return results


