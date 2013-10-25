# coding=utf-8
import json
import re
import common as cm

__author__ = 'Zephyre'

db = None
log_name = 'us_postal_log.txt'
data_map = None


def fetch_states(data):
    url = data['url']
    param = {'get': 'state'}
    try:
        body = cm.get_data(url, param)
    except Exception, e:
        cm.dump('Error in fetching countries: %s' % url, log_name)
        return ()
    results = []
    idx = 0
    for m in re.findall(ur"'([^']+)\|", body):
        idx += 1
        if idx < 5:
            continue

        d = data.copy()
        d['state'] = m
        results.append(d)
        data_map[m.strip().upper()] = {}
    return tuple(results)


def fetch_zip_codes(data):
    state = data['state'].lower().replace(' ', '-')
    county = data['county'].lower().replace(' ', '-')
    city = data['city'].lower().replace(' ', '-')
    url = 'http://www.mapsofworld.com/usa/zipcodes/%s/%s/%s.html' % (state, county, city)
    try:
        body = cm.get_data(url)
    except Exception, e:
        cm.dump('Error in fetching zip codes: %s' % url, log_name)
        return ()

    state = data['state'].strip().upper()
    county = data['county'].strip().upper()
    city = data['city'].strip().upper()
    codes = re.findall(ur"<td class='tab_bod'>(\d{5})", body)
    data_map[state][county][city] = codes

    cm.dump('Zip codes fetched: %s, %s, %s: %s' % (state, county, city, codes), log_name)

    return ()


def fetch_counties(data):
    url = data['url']
    param = {'get': 'loc', 'state': data['state']}
    try:
        body = cm.get_data(url, param)
    except Exception, e:
        cm.dump('Error in fetching countries: %s' % url, log_name)
        return ()
    results = []
    for m in re.findall(ur"'([^']+)'", body):
        d = data.copy()
        d['county'] = m
        results.append(d)
        data_map[data['state'].strip().upper()][m.strip().upper()] = {}
    return tuple(results)


def fetch_cities(data):
    url = data['url']
    param = {'get': 'district', 'state': data['state'], 'district': data['county']}
    try:
        body = cm.get_data(url, param)
    except Exception, e:
        cm.dump('Error in fetching countries: %s' % url, log_name)
        return ()
    results = []
    for m in re.findall(ur"'([^']+)'", body):
        d = data.copy()
        d['city'] = m
        results.append(d)
        data_map[data['state'].strip().upper()][data['county'].strip().upper()][m.strip().upper()] = []
    return tuple(results)


def fetch(level=1, data=None, user='root', passwd=''):
    global data_map
    def func(data, level):
        """
        :param data:
        :param level: 0：国家；1：城市；2：商店列表
        """
        if level == 0:
            # 州列表
            return [{'func': lambda data: func(data, level + 1), 'data': s} for s in fetch_states(data)]
        if level == 1:
            # 郡列表
            return [{'func': lambda data: func(data, level + 1), 'data': s} for s in fetch_counties(data)]
        if level == 2:
            # 城市列表
            return [{'func': lambda data: func(data, level + 1), 'data': s} for s in fetch_cities(data)]
        if level == 3:
            # 邮编
            return [{'func': None, 'data': s} for s in fetch_zip_codes(data)]
        else:
            return ()

    # Walk from the root node, where level == 1.
    if data is None:
        data = {'data_url': 'http://www.mapsofworld.com/usa/zipcodes/data.php',
                'url': 'http://www.mapsofworld.com/usa/zipcodes/data.php',
                'brand_id': 0000, 'brandname_e': u'xxxxxx', 'brandname_c': u'xxxxxx'}

    with open('../data/us_postal.dat', 'r') as f:
        sub = f.readlines()
    data_map = json.loads(sub[0])

    results = cm.walk_tree({'func': lambda data: func(data, 0), 'data': data})

    with open('../data/us_postal.dat', 'w') as f:
        f.write(json.dumps(data_map).encode('utf-8'))

    cm.dump('Done!', log_name)

    return results


