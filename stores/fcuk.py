# coding=utf-8
import re
from stores import geosense as gs

__author__ = 'Zephyre'

db = None


def fetch_uk_ireland(data):
    url = 'http://www.frenchconnection.com/content/stores/united+kingdom.htm'
    try:
        body = cm.get_data(url)
    except Exception:
        print 'Error occured: %s' % url
        dump_data = {'level': 0, 'time': cm.format_time(), 'data': {'url': url}, 'brand_id': data['brand_id']}
        cm.dump(dump_data)
        return []

    body, start, end = cm.extract_closure(body, ur'<article\b', ur'</article>')
    start = body.find(ur'<h3>OCEAN TERMINAL</h3>')
    body1 = body[:start]
    start2 = body.find(ur'<h3>FRENCH CONNECTION OUTLET</h3>')
    body2 = body[start + len(ur'<h3>OCEAN TERMINAL</h3>'):start2]
    body3 = body[start2 + len(ur'<h3>FRENCH CONNECTION OUTLET</h3>'):]

    tmp = []
    for m in re.finditer(ur'<h3>\s*(.+?)\s*</h3>', body1):
        tmp.append({'idx1': m.start(), 'idx2': m.end(), 'name': m.group(1).strip().upper()})
    tmp.append({'idx1': -1})
    sub_list = []
    for i in xrange(len(tmp) - 1):
        sub_list.append({'content': body1[tmp[i]['idx2']:tmp[i + 1]['idx1']], 'name': tmp[i]['name']})

    for sub in sub_list:
        for m in re.findall(ur'<p>(.+?)</p>', sub['content'], re.S):
            entry = cm.init_store_entry(data['brand_id'], data['brandname_e'], data['brandname_c'])
            entry[cm.country_e] = 'UNITED KINGDOM'
            entry[cm.city_e] = sub['name']

            addr_list = cm.reformat_addr(m).split(', ')
            entry[cm.addr_e] = ', '.join(addr_list[:-1])
            entry[cm.tel] = cm.extract_tel(addr_list[-1])
            gs.field_sense(entry)
            print '(%s / %d) Found store: %s, %s (%s, %s)' % (data['brandname_e'], data['brand_id'],
                                                              entry[cm.name_e], entry[cm.addr_e], entry[cm.country_e],
                                                              entry[cm.continent_e])
            db.insert_record(entry, 'stores')

    entry = cm.init_store_entry(data['brand_id'], data['brandname_e'], data['brandname_c'])
    entry[cm.country_e] = 'UNITED KINGDOM'
    entry[cm.city_e] = u'EDINBURGH'
    entry[cm.addr_e] = u'OCEAN DRIVE, LEITH, EDINBURGH'
    entry[cm.tel] = u'0131 554 8622'

    for m in re.findall(ur'<p>(.+?)</p>', body3, re.S):
        entry = cm.init_store_entry(data['brand_id'], data['brandname_e'], data['brandname_c'])
        entry[cm.country_e] = 'UNITED KINGDOM'

        addr_list = cm.reformat_addr(m).split(', ')
        entry[cm.city_e] = addr_list[0].strip().upper()
        entry[cm.addr_e] = ', '.join(addr_list[1:-1])
        entry[cm.tel] = cm.extract_tel(addr_list[-1])
        gs.field_sense(entry)
        print '(%s / %d) Found store: %s, %s (%s, %s)' % (data['brandname_e'], data['brand_id'],
                                                          entry[cm.name_e], entry[cm.addr_e], entry[cm.country_e],
                                                          entry[cm.continent_e])
        db.insert_record(entry, 'stores')


def fetch_uk_home(data):
    url = 'http://www.frenchconnection.com/content/stores/united+kingdom.htm'
    try:
        body = cm.get_data(url)
    except Exception:
        print 'Error occured: %s' % url
        dump_data = {'level': 0, 'time': cm.format_time(), 'data': {'url': url}, 'brand_id': data['brand_id']}
        cm.dump(dump_data)
        return []


def fetch_indv(data):
    url = data['url']
    try:
        body = cm.get_data(url)
    except Exception:
        print 'Error occured: %s' % url
        dump_data = {'level': 0, 'time': cm.format_time(), 'data': {'url': url}, 'brand_id': data['brand_id']}
        cm.dump(dump_data)
        return []

    body, start, end = cm.extract_closure(body, ur'<article\b', ur'</article>')
    tmp = []
    for m in re.finditer(ur'<h2>\s*(.+?)\s*</h2>', body):
        tmp.append({'idx1': m.start(), 'idx2': m.end(), 'name': m.group(1).strip().upper()})
    tmp.append({'idx1': -1})
    sub_list = []
    for i in xrange(len(tmp) - 1):
        sub_list.append({'content': body[tmp[i]['idx2']:tmp[i + 1]['idx1']], 'name': tmp[i]['name']})

    for sub in sub_list:
        for m in re.findall(ur'<p>(.+?)</p>', sub['content'], re.S):
            entry = cm.init_store_entry(data['brand_id'], data['brandname_e'], data['brandname_c'])
            entry[cm.country_e] = data['country']
            if data['country']=='UNITED STATES':
                entry[cm.province_e] = sub['name']
            else:
                entry[cm.city_e] = sub['name']

            addr_list = cm.reformat_addr(m).split(', ')
            entry[cm.addr_e] = ', '.join(addr_list[:-1])
            entry[cm.tel] = cm.extract_tel(addr_list[-1])
            if data['country']=='UNITED STATES':
                entry[cm.city_e]=addr_list[-2][:-3].strip().upper()

            gs.field_sense(entry)
            print '(%s / %d) Found store: %s, %s (%s, %s)' % (data['brandname_e'], data['brand_id'],
                                                              entry[cm.name_e], entry[cm.addr_e], entry[cm.country_e],
                                                              entry[cm.continent_e])
            db.insert_record(entry, 'stores')


def fetch_continents(data):
    result = []
    for v in ['http://www.frenchconnection.com/content/stores/europe.htm',
              'http://www.frenchconnection.com/content/stores/middle+east.htm',
              'http://www.frenchconnection.com/content/stores/far+east.htm',
              'http://www.frenchconnection.com/content/stores/rest+of+the+world.htm']:
        d=data.copy()
        d['url']=v
        result.append(d)
    return result


def fetch_stores(data):
    url = data['url']
    try:
        body = cm.get_data(url)
    except Exception:
        print 'Error occured: %s' % url
        dump_data = {'level': 0, 'time': cm.format_time(), 'data': {'url': url}, 'brand_id': data['brand_id']}
        cm.dump(dump_data)
        return []

    body, start, end = cm.extract_closure(body, ur'<article\b', ur'</article>')
    tmp = []
    for m in re.finditer(ur'<h2>\s*(.+?)\s*</h2>', body):
        tmp.append({'idx1': m.start(), 'idx2': m.end(), 'name': m.group(1).strip().upper()})
    tmp.append({'idx1': -1})
    sub_list = []
    for i in xrange(len(tmp) - 1):
        sub_list.append({'content': body[tmp[i]['idx2']:tmp[i + 1]['idx1']], 'name': tmp[i]['name']})

    store_list=[]
    for sub in sub_list:
        for m in re.findall(ur'<p>(.+?)</p>', sub['content'], re.S):
            entry = cm.init_store_entry(data['brand_id'], data['brandname_e'], data['brandname_c'])
            entry[cm.country_e] = sub['name']

            addr_list = cm.reformat_addr(m).split(', ')
            entry[cm.addr_e] = ', '.join(addr_list[:-1])
            entry[cm.tel] = cm.extract_tel(addr_list[-1])
            country, province, city = gs.addr_sense(entry[cm.addr_e])
            if province is not None:
                entry[cm.province_e]=province
            if city is not None:
                entry[cm.city_e] = city

            gs.field_sense(entry)
            print '(%s / %d) Found store: %s, %s (%s, %s)' % (data['brandname_e'], data['brand_id'],
                                                              entry[cm.name_e], entry[cm.addr_e], entry[cm.country_e],
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
            # 洲列表
            return [{'func': lambda data: func(data, 1), 'data': s} for s in fetch_continents(data)]
        if level == 1:
            # 国家列表
            return [{'func': None, 'data': s} for s in fetch_stores(data)]
        else:
            return []

    # Walk from the root node, where level == 1.
    if data is None:
        data = {'url': None,
                'brand_id': 10134, 'brandname_e': u'FCUK', 'brandname_c': u'FCUK'}

    global db
    db = cm.StoresDb()
    db.connect_db(user=user, passwd=passwd)
    db.execute(u'DELETE FROM %s WHERE brand_id=%d' % ('stores', data['brand_id']))

    fetch_uk_ireland(data)
    for v in [{'name': 'UNITED STATES', 'url': 'united+states.htm'},
              {'name': 'CANADA', 'url': 'canada.htm'},
              {'name': 'AUSTRALIA', 'url': 'australia.htm'}]:
        d = data.copy()
        d['url'] = 'http://www.frenchconnection.com/content/stores/' + v['url']
        d['country'] = v['name']
        fetch_indv(d)

    results = cm.walk_tree({'func': lambda data: func(data, 0), 'data': data})
    db.disconnect_db()

    return results