# coding=utf-8
import json
import string
import re
import common as cm
import geosense as gs

__author__ = 'Zephyre'

db = None


def fetch_countries(data):
    url = data['url']
    try:
        body = cm.get_data(url)
    except Exception:
        print 'Error occured: %s' % url
        dump_data = {'level': 0, 'time': cm.format_time(), 'data': {'url': url}, 'brand_id': data['brand_id']}
        cm.dump(dump_data)
        return []

    start=body.find(u'<option value="0" selected="selected">Select a country</option>')
    if start==-1:
        return []
    end = body.find(u'</select>', start)

    country_list=[]
    for m in re.findall(ur'<option value="([A-Z]{2})"[^>]*>(.+?)</option>', body[start:end]):
        d=data.copy()
        # ret=gs.look_up(m[0],1)
        d['country']  = m[1].strip()
        d['country_code']=m[0]
        country_list.append(d)
    return country_list


def fetch_cities(data):
    url = data['url']
    try:
        body = cm.post_data(url, {'searchtype':'normal','reiter_selected':'reiter1',
                                  'country_id':data['country_code'],'city_id':0})
    except Exception:
        print 'Error occured: %s' % url
        dump_data = {'level': 0, 'time': cm.format_time(), 'data': {'url': url}, 'brand_id': data['brand_id']}
        cm.dump(dump_data)
        return []

    m = re.search(ur'<option value="0"[^>]*>city</option>', body)
    if m is None:
        return []
    end = body.find(u'</select>', m.end())

    city_list=[]
    for c in re.findall(ur'<option value="(.+?)"[^>]*>.+?</option>', body[m.end():end]):
        d=data.copy()
        d['city'] = c
        city_list.append(d)
    return city_list


def fetch_stores(data):
    url = data['url']
    try:
        body = cm.post_data(url, {'searchtype':'normal','reiter_selected':'reiter1',
                                  'country_id':data['country_code'],'city_id':data['city']})
    except Exception:
        print 'Error occured: %s' % url
        dump_data = {'level': 0, 'time': cm.format_time(), 'data': {'url': url}, 'brand_id': data['brand_id']}
        cm.dump(dump_data)
        return []

    store_list=[]
    while True:
        m = re.search(ur'<h4>\s*(.+?)\s*</h4>', body)
        if m is None:
            break

        end = body.find(u'</div>', m.end())
        sub = body[m.end():end]
        body = body[end:]

        if ('Country' in m.group(1) and 'Language' in m.group(1))\
            or 'href' in m.group(1) or 'products' in m.group(1):
            continue

        entry = cm.init_store_entry(data['brand_id'], data['brandname_e'], data['brandname_c'])
        tmp=cm.reformat_addr(sub).split(',')
        addr_list=[]
        for term in tmp:
            if u'Show on map' in term:
                continue
            elif u'電話' in term or u'Phone' in term:
                entry[cm.tel]=term.replace(u'電話', '').replace(u'Phone', '').strip()
            else:
                addr_list.append(term)
        entry[cm.addr_e]=', '.join(addr_list)

        # tmp = re.compile(ur'<h4>products</h4>', re.I)
        # m = re.search(tmp, body[end:])
        # if mis
        # prodstart = body.find(, end)
        # if prodstart!=-1:
        #     prodstart += len(u'<h4>產品</h4>')
        #     prodend = body.find(u'</div>', prodstart)
        #     entry[cm.store_type] = cm.reformat_addr(body[prodstart:prodend])

        entry[cm.country_e]=data['country_code']
        entry[cm.city_e]= data['city']
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
            return [{'func': lambda data: func(data, 1), 'data': s} for s in fetch_countries(data)]
        if level == 1:
            # 城市列表
            return [{'func': lambda data: func(data, 2), 'data': s} for s in fetch_cities(data)]
        if level == 2:
            # 商店列表
            return [{'func': None, 'data': s} for s in fetch_stores(data)]
        else:
            return []

    # Walk from the root node, where level == 1.
    if data is None:
        data = {'url': 'http://www.esprit.com/storefinder',
                'brand_id': 10123, 'brandname_e': u'Esprit', 'brandname_c': u'埃斯普利特'}

    global db
    db = cm.StoresDb()
    db.connect_db(user=user, passwd=passwd)
    db.execute(u'DELETE FROM %s WHERE brand_id=%d' % ('stores', data['brand_id']))

    results = cm.walk_tree({'func': lambda data: func(data, 0), 'data': data})
    db.disconnect_db()

    return results