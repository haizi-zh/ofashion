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
        html = cm.get_data(url)
    except Exception:
        print 'Error occured: %s' % url
        dump_data = {'level': 0, 'time': cm.format_time(), 'data': {'url': url}, 'brand_id': data['brand_id']}
        cm.dump(dump_data)
        return []

    start = html.find('<select name="country" id="inp-country"')
    if start==-1:
        return []
    sub, start,end=cm.extract_closure(html[start:], ur'<select\b', ur'</select>')
    if end ==0:
        return[]
    country_list=[]
    for m in re.findall(ur'<option value="([A-Z]{2})">(.*?)</option>', sub):
        d=data.copy()
        d['country_code']=m[0]
        d[cm.country_c] = m[1].strip()
        for key in [cm.country_e, cm.continent_e, cm.continent_c]:
            d[key]=''
        ret = gs.look_up(d['country_code'], 1)
        if ret is not None:
            d[cm.country_e]=ret['name_e']
            d[cm.country_c]=ret['name_c']
            d[cm.continent_c] = ret['continent']['name_c']
            d[cm.continent_e] = ret['continent']['name_e']

        country_list.append(d)
    return country_list


def fetch_stores(data):
    url = data['url']
    try:
        body = cm.post_data(url, {'rsp':'json', 'country':data['country_code']})
    except Exception:
        print 'Error occured: %s' % url
        dump_data = {'level': 0, 'time': cm.format_time(), 'data': {'url': url}, 'brand_id': data['brand_id']}
        cm.dump(dump_data)
        return []

    raw=json.loads(body)
    store_list = []
    for s in raw['stores']:
        entry = cm.init_store_entry(data['brand_id'], data['brandname_e'], data['brandname_c'])
        entry[cm.name_e] = cm.html2plain(s['name']).strip()

        addr_list=[]
        for key in ['address1', 'address2']:
            if s[key].strip()!='':
                addr_list.append(cm.reformat_addr(s[key]))
        entry[cm.addr_e]=' '.join(addr_list)

        # r=s['region'].strip().upper()
        # m = re.search(ur'\b([A-Z]{2})\b', r)
        # if data[cm.country_e]=='UNITED STATES' and m is not None:
        #     # 美国
        #     ret = gs.look_up(m.group(1), 2)
        #     if ret is not None:
        #         r = ret['name_e']
        # entry[cm.province_e] = r

        entry[cm.city_e] =s['city'].split(',')[0].strip().upper()
        entry[cm.zip_code] = s['zip'].strip()
        entry[cm.country_e]=data[cm.country_e]
        entry[cm.lat]=string.atof(s['lat'])
        entry[cm.lng]=string.atof(s['lng'])
        entry[cm.tel]=s['phone'].strip()
        entry[cm.fax]=s['fax'].strip()
        entry[cm.email]=s['emailaddress'].strip()
        entry[cm.url]=s['website'].strip()

        days=['Mon','Tue','Wed','Thu','Fri','Sat','Sun']
        opening=[]
        if 'openingHours' in s and s['openingHours'] is not None:
            for m in re.finditer(ur'i:(\d);s:\d+:\\?"([^\\"]+?)\\?"', s['openingHours']):
                opening.append('%s: %s'%(days[string.atoi(m.group(1))], m.group(2).strip()))
            entry[cm.hours]=', '.join(opening)

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
            # 商店列表
            return [{'func': None, 'data': s} for s in fetch_stores(data)]
        # if level == 3:
        #     # 洲列表
        #     return [{'func': None, 'data': s} for s in fetch_stores(data)]
        else:
            return []

    # Walk from the root node, where level == 1.
    if data is None:
        data = {'url': 'http://www.hublot.com/zh_CN/stores',
                'brand_id': 10168, 'brandname_e': u'HUBLOT', 'brandname_c': u'宇舶'}

    global db
    db = cm.StoresDb()
    db.connect_db(user=user, passwd=passwd)
    db.execute(u'DELETE FROM %s WHERE brand_id=%d' % ('stores', data['brand_id']))

    results = cm.walk_tree({'func': lambda data: func(data, 0), 'data': data})
    db.disconnect_db()

    return results