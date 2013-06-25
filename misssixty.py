# coding=utf-8
import json
import string
import re
import common as cm
import geosense as gs

__author__ = 'Zephyre'

db = None
log_name = 'misssixty_log.txt'

global tableid, queryUrlHead, queryUrlTail


def fetch_countries(data):
    content = ur'<select name="output"><option id="0" value="0">Country</option><option id="" value="">' \
              ur'KOSOVO</option><option id="AE" value="AE">UAE</option><option id="AL" value="AL">' \
              ur'ALBANIA</option><option id="AT" value="AT">AUSTRIA</option><option id="AZ" value="AZ">' \
              ur'AZERBAIJAN</option><option id="BE" value="BE">BELGIUM</option><option id="BG" value="BG">' \
              ur'BULGARIA</option><option id="CA" value="CA">CANADA</option><option id="CH" value="CH">' \
              ur'SWITZERLAND</option><option id="CN" value="CN">CHINA</option><option id="CZ" value="CZ">' \
              ur'CZECH REPUBLIC</option><option id="DE" value="DE">GERMANY</option><option id="ES" value="ES">' \
              ur'SPAIN</option><option id="FR" value="FR">FRANCE</option><option id="GB" value="GB">' \
              ur'UK</option><option id="GR" value="GR">GREECE</option><option id="HK" value="HK">' \
              ur'HONG KONG</option><option id="IT" value="IT">ITALY</option><option id="JO" value="JO">' \
              ur'JORDAN</option><option id="KZ" value="KZ">KAZAKHSTAN</option><option id="LB" value="LB">' \
              ur'LEBANON</option><option id="LU" value="LU">LUXEMBOURG</option><option id="MA" value="MA">' \
              ur'MOROCCO</option><option id="MK" value="MK">MACEDONIA</option><option id="MT" value="MT">' \
              ur'MALTA</option><option id="NL" value="NL">NETHERLANDS</option><option id="PL" value="PL">' \
              ur'POLAND</option><option id="PT" value="PT">PORTUGAL</option><option id="RS" value="RS">' \
              ur'SERBIA</option><option id="RU" value="RU">RUSSIA</option><option id="SA" value="SA">' \
              ur'SAUDI ARABIA</option><option id="SG" value="SG">SINGAPORE</option><option id="SI" value="SI">' \
              ur'SLOVENIA</option><option id="SK" value="SK">SLOVAKIA</option><option id="SY" value="SY">' \
              ur'SYRIA</option><option id="TH" value="TH">THAILAND</option><option id="TR" value="TR">' \
              ur'TURKEY</option><option id="TW" value="TW">TAIWAN</option></select>'

    url = data['url']
    try:
        body = cm.get_data(url)
    except Exception:
        cm.dump('Error in fetching countries: %s' % url, log_name)
        return []

    global tableid, queryUrlHead, queryUrlTail

    m = re.search(ur"(?<!/\*)\s*var tableid\s*=\s*'([^']+)'", body)
    if m is not None:
        tableid = m.group(1)
    m = re.search(ur"(?<!/\*)\s*var queryUrlHead\s*=\s*'([^']+)'", body)
    if m is not None:
        queryUrlHead = m.group(1).split('?')[0]
    m = re.search(ur"(?<!/\*)\s*var queryUrlTail\s*=\s*'([^']+)'", body)
    if m is not None:
        queryUrlTail = m.group(1).replace(u'&key=', u'')

    results = []
    for m in re.findall(ur'<option id="[^"]+" value="([A-Z]{2})">([^<>]+)</option>', content):
        d = data.copy()
        d['country_code'] = m[0]
        d['country'] = m[1]
        results.append(d)
    d = data.copy()
    d['country_code'] = u''
    d['country'] = u'KOSOVO'
    results.append(d)
    return results


def fetch_cities(data):
    sql = "SELECT CityUP FROM %s WHERE Country='%s' ORDER BY CityUP ASC" % (tableid, data['country_code'])
    url = (u'%s?sql=%s&key=%s' % (data['data_url'], sql, queryUrlTail)).replace(u' ', u'%20')
    try:
        body = cm.get_data(url)
    except Exception:
        cm.dump('Error in fetching cities: %s' % url, log_name)
        return []

    results = []
    for c in set([tmp[0] for tmp in json.loads(cm.extract_closure(body, ur'\{', ur'\}')[0])['rows']]):
        d = data.copy()
        d['city'] = c
        results.append(d)
    return results


def fetch_stores(data):
    sql = "SELECT CityUP,Address,Telephone,Email,StoreName,AddressDescription,OutletStore " \
          "FROM %s WHERE CityUP='%s'" % (tableid, data['city'])
    url = (u'%s?sql=%s&key=%s' % (data['data_url'], sql, queryUrlTail)).replace(u' ', u'%20')
    try:
        body = cm.get_data(url)
    except Exception, e:
        cm.dump('Error in fetching stores: %s' % url, log_name)
        return []

    store_list = []
    for s in json.loads(cm.extract_closure(body, ur'\{', ur'\}')[0])['rows']:
        entry = cm.init_store_entry(data['brand_id'], data['brandname_e'], data['brandname_c'])
        entry[cm.country_e] = data['country_code']
        entry[cm.city_e] = data['city'].strip().upper()
        addr_list=[]
        for tmp in [s[5], s[1]]:
            if cm.html2plain(tmp).strip()!='':
                addr_list.append(cm.html2plain(tmp).strip())
        entry[cm.addr_e] = ', '.join(addr_list)
        entry[cm.tel] = s[2].strip()
        entry[cm.email] = s[3].strip()
        entry[cm.name_e] = s[4].strip()
        entry[cm.store_type] = s[6].strip()

        gs.field_sense(entry)
        ret = gs.addr_sense(entry[cm.addr_e], entry[cm.country_e])
        if ret[1] is not None and entry[cm.province_e] == '':
            entry[cm.province_e] = ret[1]
        gs.field_sense(entry)
        cm.dump('(%s / %d) Found store: %s, %s (%s, %s)' % (data['brandname_e'], data['brand_id'],
                                                            entry[cm.name_e], entry[cm.addr_e], entry[cm.country_e],
                                                            entry[cm.continent_e]), log_name)
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
            # 国家列表
            return [{'func': lambda data: func(data, level + 1), 'data': s} for s in fetch_countries(data)]
        if level == 1:
            # 城市列表
            return [{'func': lambda data: func(data, level + 1), 'data': s} for s in fetch_cities(data)]
        if level == 2:
            # 商店
            return [{'func': None, 'data': s} for s in fetch_stores(data)]
        else:
            return []

    # Walk from the root node, where level == 1.
    if data is None:
        data = {'data_url': 'https://www.googleapis.com/fusiontables/v1/query',
                'url': 'http://www.misssixty.com/ITA/en-GB/CMS/Index/stores',
                'brand_id': 10262, 'brandname_e': u'Miss Sixty', 'brandname_c': u'Miss Sixty'}

    global db
    db = cm.StoresDb()
    db.connect_db(user=user, passwd=passwd)
    db.execute(u'DELETE FROM %s WHERE brand_id=%d' % ('stores', data['brand_id']))

    results = cm.walk_tree({'func': lambda data: func(data, 0), 'data': data})
    db.disconnect_db()

    return results

