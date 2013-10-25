# coding=utf-8
import re
from stores import geosense as gs

__author__ = 'Zephyre'

db = None
log_name = 'lanvin_log.txt'


def fetch_stores(data):
    url = data['url']
    try:
        body = cm.get_data(url)
    except Exception:
        cm.dump('Error in fetching stores: %s' % url, log_name)
        return []

    store_list = []
    for m1 in re.finditer(ur'<lignecountry\s+titre\s*=\s*"([^"]+)"', body):
        country = m1.group(1).strip().upper()
        if country == 'U.S.A.':
            country = 'US'
        sub_country = cm.extract_closure(body[m1.start():], ur'<lignecountry\b', ur'</lignecountry>')[0]
        for m2 in re.finditer(ur'<lignecity\s+titre\s*=\s*"([^"]+)"', sub_country):
            city = m2.group(1).strip().upper()
            sub_city = cm.extract_closure(sub_country[m2.start():], ur'<lignecity\b', ur'</lignecity>')[0]
            m3 = re.search(ur'<!\[CDATA\[(.+?)\]\]>', sub_city, re.S)
            if m3 is None:
                continue
            sub_city = m3.group(1)
            store_subs = re.split(ur'<\s*h2\s*>\s*LANVIN BOUTIQUE\s*<\s*/h2\s*>', sub_city)
            for s in store_subs:
                if s.strip() == '':
                    continue
                m4 = re.search(ur'<p>(.+?)</p>', s, re.S)
                if m4 is None:
                    continue
                entry = cm.init_store_entry(data['brand_id'], data['brandname_e'], data['brandname_c'])
                entry[cm.country_e] = country
                entry[cm.city_e] = city
                s = m4.group(1)
                m4 = re.search(ur'(.+?)\n\s*\n', s, re.S)
                if m4 is not None:
                    entry[cm.addr_e] = cm.reformat_addr(m4.group(1))
                m4 = re.search(ur'Phone:(.+?)\n\s*\n', s, re.S)
                if m4 is not None:
                    entry[cm.tel] = cm.reformat_addr(m4.group(1).strip())
                m4 = re.search(ur'Boutique Hours:(.+?)\n\s*\n', s, re.S)
                if m4 is not None:
                    entry[cm.hours] = cm.reformat_addr(m4.group(1).strip())
                m4 = re.search(ur'Products available:(.+?)\n\s*\n', s, re.S)
                if m4 is not None:
                    entry[cm.store_type] = m4.group(1).strip()
                m4 = re.search(ur'Email:\s*<a href="mailto:([^"]+)">', s)
                if m4 is not None:
                    entry[cm.email] = m4.group(1).strip()
                gs.field_sense(entry)
                ret = gs.addr_sense(entry[cm.addr_e], entry[cm.country_e])
                if ret[1] is not None and entry[cm.province_e] == '':
                    entry[cm.province_e] = ret[1]
                gs.field_sense(entry)
                cm.dump('(%s / %d) Found store: %s, %s (%s, %s)' % (data['brandname_e'], data['brand_id'],
                                                                    entry[cm.name_e], entry[cm.addr_e],
                                                                    entry[cm.country_e],
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
            # 商店
            return [{'func': None, 'data': s} for s in fetch_stores(data)]
        else:
            return []

    # Walk from the root node, where level == 1.
    if data is None:
        data = {'url': 'http://cdn.lanvin.com/xml/footer/en/store-locator.xml',
                'brand_id': 10212, 'brandname_e': u'Lanvin', 'brandname_c': u'浪凡'}

    global db
    db = cm.StoresDb()
    db.connect_db(user=user, passwd=passwd)
    db.execute(u'DELETE FROM %s WHERE brand_id=%d' % ('stores', data['brand_id']))

    results = cm.walk_tree({'func': lambda data: func(data, 0), 'data': data})
    db.disconnect_db()

    gs.commit_maps(1)

    return results