# coding=utf-8
from stores import geosense as gs

__author__ = 'Zephyre'

import re

url = 'http://www.ysl.com/en_US/stores'
db = None
brand_id = 10388
brandname_e = 'YSL (Yve Saint Laurent)'
brandname_c = u'伊夫圣罗兰'


def get_store_details(html, opt):
    # For US cities, the content might be like: Houston, Texas. Strip the state info:
    if opt[cm.country_e].strip().__eq__(u'United States'):
        city = opt[cm.city_e]
        idx = city.rfind(',')
        if idx != -1:
            opt[cm.city_e] = city[:idx].strip().upper()
            opt[cm.province_e] = city[idx + 1:].strip().upper()

    def f(m):
        store_name = m[0].strip()
        addr_str = m[1].strip()

        spl = addr_str.split('<br/>')
        store_type = cm.html2plain(spl[0].strip())

        store_addr = spl[1].strip()
        hour_idx = 2
        store_tel = ''
        for i in xrange(2, len(spl)):
            # If this is not a phone number:
            tel = cm.extract_tel(spl[i])
            if tel == '':
                store_addr += ', ' + spl[i]
                hour_idx = i + 1
            else:
                store_tel = spl[i].strip()
                hour_idx = i + 1
                break

        if hour_idx < len(spl):
            store_hour = cm.html2plain(', '.join(spl[hour_idx:])).strip()
        else:
            store_hour = ''

        # store_addr = cm.reformat_addr('\r\n'.join([val.strip() for val in spl[1:-3]]))
        store_addr = cm.reformat_addr(store_addr)

        store_entry = cm.init_store_entry(brand_id, brandname_e, brandname_c)
        cm.update_entry(store_entry,
                        {cm.continent_e: opt[cm.continent_e].strip().upper(), cm.city_e: opt[cm.city_e].strip().upper(),
                         cm.country_e: opt[cm.country_e].strip().upper(),
                         cm.name_e: cm.name_e, cm.addr_e: store_addr, cm.store_type: store_type, cm.hours: store_hour,
                         cm.tel: store_tel})
        if opt.has_key(cm.province_e):
            store_entry[cm.province_e] = opt[cm.province_e]
        else:
            store_entry[cm.province_e] = ''
        store_entry[cm.city_e] = cm.extract_city(store_entry[cm.city_e])[0]

        gs.field_sense(store_entry)
        ret = gs.addr_sense(store_entry[cm.addr_e], store_entry[cm.country_e])
        if ret[1] is not None and store_entry[cm.province_e] == '':
            store_entry[cm.province_e] = ret[1]
        if ret[2] is not None and store_entry[cm.city_e] == '':
            store_entry[cm.city_e] = ret[2]
        gs.field_sense(store_entry)

        print '%s Found store: %s, %s (%s, %s)' % (
            brandname_e, store_entry[cm.name_e], store_entry[cm.addr_e], store_entry[cm.country_e],
            store_entry[cm.continent_e])
        db.insert_record(store_entry, 'stores')

        return store_entry

    stores = [f(m) for m in re.findall('<li>\s*?<address>\s*?<h2>(.*?)</h2><br/>(.*?)</address>\s*?</li>', html, re.S)]
    return stores


def get_entries(html, pat):
    entries = [m.strip() for m in re.findall(pat, html, re.S | re.U)]

    # 分成若干洲片段
    con_split = []
    for pat in [r'<li><span>\s*?%s\s*?</span>' % con for con in entries]:
        itor = re.finditer(pat, html)
        for m in itor:
            con_split.append(m.start())
            break

    # con_split = [html.find(pat) for pat in [r'<li><span>\s*?%s\s*?</span>' % con for con in entries]]
    con_split.append(-1)
    con_map = {}  # {'America':'html sub str'}
    for i in xrange(con_split.__len__() - 1):
        con_map[entries[i]] = html[con_split[i]:con_split[i + 1]].strip()
    return con_map


def fetch(user='root', passwd=''):
    pattern = [ur'<li><span>([\w\s,]+?)</span>\s+<ul class="countries">[\w\s]',
               ur'<li><span>([\w\s,]+?)</span>\s+<ul class="cities">[\w\s]',
               ur'<li><span>([\w\s,]+?)</span>\s+<ul class="stores">']

    def func(data, level):
        """
        :param data:
        :param level:
        :return: siblings
        """
        if level == 4:
            # get store details
            stores = get_store_details(data['content'], data)
            return [{'func': None, 'data': s} for s in stores]
        else:
            if level == 1:
                content = cm.get_data(data['url'])
            else:
                content = data['content']

            entries = get_entries(content, pattern[level - 1])

            def siblings_data(ent):
                # Each time when a new level is reached, a new field is added to data, and the 'content'
                # field is updated. This is returned to build new siblings.
                local_d = dict(data)
                if level == 1:
                    local_d[cm.continent_e] = ent
                elif level == 2:
                    local_d[cm.country_e] = ent
                elif level == 3:
                    local_d[cm.city_e] = ent
                local_d['content'] = entries[ent]
                return local_d

            return [{'func': lambda data: func(data, level + 1), 'data': siblings_data(ent)} for ent in entries]

    global db
    db = cm.StoresDb()
    db.connect_db(user=user, passwd=passwd)
    db.execute(u'DELETE FROM %s WHERE brand_id=%d' % ('stores', brand_id))

    # Walk from the root node, where level == 1.
    results = cm.walk_tree({'func': lambda data: func(data, 1), 'data': {'url': url}})
    db.disconnect_db()
    return results