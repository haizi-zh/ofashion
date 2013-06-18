# coding=utf-8
import json
import string
import re
import urllib
import common as cm
import geosense as gs

__author__ = 'Zephyre'

db = None
#
#
# def get_geo_entries(content):
#     html = content
#     # 获得省信息
#     dsy_search_start = 0
#     geoentries = {}
#     while True:
#         start = html.find('dsy.add("0",', dsy_search_start);
#         if start == -1:
#             break
#             # 检查是否被注释
#         tmpstr = html[start - 10:start].strip()
#         if tmpstr[tmpstr.__len__() - 2:tmpstr.__len__()].__eq__('//'):
#             continue
#
#         start += 'dsy.add("0",'.__len__()
#         end = html.find(');', start)
#         dsy_search_start = end + 2
#
#         province_str = html[start:end]
#         m = re.findall(r'"(.+?)"', province_str)
#         provinces = [val for val in m]
#         # 获得市信息
#         idx = 0
#         while idx < provinces.__len__():
#             pattern = 'dsy.add("0_%d",' % idx
#             start = html.find(pattern, dsy_search_start)
#             # 检查是否被注释
#             tmpstr = html[start - 10:start].strip()
#             if tmpstr[tmpstr.__len__() - 2:tmpstr.__len__()].__eq__('//'):
#                 dsy_search_start += pattern.__len__()
#                 continue
#             start += pattern.__len__()
#             end = html.find(');', start)
#
#             city_str = html[start:end]
#             m = re.findall(r'"(.+?)"', city_str)
#             cities = [val for val in m]
#             geoentries[provinces[idx]] = cities
#             idx += 1
#     return geoentries
#
#
# def fetch1():
#     """
#     获得页面的行政区划信息
#     """
#     url = 'http://www.samsonite.com.cn/pub1/ShopSearch.aspx'
#     opener = urllib2.build_opener()
#     opener.addheaders = [("User-Agent",
#                           "Mozilla/5.0 (Windows NT 6.1) AppleWebKit/537.36 (KHTML, like Gecko)"
#                           "Chrome/27.0.1453.94 Safari/537.36"),
#                          ('Accept', '*/*'), ('X-Requested-With', 'XMLHttpRequest'), ('Connection', 'keep-alive')]
#     response = opener.open(url)
#     html = response.read()
#     # 开始解析
#     geoentries = get_geo_entries(html)
#     store_entries = []
#     for province in geoentries.keys():
#         for city in geoentries[province]:
#             print ('Fetching data for %s%s' % (province, city))
#             qp = urllib.quote(province)
#             qc = urllib.quote(city)
#             data_str = '__VIEWSTATE=%2FwEPDwUJMzk4NDU1MDI4D2QWAmYPZBYCAgIPZBYCAgMPDxYCHgdWaXNpYmxlaGRkGAEFHl9fQ29' \
#                        'udHJvbHNSZXF1aXJlUG9zdEJhY2tLZXlfXxYEBSZjdGwwMCRDb250ZW50UGxhY2VIb2xkZXIxJFJhZGlvQnV0dG9uM' \
#                        'QUmY3RsMDAkQ29udGVudFBsYWNlSG9sZGVyMSRSYWRpb0J1dHRvbjEFJmN0bDAwJENvbnRlbnRQbGFjZUhvbGRlcjE' \
#                        'kUmFkaW9CdXR0b24yBSZjdGwwMCRDb250ZW50UGxhY2VIb2xkZXIxJFJhZGlvQnV0dG9uMiuWfFQ5iarpUvSPGdgJ8L3Pp%2B80&' \
#                        '__EVENTVALIDATION=%2FwEWAwLw67FlAuXYsLcEAuXYsJMNRyzf0HrF9JjhnKGKxA9VSQz4GUM%3D&lz_sf=' \
#                        + qp + '&lz_sx=' + qc + '&sumbit.x=37&sumbit.y=7'
#
#             # POST
#             req = urllib2.Request(url)
#             req.add_data(data_str)
#             response = urllib2.urlopen(req)
#             html = response.read().decode('utf-8')
#             start = html.find(u'搜索结果')
#             if start == -1:
#                 continue
#             html = html[start + 4:]
#             store_names = []
#             store_addrs = []
#
#             itor = re.finditer(r'</script>\s*?(\w+)\s*?</span>', html, re.U | re.S)
#             for m in itor:
#                 store_names.append(m.group(1).strip())
#
#             itor = re.finditer(r'<!--<tr><td class="text">(\w+)</td></tr>-->', html, re.U | re.S)
#             for m in itor:
#                 store_addrs.append(common.reformat_addr(m.group(1)))
#             count = min(store_addrs.__len__(), store_names.__len__())
#
#             for i in xrange(count):
#                 print ('%s, %s' % (store_names[i], store_addrs[i]))
#                 store_entries.append({common.country_c: '中国', common.province_c: province,
#                                       common.city_c: city, common.name_c: store_names[i],
#                                       common.addr_c: store_addrs[i]})
#
#     return store_entries


def fetch_cities(data):
    url = data['home_url']
    try:
        body = cm.get_data(url)
    except Exception:
        cm.dump('Error in fetching geo info: %s' % url, 'samsonite_log.txt')
        dump_data = {'level': 0, 'time': cm.format_time(), 'data': {'url': url}, 'brand_id': data['brand_id']}
        cm.dump(dump_data)
        return []

    pat = re.compile(ur"if \(currlan == 'ZHT'\)\s*\{.+?\}", re.S)
    body = re.sub(pat, '', body)

    m = re.search(ur'dsy.add\("0",\[(.+?)\]', body)
    if m is None:
        cm.dump('Error in fetching geo info: %s' % url, 'samsonite_log.txt')
        return []
    province_list = [m1 for m1 in re.findall(ur'"(.+?)"', m.group(1))]

    city_list = []
    for m in re.findall(ur'dsy.add\("0_(\d+)",\[(.+?)\]', body):
        for m1 in re.findall(ur'"(.+?)"', m[1]):
            c = data.copy()
            c['province'] = province_list[string.atoi(m[0])]
            c['city'] = m1
            city_list.append(c)

    return city_list


def fetch_stores(data):
    url = data['home_url']
    try:
        body = cm.post_data(url, {'lz_sf': data['province'], 'lz_sx': data['city']})
    except Exception:
        cm.dump('Error in fetching stores: %s, %s, %s' % (url, data['province'], data['city']),
                'samsonite_log.txt')
        dump_data = {'level': 0, 'time': cm.format_time(), 'data': {'url': url}, 'brand_id': data['brand_id']}
        cm.dump(dump_data)
        return []

    start = body.find(u'搜索结果')
    if start == -1:
        cm.dump('Error in fetching stores: %s, %s, %s' % (url, data['province'], data['city']),
                'samsonite_log.txt')
        return []

    body = body[start + 4:]

    store_list = []
    for m in re.findall(ur'</script>\s*(\S+)\s*</span>', body, re.S):
        entry = cm.init_store_entry(data['brand_id'], data['brandname_e'], data['brandname_c'])
        entry[cm.name_e] = m.strip()
        entry[cm.addr_e] = m.strip()
        entry[cm.city_c] = data['city']
        ret = gs.look_up(data['city'], 3)
        if ret is not None:
            entry[cm.city_e] = ret['name_e']
            if ret['province'] != '':
                entry[cm.province_e] = ret['province']['name_e']
        entry[cm.province_c] = data['province']
        ret = gs.look_up(data['province'], 2)
        if ret is not None:
            entry[cm.province_e] = ret['name_e']
        entry[cm.country_e] = u'CHINA'

        gs.field_sense(entry)
        cm.dump('(%s / %d) Found store: %s, %s (%s, %s)' % (data['brandname_e'], data['brand_id'],
                                                            entry[cm.name_e], entry[cm.addr_e], entry[cm.country_e],
                                                            entry[cm.continent_e]), 'benetton_log.txt', False)
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
            # 城市列表
            return [{'func': lambda data: func(data, 1), 'data': s} for s in fetch_cities(data)]
        if level == 1:
            # 商店列表
            return [{'func': None, 'data': s} for s in fetch_stores(data)]
        else:
            return []

    # Walk from the root node, where level == 1.
    if data is None:
        data = {'home_url': 'http://www.samsonite.com.cn/pub1/ShopSearch.aspx',
                'brand_id': 10309, 'brandname_e': u'Samsonite', 'brandname_c': u'新秀丽'}

    global db
    db = cm.StoresDb()
    db.connect_db(user=user, passwd=passwd)
    db.execute(u'DELETE FROM %s WHERE brand_id=%d' % ('stores', data['brand_id']))

    results = cm.walk_tree({'func': lambda data: func(data, 0), 'data': data})
    db.disconnect_db()

    return results