# coding=utf-8
import string
import urllib
import urllib2
import re
import common

__author__ = 'Zephyre'


def get_geo_entries(content):
    html = content
    # 获得省信息
    dsy_search_start = 0
    geoentries = {}
    while True:
        start = html.find('dsy.add("0",', dsy_search_start);
        if start == -1:
            break
            # 检查是否被注释
        tmpstr = html[start - 10:start].strip()
        if tmpstr[tmpstr.__len__() - 2:tmpstr.__len__()].__eq__('//'):
            continue

        start += 'dsy.add("0",'.__len__()
        end = html.find(');', start)
        dsy_search_start = end + 2

        province_str = html[start:end]
        m = re.findall(r'"(.+?)"', province_str)
        provinces = [val for val in m]
        # 获得市信息
        idx = 0
        while idx < provinces.__len__():
            pattern = 'dsy.add("0_%d",' % idx
            start = html.find(pattern, dsy_search_start)
            # 检查是否被注释
            tmpstr = html[start - 10:start].strip()
            if tmpstr[tmpstr.__len__() - 2:tmpstr.__len__()].__eq__('//'):
                dsy_search_start += pattern.__len__()
                continue
            start += pattern.__len__()
            end = html.find(');', start)

            city_str = html[start:end]
            m = re.findall(r'"(.+?)"', city_str)
            cities = [val for val in m]
            geoentries[provinces[idx]] = cities
            idx += 1
    return geoentries


def fetch():
    """
    获得页面的行政区划信息
    """
    url = 'http://www.samsonite.com.cn/pub1/ShopSearch.aspx'
    opener = urllib2.build_opener()
    opener.addheaders = [("User-Agent",
                          "Mozilla/5.0 (Windows NT 6.1) AppleWebKit/537.36 (KHTML, like Gecko)"
                          "Chrome/27.0.1453.94 Safari/537.36"),
                         ('Accept', '*/*'), ('X-Requested-With', 'XMLHttpRequest'), ('Connection', 'keep-alive')]
    response = opener.open(url)
    html = response.read()
    # 开始解析
    geoentries = get_geo_entries(html)
    store_entries = []
    for province in geoentries.keys():
        for city in geoentries[province]:
            print ('Fetching data for %s%s' % (province, city))
            qp = urllib.quote(province)
            qc = urllib.quote(city)
            data_str = '__VIEWSTATE=%2FwEPDwUJMzk4NDU1MDI4D2QWAmYPZBYCAgIPZBYCAgMPDxYCHgdWaXNpYmxlaGRkGAEFHl9fQ29' \
                       'udHJvbHNSZXF1aXJlUG9zdEJhY2tLZXlfXxYEBSZjdGwwMCRDb250ZW50UGxhY2VIb2xkZXIxJFJhZGlvQnV0dG9uM' \
                       'QUmY3RsMDAkQ29udGVudFBsYWNlSG9sZGVyMSRSYWRpb0J1dHRvbjEFJmN0bDAwJENvbnRlbnRQbGFjZUhvbGRlcjE' \
                       'kUmFkaW9CdXR0b24yBSZjdGwwMCRDb250ZW50UGxhY2VIb2xkZXIxJFJhZGlvQnV0dG9uMiuWfFQ5iarpUvSPGdgJ8L3Pp%2B80&' \
                       '__EVENTVALIDATION=%2FwEWAwLw67FlAuXYsLcEAuXYsJMNRyzf0HrF9JjhnKGKxA9VSQz4GUM%3D&lz_sf=' \
                       + qp + '&lz_sx=' + qc + '&sumbit.x=37&sumbit.y=7'

            # POST
            req = urllib2.Request(url)
            req.add_data(data_str)
            response = urllib2.urlopen(req)
            html = response.read().decode('utf-8')
            start = html.find(u'搜索结果')
            if start == -1:
                continue
            html = html[start + 4:]
            store_names = []
            store_addrs = []

            itor = re.finditer(r'</script>\s*?(\w+)\s*?</span>', html, re.U | re.S)
            for m in itor:
                store_names.append(m.group(1).strip())

            itor = re.finditer(r'<!--<tr><td class="text">(\w+)</td></tr>-->', html, re.U | re.S)
            for m in itor:
                store_addrs.append(common.reformat_addr(m.group(1)))
            count = min(store_addrs.__len__(), store_names.__len__())

            for i in xrange(count):
                print ('%s, %s' % (store_names[i], store_addrs[i]))
                store_entries.append({common.country_c: '中国', common.province_c: province,
                                      common.city_c: city, common.name_c: store_names[i],
                                      common.addr_c: store_addrs[i]})

    return store_entries


