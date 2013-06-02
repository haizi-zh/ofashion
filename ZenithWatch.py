# coding=utf-8
import string
import urllib2
import re
import common

__author__ = 'Zephyre'


def fetch():
    url = 'http://www.zenith-watches.com/zh_zh/shoplocator.html'
    html = common.get_data(url)
    # opener = urllib2.build_opener()
    # opener.addheaders = [("User-Agent",
    #                       "Mozilla/5.0 (Windows NT 6.1) AppleWebKit/537.36 (KHTML, like Gecko)"
    #                       "Chrome/27.0.1453.94 Safari/537.36"),
    #                      ('Accept', '*/*'), ('X-Requested-With', 'XMLHttpRequest'), ('Connection', 'keep-alive')]
    # response = opener.open(url)
    # html = response.read()

    # 开始解析工作
    # 查找数据部分，位于var items和var\s\w+之间
    start = html.find('var items')
    end = html.find('var ', start + 1)
    html = html[start:end]
    stores = []

    pattern = r'\[\s*?(\'.+?)\],'
    itor = re.finditer(pattern, html, re.S)
    for m in itor:
        # 找到单个商店
        # 确保字符串以,结尾
        desc = m.group(1) + ','
        # 经纬度
        sm = re.findall(r'(-?\d+\.\d+),', desc)
        lat = string.atof(sm[0])
        lng = string.atof(sm[1])
        # 其它信息
        sm = re.findall(r'\'(.*?)\',', desc)
        store_name = common.html2plain(sm[0])
        store_type = sm[2]
        store_url = sm[4]
        stores.append({common.name_e: store_name, common.lat: lat, common.lng: lng, common.store_type: store_type,
                       common.url: store_url})
        print('Found store: %s' % store_name)

    return stores