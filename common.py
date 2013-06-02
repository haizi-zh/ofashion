# coding=utf-8
import StringIO
import gzip
import re
import socket
import urllib
import urllib2

__author__ = 'Zephyre'

timeout = 15

# 字段名
continent_c = 'continent_c'
continent_e = 'continent_e'
country_c = 'country_c'
province_c = 'province_c'
city_c = 'city_c'
name_c = 'name_c'
name_e = 'name_e'
addr_c = 'addr_c'
url = 'url'
lng = 'lng'
lat = 'lat'
store_type = 'storetype'


def html2plain(text):
    """
    消除诸如&amp;等符号
    ("'", '&#39;'),
    ('"', '&quot;'),
    ('>', '&gt;'),
    ('<', '&lt;'),
    ('&', '&amp;')
    """
    return text.replace('&amp;', '&').replace('&#39;', "'").replace('&quot;', '"').replace('&gt;', '>').replace('&lt;',
                                                                                                                '<')


def decode_data(data):
    """
    解码(gzip)
    """
    data = StringIO.StringIO(data)
    gzipper = gzip.GzipFile(fileobj=data)
    return gzipper.read()


def proc_response(response):
    """
    对response作处理，比如gzip，决定编码信息等
    """
    hd = response.headers.dict
    charset = 'utf-8'
    if 'content-type' in hd:
        desc = hd['content-type']
        m = re.findall('charset=(.*)', desc)
        if m.__len__() > 0:
            charset = m[0]

    data = response.read()
    if 'content-encoding' in hd:
        desc = hd['content-encoding']
        if desc.__eq__('gzip'):
            html = decode_data(data)
        else:
            html = data

    if charset.__eq__('utf-8'):
        html = html.decode('utf-8')
    elif charset in set(('gb2312', 'gb18030', 'gbk')):
        html = html.decode('gb18030')
    elif charset.__eq__('big5'):
        html = html.decode('big5')
    return html


def get_data(url, data=None, timeout=timeout):
    """
    GET指定url的
    """
    opener = urllib2.build_opener()
    opener.addheaders = [("User-Agent",
                          "Mozilla/5.0 (Windows NT 6.1) AppleWebKit/537.36 (KHTML, like Gecko)"
                          "Chrome/27.0.1453.94 Safari/537.36"), ('Accept-Encoding', 'gzip,deflate,sdch'),
                         ('Accept-Language', 'en-US,en;q=0.8,zh-CN;q=0.6,zh;q=0.4,zh-TW;q=0.2'),
                         ('Accept', '*/*'), ('X-Requested-With', 'XMLHttpRequest'), ('Connection', 'keep-alive')]
    try:
        if data is not None:
            response = opener.open(url, urllib.urlencode(data), timeout=timeout)
        else:
            response = opener.open(url, timeout=timeout)

        return proc_response(response)
    except Exception, e:
        if isinstance(e, urllib2.HTTPError):
            print 'http error: {0}'.format(e.code)
        elif isinstance(e, urllib2.URLError) and isinstance(e.reason, socket.timeout):
            print 'url error: socket timeout {0}'.format(e.__str__())
        else:
            print 'misc error: ' + e.__str__()
        raise e


def post_data(url, data):
    """
    POST指定url
    """

    headers = [("User-Agent",
                "Mozilla/5.0 (Windows NT 6.1) AppleWebKit/537.36 (KHTML, like Gecko)"
                "Chrome/27.0.1453.94 Safari/537.36"), ('Accept-Encoding', 'gzip,deflate,sdch'),
               ('Accept-Language', 'en-US,en;q=0.8,zh-CN;q=0.6,zh;q=0.4,zh-TW;q=0.2'),
               ('Accept', '*/*'), ('X-Requested-With', 'XMLHttpRequest'), ('Connection', 'keep-alive')]

    try:
        req = urllib2.Request(url)
        req.add_data(data)
        for pair in headers:
            req.add_header(pair[0], pair[1])
        response = urllib2.urlopen(req)

        return proc_response(response)
    except Exception, e:
        if isinstance(e, urllib2.HTTPError):
            print 'http error: {0}'.format(e.code)
        elif isinstance(e, urllib2.URLError) and isinstance(e.reason, socket.timeout):
            print 'url error: socket timeout {0}'.format(e.__str__())
        else:
            print 'misc error: ' + e.__str__()
        raise e