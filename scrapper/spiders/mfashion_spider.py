# coding=utf-8
import copy
import re
import types
from scrapy import log
import scrapy.contrib.spiders
from scrapy.http import Request
import common as cm

__author__ = 'Zephyre'


class MFashionSpider(scrapy.contrib.spiders.CrawlSpider):
    def get_host_url(self, region):
        """
        根据region，获得对应的host地址
        :param region:
        """
        pass

    @classmethod
    def get_instance(cls, region=None):
        """
        获得spider实例。
        :param region: 如果region=None，则对所有支持的region进行爬取。如果region为iterable，则对指定地区批量爬取。
        """
        pass

    @classmethod
    def get_supported_regions(cls):
        """
        获得爬虫支持的区域列表
        """
        pass

    def process_href(self, href, region, host=None):
        if not href or not href.strip():
            return None
        else:
            href = href.strip()

        if re.search('^(http|https)://', href):
            return href
        elif re.search('^//', href):
            return 'http:' + href
        elif re.search('^/', href):
            if not host:
                host = self.get_host_url(region)
            return host + href

    def reformat(self, text):
        """
        格式化字符串，将多余的空格、换行、制表符等合并
        """
        if text is None:
            return None
        text = cm.html2plain(text.strip())
        # <br/>换成换行符
        text = re.sub(ur'<\s*br\s*/?>', u'\r\n', text)
        # 去掉多余的标签
        text = re.sub(ur'<[^<>]*?>', u'', text)
        # # 换行转换
        text = re.sub('[\r\n]+', '\r', text)
        # text = re.subn(ur'(?:[\r\n])+', ', ', text)[0]
        return text

    def __init__(self, name, region, *a, **kw):
        self.name = str.format('{0}-{1}', name, region) if region else name
        super(MFashionSpider, self).__init__(*a, **kw)

        if not region:
            self.region_list = self.get_supported_regions()
        elif cm.iterable(region):
            self.region_list = region
        else:
            self.region_list = [region]

    def start_requests(self):
        for region in self.region_list:
            if region in self.get_supported_regions():
                metadata = {'region': region, 'brand_id': self.spider_data['brand_id'],
                            'tags_mapping': {}, 'category': []}

                return [Request(url=self.spider_data['home_urls'][region], meta={'userdata': metadata},
                                callback=self.parse, errback=self.onerr)]
            else:
                self.log(str.format('No data for {0}', region), log.WARNING)
                return []

    def onerr(self, reason):
        url_main = None
        response = reason.value.response
        url = response.url

        temp = reason.request.meta
        if 'userdata' in temp:
            metadata = temp['userdata']
            if 'url' in metadata:
                url_main = metadata['url']
        if url_main and url_main != url:
            msg = str.format('ERROR ON PROCESSING {0}, REFERER: {1}, CODE: {2}', url, url_main, response.status)
        else:
            msg = str.format('ERROR ON PROCESSING {1}, CODE: {0}', response.status, url)

        self.log(msg, log.ERROR)