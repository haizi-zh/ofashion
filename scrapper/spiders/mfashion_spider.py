# coding=utf-8
import copy
import re
import urlparse
from scrapy import log
import scrapy.contrib.spiders
from scrapy.http import Request
import common as cm
from utils.utils import unicodify, iterable

__author__ = 'Zephyre'


class MFashionSpider(scrapy.contrib.spiders.CrawlSpider):
    # def get_host_url(self, region):
    #     """
    #     根据region，获得对应的host地址
    #     :param region:
    #     """
    #     return self.spider_data['hosts'][region]

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

    @staticmethod
    def process_href(href, referer):
        ret = urlparse.urlparse(href)
        netloc = ret.netloc if ret.netloc else urlparse.urlparse(referer).netloc
        scheme = ret.scheme if ret.scheme else 'http'
        return urlparse.urlunparse((scheme, netloc, ret.path, ret.params, ret.query, ret.fragment))

    def reformat(self, text):
        """
        格式化字符串，将多余的空格、换行、制表符等合并
        """
        text = unicodify(text)
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
        # 去掉连续的多个空格
        text = re.sub(r'[ \t]+', ' ', text)
        return text

    def __init__(self, name, region, *a, **kw):
        # self.name = str.format('{0}-{1}', name, region) if region else name
        self.name = name
        super(MFashionSpider, self).__init__(*a, **kw)

        if not region:
            self.region_list = self.get_supported_regions()
        elif iterable(region):
            self.region_list = region
        else:
            self.region_list = [region]

    def start_requests(self):
        for region in self.region_list:
            if region in self.get_supported_regions():
                metadata = {'region': region, 'brand_id': self.spider_data['brand_id'],
                            'tags_mapping': {}, 'category': []}

                tmp = self.spider_data['home_urls'][region]
                start_urls = tmp if iterable(tmp) else [tmp]
                for url in start_urls:
                    m = copy.deepcopy(metadata)
                    yield Request(url=url, meta={'userdata': m}, callback=self.parse, errback=self.onerr)
            else:
                self.log(str.format('No data for {0}', region), log.WARNING)

    def onerr(self, reason):
        url_main = None
        if hasattr(reason.value, 'response'):
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
        else:
            self.log(str.format('Error: {0}', reason))
