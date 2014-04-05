# coding=utf-8
import copy
import re
import urlparse
from scrapy import log
import scrapy.contrib.spiders
from scrapy.http import Request
import common as cm
from utils.utils_core import unicodify, iterable

__author__ = 'Zephyre'


class MFashionBaseSpider(scrapy.contrib.spiders.CrawlSpider):
    @staticmethod
    def reformat(text):
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


class MFashionSpider(MFashionBaseSpider):
    @classmethod
    def get_supported_regions(cls):
        """
        获得爬虫支持的区域列表
        """
        raise NotImplementedError

    @staticmethod
    def process_href(href, referer):
        # ret = urlparse.urlparse(href)
        # netloc = ret.netloc if ret.netloc else urlparse.urlparse(referer).netloc
        # scheme = ret.scheme if ret.scheme else urlparse.urlparse(referer).scheme
        # return urlparse.urlunparse((scheme, netloc, ret.path, ret.params, ret.query, ret.fragment))
        return urlparse.urljoin(referer, href)

    def __init__(self, name, region, *a, **kw):
        self.name = str.format('{0}-{1}', name, '-'.join(region) if region else 'all')
        super(MFashionSpider, self).__init__(*a, **kw)

        if not region:
            self.region_list = self.get_supported_regions()
        else:
            self.region_list = list((set(region) if iterable(region) else
                                     {region}).intersection(set(self.get_supported_regions())))

    def start_requests(self):
        for region in self.region_list:
            metadata = {'region': region, 'brand_id': getattr(self, 'spider_data')['brand_id'],
                        'tags_mapping': {}, 'category': []}

            tmp = getattr(self, 'spider_data')['home_urls'][region]
            start_urls = tmp if iterable(tmp) else [tmp]
            for url in start_urls:
                m = copy.deepcopy(metadata)
                yield Request(url=url, meta={'userdata': m}, callback=self.parse, errback=self.onerr)

    def onerr(self, reason):
        url_main = None
        try:
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
        except (TypeError, AttributeError):
            self.log(str.format('Error: {0}', reason))
