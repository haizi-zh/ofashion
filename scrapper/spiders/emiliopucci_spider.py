# coding=utf-8
import urlparse
import copy

from scrapy.http import Request
from scrapy.selector import Selector

from scrapper.items import ProductItem
from scrapper.spiders.mfashion_spider import MFashionSpider
import common as cm
import re


__author__ = 'Zephyre'


class EmilioPucciSpider(MFashionSpider):
    spider_data = {'brand_id': 10117,
                   'home_urls': {'cn': 'http://www.emiliopucci.com/home.asp?tskay=06B33963&memory=1',
                                 'us': 'http://www.emiliopucci.com/home.asp?tskay=8D8F600C&memory=1',
                                 'au': 'http://www.emiliopucci.com/home.asp?tskay=F3954597&memory=1',
                                 'at': 'http://www.emiliopucci.com/home.asp?tskay=2ADBB68D&memory=1',
                                 'be': 'http://www.emiliopucci.com/home.asp?tskay=E6A9F090&memory=1',
                                 'ca': 'http://www.emiliopucci.com/home.asp?tskay=7EC826A9&memory=1',
                                 'dk': 'http://www.emiliopucci.com/home.asp?tskay=96511E71&memory=1',
                                 'cz': 'http://www.emiliopucci.com/home.asp?tskay=678D1A7D&memory=1',
                                 'fi': 'http://www.emiliopucci.com/home.asp?tskay=566B3ABD&memory=1',
                                 'fr': 'http://www.emiliopucci.com/home.asp?tskay=7271828A&memory=1',
                                 'de': 'http://www.emiliopucci.com/home.asp?tskay=C712808D&memory=1',
                                 'gr': 'http://www.emiliopucci.com/home.asp?tskay=372D4D65&memory=1',
                                 'hk': 'http://www.emiliopucci.com/home.asp?tskay=D2195088&memory=1',
                                 'in': 'http://www.emiliopucci.com/home.asp?tskay=7BC52705&memory=1',
                                 'id': 'http://www.emiliopucci.com/home.asp?tskay=4341C852&memory=1',
                                 'ie': 'http://www.emiliopucci.com/home.asp?tskay=CE27E1D3&memory=1',
                                 'il': 'http://www.emiliopucci.com/home.asp?tskay=1796E42B&memory=1',
                                 'jp': 'http://www.emiliopucci.com/home.asp?tskay=C0494252&memory=1',
                                 'it': 'http://www.emiliopucci.com/home.asp?tskay=B9526778&memory=1',
                                 'my': 'http://www.emiliopucci.com/home.asp?tskay=1B33C030&memory=1',
                                 'nl': 'http://www.emiliopucci.com/home.asp?tskay=DD1E43AE&memory=1',
                                 'nz': 'http://www.emiliopucci.com/home.asp?tskay=63F3D281&memory=1',
                                 'no': 'http://www.emiliopucci.com/home.asp?tskay=5161DC60&memory=1',
                                 'ru': 'http://www.emiliopucci.com/home.asp?tskay=4A5B5D93&memory=1',
                                 'sg': 'http://www.emiliopucci.com/home.asp?tskay=4A5B5D93&memory=1',
                                 'se': 'http://www.emiliopucci.com/home.asp?tskay=00890543&memory=1',
                                 'ch': 'http://www.emiliopucci.com/home.asp?tskay=8586A6D5&memory=1',
                                 'tw': 'http://www.emiliopucci.com/home.asp?tskay=CFAF0777&memory=1',
                                 'th': 'http://www.emiliopucci.com/home.asp?tskay=0221431B&memory=1',
                                 'uk': 'http://www.emiliopucci.com/home.asp?tskay=229040E8&memory=1',
                   }}

    @classmethod
    def get_supported_regions(cls):
        return EmilioPucciSpider.spider_data['home_urls'].keys()

    def __init__(self, region):
        super(EmilioPucciSpider, self).__init__('emiliopucci', region)

    @classmethod
    def get_instance(cls, region=None):
        return cls(region)

    def parse(self, response):
        metadata = response.meta['userdata']
        sel = Selector(response)

        for node1 in sel.xpath('//div[@class="categNav"]//li[@class="firstLevelItem"]'):
            tmp = node1.xpath('./h2/text()').extract()
            if not tmp:
                continue
            m1 = copy.deepcopy(metadata)
            try:
                tag_title = self.reformat(tmp[0])
                tag_name = tag_title.lower()
                m1['tags_mapping']['category-0'] = [{'name': tag_name, 'title': tag_title}]
            except (IndexError, TypeError):
                continue

            for node2 in node1.xpath('./ul[contains(@class,"secondSel")]/li/a[@href]'):
                tmp = node2.xpath('./text()').extract()
                if not tmp:
                    continue
                m2 = copy.deepcopy(m1)
                try:
                    tag_title = self.reformat(tmp[0])
                    tag_name = tag_title.lower()
                except (IndexError, TypeError):
                    continue
                m2['tags_mapping']['category-1'] = [{'name': tag_name, 'title': tag_title}]
                yield Request(url=self.process_href(node2.xpath('./@href').extract()[0], response.url),
                              callback=self.parse_list, errback=self.onerr, meta={'userdata': m2})

    def parse_list(self, response):
        metadata = response.meta['userdata']
        sel = Selector(response)

        for page_url in sel.xpath('//div[@class="nvBut"]/div[@class="pagnum"]/a[@href]/@href').extract():
            m = copy.deepcopy(metadata)
            yield Request(url=self.process_href(page_url, response.url), callback=self.parse_list, errback=self.onerr,
                          meta={'userdata': m})

        for node in sel.xpath('//div[@id="elementsContainer"]/div[contains(@id,"item")]'):
            m = copy.deepcopy(metadata)
            tmp = node.xpath('.//a[@href and @class="linkImage"]/@href').extract()
            if not m:
                continue
            url = self.process_href(re.sub(r'\s+', '', self.reformat(tmp[0])), response.url)

            tmp = node.xpath('.//div[@class="nomeprodotto"]/text()').extract()
            if tmp:
                try:
                    tag_title = self.reformat(tmp[0])
                    tag_name = tag_title.lower()
                    m['tags_mapping']['category-2'] = [{'name': tag_name, 'title': tag_title}]
                except (IndexError, TypeError):
                    pass

            tmp = node.xpath('.//*[@class="sconto" or contains(@class,"price")]/*[@class="currency"]/text()').extract()
            currency = self.reformat(tmp[0]) + ' ' if tmp else ''
            tmp = node.xpath('.//*[@class="sconto"]/*[@class="priceValue"]/text()').extract()
            sconto = self.reformat(tmp[0]) if tmp else None
            tmp = node.xpath('.//*[@class="price newprice"]/*[@class="priceValue"]/text()').extract()
            new_price = self.reformat(tmp[0]) if tmp else None

            if sconto:
                m['price'] = currency + sconto
                if new_price:
                    m['price_discount'] = currency + new_price
            elif new_price:
                m['price'] = currency + new_price

            yield Request(url=url, callback=self.parse_details, errback=self.onerr, meta={'userdata': m})

    def parse_details(self, response):
        metadata = response.meta['userdata']
        sel = Selector(response)
        metadata['url'] = response.url

        tmp = sel.xpath('//div[@id="descr_content"]/text()').extract()
        if tmp:
            tmp = self.reformat('\r'.join(tmp))
            if tmp:
                metadata['description'] = tmp
                # 寻找model
                model_list = re.findall(r'[A-Z\d\-]{4,}', tmp)
                # if model_list:
                #     metadata['model'] = model_list[-1]
        tmp = sel.xpath('//div[@id="details_content"]/text()').extract()
        if tmp:
            tmp = self.reformat('\r'.join(tmp))
            if tmp:
                metadata['details'] = tmp
        mt = re.search(r'([^/]+)/areaid', metadata['url'])
        if not mt:
            return
        metadata['model'] = mt.group(1).strip()

        image_urls = []
        for src in sel.xpath('//div[@id="innerThumbs"]/div[@class="thumbElement"]/img[@src]/@src').extract():
            img_url = self.process_href(src, response.url)
            mt = re.search(r'_(\d+)_[a-z]\.[a-z]+$', img_url)
            if not mt:
                continue
            start = int(mt.group(1))
            for idx in xrange(start, 15):
                image_urls.append(re.sub(r'(.+)_\d+_([a-z]\.[a-z]+$)', str.format(r'\1_{0}_\2', idx), img_url))

        item = ProductItem()
        item['image_urls'] = image_urls
        item['url'] = metadata['url']
        item['model'] = metadata['model']
        item['metadata'] = metadata
        yield item