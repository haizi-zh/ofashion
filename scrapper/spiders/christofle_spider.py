# coding=utf-8
import copy
from urlparse import urljoin
from scrapy.contrib.linkextractors.sgml import SgmlLinkExtractor

from scrapy.http import Request
from scrapy.selector import Selector

from scrapper.items import ProductItem
from scrapper.spiders.mfashion_spider import MFashionSpider
import common as cm


__author__ = 'wuya'
#brand_id	brand_name	url
#10085	Christofle	http://www.christofle.com

#备注：
#部分商品暂停销售时没有价格

_regions = [
    'us',
    'fr',
    'de-en',  #de
    'es-en',  #es
    'it-en',  #it
    'ca-en',  #ca
    'jp-fr',  #jp
    'gb-en',  #uk
    'nl-en',  #nl
]


class ChristofleSpider(MFashionSpider):
    spider_data = {'brand_id': 10085, }
    home_urls = {
        region: ['http://www.christofle.com/%s' % region, ]
        for region in _regions
    }
    home_urls['de'] = home_urls['de-en']
    home_urls['es'] = home_urls['es-en']
    home_urls['it'] = home_urls['it-en']
    home_urls['ca'] = home_urls['ca-en']
    home_urls['jp'] = home_urls['jp-fr']
    home_urls['uk'] = home_urls['gb-en']
    home_urls['nl'] = home_urls['nl-en']
    _pops = ['de-en', 'es-en', 'it-en', 'ca-en', 'jp-fr', 'gb-en', 'nl-en', ]
    [home_urls.pop(pop) for pop in _pops]
    spider_data['home_urls'] = home_urls
    _base_url = 'http://www.christofle.com/'

    def __init__(self, region):
        super(ChristofleSpider, self).__init__('christofle', region)


    @classmethod
    def get_supported_regions(cls):
        return cls.spider_data['home_urls'].keys()


    @classmethod
    def get_instance(cls, region=None):
        return cls(region)

    def parse(self, response):
        link_extractor = SgmlLinkExtractor(restrict_xpaths=('//div[@id="mainMenu"]'))
        links = link_extractor.extract_links(response)
        metadata = response.meta['userdata']
        for link in links:
            m = copy.deepcopy(metadata)
            cat_title = link.text
            cat_name = cat_title.lower()
            m['tags_mapping']['category-0'] = [{'title': cat_title, 'name': cat_name}]
            gender = cm.guess_gender(cat_name)
            if gender:
                m['gender'] = [gender]
            url = link.url
            yield Request(url=url, callback=self.parse_cat, errback=self.onerr, meta={'userdata': m})

    def parse_cat(self, response):
        sel = Selector(response)
        metadata = response.meta['userdata']
        sels = sel.xpath('//ul[@class="productList"]//li')
        for _sel in sels:
            m = copy.deepcopy(metadata)
            url = urljoin(self._base_url, ''.join(_sel.xpath('./a/@href').extract()))
            old_price = ''.join(_sel.xpath('.//del//text()').extract())
            new_price = None
            if old_price:
                old_price_only = False
                new_price = ''.join(_sel.xpath('.//span[@class="orange"]//text()').extract())
            else:
                old_price_only = True
                old_price = ''.join(_sel.xpath('.//span[@class="price"]//text()').extract())
            #价格在此处获取，无货时价格在详情页不会出现
            if 'price' not in metadata:
                m['price'] = self.reformat(old_price)
                if not old_price_only and new_price:
                    m['price_discount'] = self.reformat(new_price)
            yield Request(url=url, callback=self.parse_details, errback=self.onerr, meta={'userdata': m})

    def parse_details(self, response):
        metadata = response.meta['userdata']
        metadata['url'] = response.url
        sel = Selector(response)

        model = self.fetch_model(response)
        if model:
            metadata['model'] = model
        else:
            return

        name = self.fetch_name(response)
        if name:
            metadata['name'] = name

        # 无details
        # details = ''.join(sel.xpath('//span[@class="itemNameTitle"]//text()').extract())
        # metadata['details'] = self.reformat(details)

        description = self.fetch_description(response)
        if description:
            metadata['description'] = description

        base_url = 'http://www.christofle.com/'
        image_urls1 = sel.xpath('//div[@id="product"]//a[@id="Zoomer"]/@href').extract()
        # 这里image_urls2都是一些很小的缩略图，所以去掉了
        # image_urls2 = sel.xpath('//div[@id="product"]//img/@src').extract()
        # image_urls = image_urls1 + image_urls2
        image_urls = image_urls1
        image_urls = [urljoin(base_url, url) for url in image_urls if not url.startswith(base_url)]

        item = ProductItem()
        item['url'] = metadata['url']
        item['model'] = metadata['model']
        item['image_urls'] = image_urls
        item['metadata'] = metadata
        yield item

    @classmethod
    def is_offline(cls, response, spider=None):
        model = cls.fetch_model(response)
        name = cls.fetch_name(response)

        if model and name:
            return False
        else:
            return True

    @classmethod
    def fetch_model(cls, response, spider=None):
        sel = Selector(response)

        model = None
        try:
            model = ''.join(sel.xpath('//input[@class="id_produit"]/@value').extract())
            model = cls.reformat(model)
        except(TypeError, IndexError):
            model = None
            pass

        return model

    @classmethod
    def fetch_name(cls, response, spider=None):
        sel = Selector(response)

        name = None
        try:
            name1 = ''.join(sel.xpath('//h1[@class="name"]//text()').extract())
            name2 = ''.join(sel.xpath('//h2[@class="range"]//text()').extract())
            name = name1 + name2
            name = cls.reformat(name)
        except(TypeError, IndexError):
            name = None
            pass

        return name

    # TODO fetch_price未完成，
    # 断货产品详细页无价格，单在列表页面有价格
    # http://www.christofle.com/fr/160-36-pieces-pour-6-personnes/23629-ens-36-pieces-origine-mat
    @classmethod
    def fetch_price(cls, response, spider=None):
        sel = Selector(response)
        ret = {}

        old_price = None
        new_price = None

        return ret

    @classmethod
    def fetch_description(cls, response, spider=None):
        sel = Selector(response)

        description = None
        try:
            description = ''.join(sel.xpath('//div[@id="tab-content_1"]//text()').extract())
            description = cls.reformat(description)
        except(TypeError, IndexError):
            description = None
            pass

        return description
