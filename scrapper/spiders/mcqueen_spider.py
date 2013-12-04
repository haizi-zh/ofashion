import re
from scrapy.http import Request
from scrapy.selector import Selector
from scrapper.items import ProductItem
from scrapper.spiders.mfashion_spider import MFashionSpider
import common as cm
import copy

__author__ = 'Zephyre'


class McQueenSpider(MFashionSpider):
    spider_data = {
        'currency': {'au': 'USD', 'bn': 'USD', 'ca': 'USD', 'mo': 'USD', 'my': 'USD', 'bh': 'EUR', 'bg': 'EUR',
                     'cz': 'EUR',
                     'eg': 'EUR', 'ge': 'EUR', 'hu': 'EUR', 'is': 'EUR', 'in': 'USD', 'id': 'USD', 'il': 'EUR',
                     'jo': 'EUR',
                     'kw': 'EUR', 'lv': 'EUR', 'li': 'EUR', 'lt': 'EUR', 'mk': 'EUR', 'mx': 'EUR', 'nz': 'USD',
                     'no': 'EUR',
                     'pl': 'EUR', 'qa': 'EUR', 'ru': 'EUR', 'sg': 'USD', 'kr': 'USD', 'se': 'EUR', 'ch': 'EUR',
                     'tw': 'USD'},
        'brand_id': 10008}


    @classmethod
    def get_supported_regions(cls):
        return McQueenSpider.spider_data['hosts'].keys()

    def __init__(self, region):
        region_list = {'us', 'fr', 'it', 'uk', 'au', 'at', 'bh', 'be', 'bn', 'bg', 'ca', 'cy', 'cz', 'dk', 'fi', 'ge',
                       'de', 'gr', 'hu', 'is', 'in', 'id', 'ie', 'il', 'jp', 'jo', 'kw', 'lv', 'li', 'lt', 'lu', 'mo',
                       'mk', 'my', 'mt', 'mx', 'mc', 'nl', 'eg', 'nz', 'no', 'pl', 'pt', 'qa', 'ru', 'sg', 'si', 'sk',
                       'kr', 'es', 'se', 'ch', 'tw', }
        self.spider_data['hosts'] = {k: 'http://www.alexandermcqueen.com' for k in region_list}
        self.spider_data['home_urls'] = {k: str.format('http://www.alexandermcqueen.com/{0}', k if k != 'uk' else 'gb')
                                         for k in region_list}
        super(McQueenSpider, self).__init__('mcqueen', region)

    @classmethod
    def get_instance(cls, region=None):
        return cls(region)

    def get_host_url(self, region):
        return self.spider_data['hosts'][region]

    def parse(self, response):
        sel = Selector(response)
        metadata = response.meta['userdata']

        for node1 in sel.xpath('//nav[@id="mainMenu"]/ul[contains(@class, "menuHeader") and '
                               'contains(@class, "firstLevel")]/li'):
            tag_text = None
            if 'data-main-menu' in node1._root.attrib:
                tag_text = self.reformat(cm.unicodify(node1._root.attrib['data-main-menu']))
            else:
                tmp = node1.xpath('./a[@href]')
                if tmp:
                    tag_text = self.reformat(cm.unicodify(tmp[0]._root.text))
            if not tag_text:
                continue

            m1 = copy.deepcopy(metadata)
            m1['tags_mapping']['category-0'] = [{'name': tag_text.lower(), 'title': tag_text}]
            gender = cm.guess_gender(tag_text.lower())
            if gender:
                m1['gender'] = [gender]

            for node2 in node1.xpath('./ul[contains(@class,"secondLevel")]/li/a[@href]'):
                tag_text = self.reformat(cm.unicodify(node2._root.text))
                if not tag_text:
                    continue

                m2 = copy.deepcopy(metadata)
                m2['tags_mapping']['category-1'] = [{'name': tag_text.lower(), 'title': tag_text}]
                m2['category'] = [tag_text.lower()]
                yield Request(url=self.process_href(node2._root.attrib['href'], response.url),
                              callback=self.parse_cat1, errback=self.onerr, meta={'userdata': m2})

    def parse_cat1(self, response):
        metadata = response.meta['userdata']
        sel = Selector(response)

        for node in sel.xpath('//nav[@id="categoriesMenu"]/ul[@class="level1"]//ul[@class="level2"]/li/a[@href]'):
            tag_text = self.reformat(cm.unicodify(node._root.text))
            if not tag_text:
                continue
            m = copy.deepcopy(metadata)
            m['tags_mapping']['category-2'] = [{'name': tag_text.lower(), 'title': tag_text}]
            yield Request(url=self.process_href(node._root.attrib['href'], response.url),
                          callback=self.parse_cat2, errback=self.onerr, meta={'userdata': m})

    def parse_cat2(self, response):
        metadata = response.meta['userdata']
        sel = Selector(response)

        node_list = sel.xpath('//nav[@id="categoriesMenu"]/ul[@class="level1"]//ul[@class="level2"]//'
                              'ul[@class="level2"]/li/a[@href]')
        if node_list:
            for node in node_list:
                tag_text = self.reformat(cm.unicodify(node._root.text))
                if not tag_text:
                    continue
                m = copy.deepcopy(metadata)
                m['tags_mapping']['category-3'] = [{'name': tag_text.lower(), 'title': tag_text}]
                yield Request(url=self.process_href(node._root.attrib['href'], response.url),
                              callback=self.parse_list, errback=self.onerr, meta={'userdata': m})
        else:
            for val in self.parse_list(response):
                yield val

    def parse_list(self, response):
        metadata = response.meta['userdata']
        sel = Selector(response)

        for node in sel.xpath('//ul[@id="productsContainer"]/li//div[@class="productInfo"]'):
            m = copy.deepcopy(metadata)
            tmp = node.xpath('./a[@href]/div[@class="modelName"]')
            url = None
            if tmp:
                model_name = self.reformat(cm.unicodify(tmp[0]._root.text))
                if not model_name:
                    continue
                m['name'] = model_name
                url = self.process_href(tmp[0].xpath('..')[0]._root.attrib['href'], response.url)
            if not url:
                continue

            tmp = node.xpath('./div[contains(@class,"priceContainer")]//span[@class="priceValue"]')
            if tmp:
                val = cm.unicodify(tmp[0]._root.text)
                if not val:
                    try:
                        val = cm.unicodify(tmp[0]._root.iterdescendants().next().tail)
                    except StopIteration:
                        pass
                m['price'] = val

            yield Request(url=self.process_href(url, response.url), callback=self.parse_details,
                          errback=self.onerr, meta={'userdata': m}, dont_filter=True)

    def parse_details(self, response):
        metadata = response.meta['userdata']
        sel = Selector(response)

        tmp = sel.xpath('//span[@id="modelFabricColorContainer"]')
        if not tmp:
            return
        metadata['model'] = cm.unicodify(tmp[0]._root.text)
        if not metadata['model']:
            return

        metadata['url'] = response.url

        image_urls = []
        for node in sel.xpath('//ul[@id="zoomAlternatives"]/li/img[@src]'):
            href = node._root.attrib['src']
            pattern = re.compile(r'_(\d+)([a-z]?_[a-z]\.[^\./]+)$')
            mt = pattern.search(href)
            if not mt:
                continue
            start_idx = int(mt.group(1))
            for i in xrange(start_idx, 15):
                image_urls.append(pattern.sub(str.format(r'_{0}\2', i), href))

        tmp = sel.xpath('//div[@id="description_pane"]')
        if tmp:
            metadata['description'] = self.reformat(cm.unicodify(tmp[0]._root.text))

        tmp = sel.xpath('//div[@id="colorsContainer"]/ul[@id="colors"]/li[@data-title]')
        if tmp:
            metadata['color'] = [self.reformat(cm.unicodify(val._root.attrib['data-title'])).lower() for val in tmp]

        tmp = sel.xpath('//div[@id="sizesContainer"]/ul[@id="sizes"]/li[@data-title]')
        if tmp:
            metadata['tags_mapping']['size'] = [{'name': k, 'title': k} for k in
                                                (self.reformat(cm.unicodify(val._root.attrib['data-title'])) for val in
                                                 tmp)]
            metadata['color'] = [self.reformat(cm.unicodify(val._root.attrib['data-title'])).lower() for val in tmp]

        item = ProductItem()
        item['image_urls'] = image_urls
        item['url'] = metadata['url']
        item['model'] = metadata['model']
        item['metadata'] = metadata
        return item
