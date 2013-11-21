# coding=utf-8
import re
import datetime
from scrapy import log
from scrapy.contrib.spiders import CrawlSpider
from scrapy.http import Request
from scrapy.selector import Selector
from scrapper import utils
from scrapper.items import ProductItem
import global_settings as glob
import copy

__author__ = 'Zephyre'

brand_id = 10057


def create_spider():
    return BurberrySpider()


def supported_regions():
    return BurberrySpider.spider_data['supported_regions']


class BurberrySpider(CrawlSpider):
    name = 'burberry'
    # allowed_domains = ['burberry.com']

    handle_httpstatus_list = [403]

    spider_data = {'host': {'cn': 'http://cn.burberry.com',
                            'us': 'http://us.burberry.com',
                            'fr': 'http://fr.burberry.com',
                            'uk': 'http://uk.burberry.com',
                            'hk': 'http://hk.burberry.com',
                            'jp': 'http://jp.burberry.com',
                            'it': 'http://it.burberry.com',
                            'sg': 'http://sg.burberry.com',
                            'tw': 'http://sg.burberry.com',
                            'mo': 'http://mo.burberry.com',
                            'au': 'http://au.burberry.com',
                            'ae': 'http://ae.burberry.com',
                            'de': 'http://de.burberry.com',
                            'ca': 'http://ca.burberry.com',
                            'es': 'http://es.burberry.com',
                            'ru': 'http://ru.burberry.com',
                            'br': 'http://br.burberry.com',
                            'kr': 'http://kr.burberry.com',
                            'my': 'http://my.burberry.com', }}
    spider_data['supported_regions'] = spider_data['host'].keys()

    def __init__(self, *a, **kw):
        super(BurberrySpider, self).__init__(*a, **kw)
        self.spider_data = copy.deepcopy(BurberrySpider.spider_data)
        self.spider_data['brand_id'] = brand_id
        for k, v in glob.BRAND_NAMES[self.spider_data['brand_id']].items():
            self.spider_data[k] = v

    def start_requests(self):
        region = self.crawler.settings['REGION']
        self.log(str.format('Fetching data for {0}', region), log.INFO)
        self.name = str.format('{0}-{1}', BurberrySpider.name, region)
        if region in self.spider_data['host']:
            return [Request(url=self.spider_data['host'][region], callback=self.parse, errback=self.onerr)]
        else:
            self.log(str.format('No data for {0}', region), log.WARNING)
            return []

    def onerr(self, reason):
        url_main = None
        response = reason.value.response if hasattr(reason.value, 'response') else None
        if not response:
            self.log(unicode.format(u'ERROR ON PROCESSING {0}', reason.request.url).encode('utf-8'), log.ERROR)
            return
        url = response.url
        temp = reason.request.meta
        if 'userdata' in temp:
            metadata = temp['userdata']
            if 'url' in metadata:
                url_main = metadata['url']

        if url_main and url_main != url:
            msg = unicode.format(u'ERROR ON PROCESSING {0}, REFERER: {1}, CODE: {2}', url, url_main,
                                 response.status).encode('utf-8')
        else:
            msg = unicode.format(u'ERROR ON PROCESSING {1}, CODE: {0}', response.status, url).encode('utf-8')

        self.log(msg, log.ERROR)

    def parse(self, response):
        self.log(unicode.format(u'PARSE_HOME: URL={0}', response.url).encode('utf-8'), level=log.DEBUG)
        m = re.search(r'([a-zA-Z]{2})\.burberry\.com', response.url)
        if m:
            metadata = {'region': m.group(1), 'tags_mapping': {}, 'extra': {}}
            region = metadata['region']

            hxs = Selector(response)
            for item in hxs.xpath(
                    "//div[@id='shared_sidebar']//div[@id='nav']//ul[@class='l-1-set']//li[@class='l-1-link "
                    "l-1-link-open']//li/a[@href]"):
                href = item._root.attrib['href']
                cat = utils.unicodify(re.sub(r'/', '', href)).lower()
                title = utils.unicodify(item._root.attrib['title'])
                if title:
                    title = title.lower()
                m = copy.deepcopy(metadata)
                m['extra']['category-1'] = [cat]
                m['tags_mapping']['category-1'] = [{'name': cat, 'title': title}]
                if cat in {'women', 'femme', 'donna'}:
                    m['gender'] = [u'female']
                elif cat in {'men', 'homme', 'uomo'}:
                    m['gender'] = [u'male']
                else:
                    m['gender'] = []
                url = self.spider_data['host'][region] + href
                yield Request(url=url, meta={'userdata': m}, dont_filter=True, callback=self.parse_category_1,
                              errback=self.onerr)

    def parse_category_1(self, response):
        self.log(unicode.format(u'PARSE_CAT_1: URL={0}', response.url).encode('utf-8'), level=log.DEBUG)
        metadata = response.meta['userdata']
        region = metadata['region']
        hxs = Selector(response)
        for item in hxs.xpath(
                "//div[@id='shared_sidebar']//div[@id='nav']//ul[@class='l-1-set']//li[@class='l-1-link l-1-link-open']"
                "//li[@class='l-2-link']/a[@href]"):
            href = item._root.attrib['href']
            cat = utils.unicodify(re.sub(r'/', '', href)).lower()
            title = utils.unicodify(item._root.attrib['title'])
            if title:
                title = title.lower()
            m = copy.deepcopy(metadata)
            m['extra']['category-2'] = [cat]
            m['tags_mapping']['category-2'] = [{'name': cat, 'title': title}]
            m['category'] = [cat]
            url = self.spider_data['host'][region] + href
            yield Request(url=url, meta={'userdata': m}, dont_filter=True, callback=self.parse_category_2,
                          errback=self.onerr)

    def parse_category_2(self, response):
        self.log(unicode.format(u'PARSE_CAT_2: URL={0}', response.url).encode('utf-8'), level=log.DEBUG)
        # metadata = self.extract_metadata(response.meta)
        metadata = response.meta['userdata']
        region = metadata['region']

        hxs = Selector(response)
        temp = hxs.xpath(
            "//div[@id='shared_sidebar']//div[@id='nav']//ul[@class='l-1-set']//li[@class='l-1-link l-1-link-open']"
            "//li[@class='l-2-link']//li[@class='l-3-link']/a[@href]")
        if not temp:
            ret = self.parse_category_3(response)
            for item in ret:
                yield item
        else:
            for item in temp:
                href = item._root.attrib['href']
                cat = utils.unicodify(re.sub(r'/', '', href)).lower()
                title = utils.unicodify(item._root.attrib['title'])
                if title:
                    title = title.lower()
                m = copy.deepcopy(metadata)
                m['extra']['category-3'] = [cat]
                m['tags_mapping']['category-3'] = [{'name': cat, 'title': title}]
                url = self.spider_data['host'][region] + href
                yield Request(url=url, meta={'userdata': m}, dont_filter=True, callback=self.parse_category_3,
                              errback=self.onerr)

    def parse_category_3(self, response):
        self.log(unicode.format(u'PARSE_CAT_3: URL={0}', response.url).encode('utf-8'), level=log.DEBUG)
        metadata = response.meta['userdata']
        region = metadata['region']

        hxs = Selector(response)
        for item in hxs.xpath("//div[@id='product_split' or @id='product_list']//div[contains(@class,'products')]/"
                              "ul[contains(@class,'product-set')]/li[contains(@class, 'product')]/a[@href]"):
            href = item._root.attrib['href']
            url = self.spider_data['host'][region] + href
            if 'data-product-id' not in item._root.attrib:
                continue
            model = item._root.attrib['data-product-id']
            m = copy.deepcopy(metadata)
            m['model'] = model
            m['url'] = url
            yield Request(url=url, meta={'userdata': m}, dont_filter=True, callback=self.parse_details,
                          errback=self.onerr)

    def parse_details(self, response):
        self.log(unicode.format(u'PARSE_DETAILS: URL={0}', response.url).encode('utf-8'), level=log.DEBUG)
        # metadata = self.extract_metadata(response.meta)
        metadata = response.meta['userdata']
        item = ProductItem()

        hxs = Selector(response)
        ret = hxs.xpath("//div[@class='price']//span[@class='price-amount']")
        if len(ret) > 0:
            metadata['price'] = ret[0]._root.text
        ret = hxs.xpath("//div[contains(@class,'colors')]/ul[contains(@class,'color-set')]/"
                        "li[contains(@class,'color')]/a[@title]/@title")
        if len(ret) > 0:
            clrs = filter(lambda x: x, (utils.unicodify(val.extract()) for val in ret))
            metadata['color'] = [c.lower() for sublist in [re.split(u'[|/]', v) for v in clrs] for c in sublist]
            metadata['tags_mapping']['color'] = [{'name': c, 'title': c} for c in metadata['color']]
        ret = hxs.xpath("//div[contains(@class,'sizes')]/ul[contains(@class,'size-set')]/"
                        "li[contains(@class,'size')]/label[@class='-radio-label']")
        if len(ret) > 0:
            metadata['extra']['size'] = filter(lambda x: x, (utils.unicodify(val._root.text) for val in ret))
        ret = hxs.xpath("//li[@id='description-panel']//ul//li")
        if len(ret) > 0:
            metadata['description'] = u', '.join(filter(lambda x: x, (val._root.text for val in ret)))
        ret = hxs.xpath("//li[@id='feature-care-panel']//ul//li")
        if len(ret) > 0:
            metadata['details'] = u', '.join(filter(lambda x: x, (val._root.text for val in ret)))
        for k in {'brand_id', 'brandname_e', 'brandname_c'}:
            metadata[k] = self.spider_data[k]
        ret = hxs.xpath("//div[@class='product-title-container']/h1")
        if len(ret) > 0:
            metadata['name'] = utils.unicodify(ret[0]._root.text.strip() if ret[0]._root.text is not None else '')
        metadata['fetch_time'] = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')

        if 'name' in metadata and 'details' in metadata and 'description' in metadata:
            ret = hxs.xpath(
                "//div[@class='product_detail_container']/div[@class='product_viewer']//ul[@class='product-media-set']/"
                "li[@class='product-image']/img[@src]")
            image_urls = [val._root.attrib['src'] for val in ret]
            item['image_urls'] = image_urls
            item['url'] = metadata['url']
            item['model'] = metadata['model']
            item['metadata'] = metadata
            return item
        else:
            self.log(unicode.format(u'INVALID ITEM: {0}', metadata['url']).encode('utf-8'), log.ERROR)
            return None
