# coding=utf-8
__author__ = 'Administrator'

from core import RoseVisionDb
import global_settings as gs
from selenium import webdriver
import re
from scrapper.spiders.mfashion_spider import MFashionSpider
from scrapper.items import ProductItem
from scrapy.http import Request
from scrapy.selector import Selector


class RandomCheckSpider(MFashionSpider):
    """
    单品随机抽检名称、价格
    @param param_dict:
    """
    spider_data = {'brand_id': 10339,
                   'home_urls': {'cn': '', }}

    @classmethod
    def get_supported_regions(cls):
        return RandomCheckSpider.spider_data['home_urls'].keys()


    @classmethod
    def get_instance(cls, region=None):
        return cls(region)

    def __init__(self, region):
        super(RandomCheckSpider, self).__init__('random', region)
        self.selenium = webdriver.PhantomJS(
            executable_path=u'C:\phantomjs-1.9.7-windows\phantomjs-1.9.7-windows\phantomjs.exe')
        # self.selenium = webdriver.Firefox()

    def start_requests(self):
        sel = self.selenium

        with RoseVisionDb(getattr(gs, 'DB_SPEC')) as db:
            rs = db.query_match(['brand_id', 'model', 'name', 'url', 'description', 'price', 'offline'],
                                'products').fetch_row(maxrows=0)
            db.start_transaction()
            try:
                for brand_id, model, name, url, description, price, offline in rs:

                    # print brand_id, model, url, description, price
                    if offline == '1' or price == None:
                        self.log(offline + '   ' + str(price))
                        continue

                    brand_id = unicode(brand_id, 'utf-8')
                    model = unicode(model, 'utf-8')
                    name = unicode(name, 'utf-8')
                    url = unicode(url, 'utf-8')
                    description = unicode(description, 'utf-8')
                    price = unicode(price, 'utf-8')
                    offline = unicode(offline, 'utf-8')

                    yield Request(url=url, method='get',
                                  meta={'brand_id': brand_id, 'model': model, 'name': name, 'url': url,
                                        'description': description, 'price': price, 'offline': offline},
                                  callback=self.parse_item, dont_filter=True)
            except:
                raise

    def parse_item(self, response):
        """
        default parse method, rule is not useful now
        """

        sel = self.selenium
        sel.get(response.url)
        brand_id = response.meta['brand_id']
        model = response.meta['model']
        name = response.meta['name']
        url = response.meta['url']
        description = response.meta['description']
        price = response.meta['price']
        offline = response.meta['offline']

        content = sel.find_element_by_xpath("//*").get_attribute('outerHTML')
        if False not in map(lambda x: x in content, (word for word in description.split(','))):
            self.log('OK!!')
        else:
            self.log('ERROR!!!!!!!!!!!!!!!!!!!!!!    :' + offline)
            # print sel.find_element('FF chain pouch with colored leather trimming and short chain strap. ')


