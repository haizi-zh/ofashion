# coding=utf-8
import urlparse
import copy
import re

from scrapy.http import Request
from scrapy.selector import Selector

from scrapper.items import ProductItem
from scrapper.spiders.mfashion_spider import MFashionSpider
import common as cm


__author__ = 'Zephyre'


class ChaumetSpider(MFashionSpider):
    spider_data = {'brand_id': 10076,
                   'home_urls': {'cn': 'http://www.chaumet.cn',
                                 'us': 'http://www.chaumet.com'}}

    @classmethod
    def get_supported_regions(cls):
        return ChaumetSpider.spider_data['home_urls'].keys()

    def __init__(self, region):
        super(ChaumetSpider, self).__init__('chaumet', region)

    def parse(self, response):
        metadata = response.meta['userdata']
        sel = Selector(response)

        for node in sel.xpath('//div[@id="nav"]/ul[@class="hList"]/li[contains(@id,"section")]/a[@href]'):
            try:
                tag_text = self.reformat(node.xpath('text()').extract()[0])
                tag_name = tag_text.lower()
                m = copy.deepcopy(metadata)
                m['tags_mapping']['category-0'] = [{'name': tag_name, 'title': tag_text}]
                yield Request(url=self.process_href(node.xpath('@href').extract()[0], response.url),
                              meta={'userdata': m}, callback=self.parse_cat, errback=self.onerr)
            except (TypeError, IndexError):
                continue

    def parse_cat(self, response):
        metadata = response.meta['userdata']
        sel = Selector(response)

        # 右边是否有”显示所有“之类的按钮？
        node_list = filter(lambda node: self.reformat(node.xpath('@title').extract()[0]).lower() in \
                                        (u'显示所有', 'view all', 'show all', u'すべて表示する', 'tout afficher'),
                           sel.xpath('//div[@id="pageContent"]/div[@class="btnCtn right"]/a[@title and @href]'))
        if node_list:
            yield Request(url=self.process_href(node_list[0].xpath('@href').extract()[0], response.url),
                          meta={'userdata': metadata}, callback=self.parse_cat, errback=self.onerr)
        else:
            # 查找grid内容
            cat_nodes = sel.xpath('//div[@id="pageContent"]/div[contains(@class,"grid")]/div[contains(@class,"Cell")]'
                                  '/a[@href]')
            if cat_nodes:
                for node in cat_nodes:
                    m = copy.deepcopy(metadata)
                    url = self.process_href(node.xpath('@href').extract()[0], response.url)

                    # 该节点是单品还是分类？
                    prod_node = node.xpath('../div[contains(@class,"layerProduit")]/div[@class="inner"]')
                    if prod_node:
                        # 单品
                        prod_node = prod_node[0]
                        tmp = self.reformat(prod_node.xpath('./a[@class="title" and @title]/@title').extract()[0])
                        if tmp:
                            m['name'] = tmp


                    # 尝试查找分类信息
                    tmp = node.xpath('./img[@title]/@title').extract()
                    try:
                        tag_text = self.reformat(tmp[0])
                        tag_name = tag_text.lower()
                        if tag_text:
                            # 目前metadata中最深层次的category
                            deepest = sorted(filter(lambda val: re.search(r'^category-\d+', val),
                                                    m['tags_mapping'].keys()))[-1]
                            new_level = int(re.search(r'^category-(\d+)', deepest).group(1)) + 1
                            m['tags_mapping'][str.format('category-{0}', new_level)] = [
                                {'name': tag_name, 'title': tag_text}]
                    except (TypeError, IndexError):
                        pass

                    yield Request(url=url, meta={'userdata': m}, callback=self.parse_cat, errback=self.onerr)
            else:
                # 到达叶节点
                for val in self.parse_details(response):
                    yield val

    def parse_details(self, response):
        metadata = response.meta['userdata']
        sel = Selector(response)

        metadata['url'] = response.url

        sel.xpath('//div[@class="mod productInfosMod"]')

        try:
            tmp = self.reformat(sel.xpath('//div[@class="productHead"]/*[@itemprop="name"]/text()').extract()[0])
            if tmp and 'name' not in metadata:
                metadata['name'] = tmp
        except IndexError:
            pass

        yield None