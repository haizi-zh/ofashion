# coding=utf-8
import copy
import json
import re
from scrapy import log
from scrapy.http import Request
from scrapy.selector import Selector
from scrapper.items import ProductItem
from scrapper.spiders.mfashion_spider import MFashionSpider
import common as cm

__author__ = 'Zephyre'


class MonclerSpider(MFashionSpider):
    spider_data = {'hosts': {'cn': 'http://store.moncler.cn'},
                   # 'home_urls': {'cn': 'http://www.moncler.cn'},
                   'brand_id': 13084}

    @classmethod
    def get_supported_regions(cls):
        return MonclerSpider.spider_data['hosts'].keys()

    def __init__(self, region):
        super(MonclerSpider, self).__init__('moncler', region)

    @classmethod
    def get_instance(cls, region=None):
        return cls(region)

    def get_host_url(self, region):
        return self.spider_data['hosts'][region]

    def parse(self, response):
        metadata = response.meta['userdata']
        sel = Selector(response)

        for node in sel.xpath('//div[@id="mainmenu"]/ul[@id="menu"]/li/a[@href]'):
            m = copy.deepcopy(metadata)
            tag_type = 'category-0'
            tag_name = cm.unicodify(node._root.text)
            m['extra'][tag_type] = tag_name
            m['tags_mapping'][tag_type] = [{'name': tag_name, 'title': tag_name}]

            url = self.process_href(node._root.attrib['href'], metadata['region'])
            yield Request(url=url, meta={'userdata': m, 'filter-level': 0}, callback=self.parse_filter,
                          errback=self.onerr, dont_filter=True)

    def parse_filter(self, response):
        metadata = response.meta['userdata']
        sel = Selector(response)

        filter_idx = response.meta['filter-level']
        tmp = sel.xpath('//ul[@id="menuFilter"]/li[@id="colorLink" or @id="microLink"]')
        if filter_idx >= len(tmp):
            # filter过滤完毕，开始解析单品
            for val in self.parse_product_list(response):
                yield val
        else:
            filter_node = tmp[filter_idx]
            if filter_node._root.attrib['id'] != 'colorLink':
                tag_type = 'category-1'
            else:
                tag_type = None
            for node in filter_node.xpath('./div/ul[contains(@class,"sub")]/li[contains(@class,"sub")]/a[@href]'):
                m = copy.deepcopy(metadata)
                tag_name = cm.unicodify(node._root.text)
                if tag_type:
                    m['extra'][tag_type] = tag_name
                    m['tags_mapping'][tag_type] = [{'name': tag_name, 'title': tag_name}]
                else:
                    m['color'] = [tag_name]

                url = self.process_href(node._root.attrib['href'], metadata['region'])
                yield Request(url=url, meta={'userdata': m, 'filter-level': filter_idx + 1},
                              callback=self.parse_filter, errback=self.onerr)

    def parse_product_list(self, response):
        metadata = response.meta['userdata']
        sel = Selector(response)

        for node in sel.xpath(
                '//div[@id="elementsContainer"]/div[contains(@id,"item")]/a[@class="itemImage" and @href]'):
            url = self.process_href(re.sub(r'\s', '', cm.html2plain(node._root.attrib['href'])),
                                    metadata['region'])
            m = copy.deepcopy(metadata)
            yield Request(url=url, meta={'userdata': m}, callback=self.parse_product_details, errback=self.onerr)

    def parse_product_details(self, response):
        metadata = response.meta['userdata']
        sel = Selector(response)

        metadata['url'] = response.url
        metadata['name'] = self.reformat(' '.join(cm.unicodify(val._root.text) for val in
                                                  sel.xpath('//div[@id="titlePrice"]/div[@class="itemTitle"]')
                                                  if val._root.text))

        mt = re.search(r'var\s+jsoninit_dejavu\s*=\s*\{\s*ITEM:', response.body)
        if not mt:
            return
        tmp = json.loads(cm.extract_closure(response.body[mt.regs[0][1]:], '{', '}')[0])
        if 'cod10' not in tmp:
            return
        metadata['model'] = tmp['cod10']
        if 'price' in tmp:
            metadata['price'] = tmp['price']

        tmp = sel.xpath('//div[@id="descr_content"]')
        if tmp:
            metadata['description'] = self.reformat(cm.unicodify(tmp[0]._root.text))

        tmp = sel.xpath('//div[@id="details_content"]')
        if tmp:
            metadata['details'] = self.reformat(cm.unicodify(tmp[0]._root.text))

            # tag_text = u', '.join([cm.html2plain(cm.unicodify(val.text)) for val in temp[0]._root.iterdescendants() if
            # val.text and val.text.strip()]).lower()

        image_urls = []
        mt = re.search(r'var\s+jsoninit_item', response.body)
        if mt:
            idx = response.body[mt.regs[0][1]:].find('AVAILABLEZOOM')
            if idx != -1:
                idx += mt.regs[0][1]
                tmp = json.loads(cm.extract_closure(response.body[idx:], '{', '}')[0].replace("'", '"'))
                for c in tmp:
                    model = metadata['model']
                    if re.search(c + '$', model, flags=re.I):
                        # 找到放大的图像
                        image_urls = [str.format('http://cdn.yoox.biz/{0}/{1}_{2}.jpg', model[:2], model, val) for val
                                      in tmp[c]]

        item = ProductItem()
        item['image_urls'] = image_urls
        item['url'] = metadata['url']
        item['model'] = metadata['model']
        item['metadata'] = metadata
        return item


    def start_requests(self):
        for region in self.region_list:
            if region in self.get_supported_regions():
                metadata = {'region': region, 'brand_id': self.spider_data['brand_id'],
                            'tags_mapping': {}, 'extra': {}}

                m = copy.deepcopy(metadata)
                m['gender'] = 'male'
                yield Request(
                    url='http://store.moncler.cn/cn/%E7%94%B7%E5%A3%AB/%E6%96%B0%E5%93%81%E4%B8%8A%E7%BA%BF_gid24319',
                    meta={'userdata': metadata},
                    callback=self.parse, errback=self.onerr)

                m = copy.deepcopy(metadata)
                m['gender'] = 'female'
                yield Request(
                    url='http://store.moncler.cn/cn/%E5%A5%B3%E5%A3%AB/%E6%96%B0%E5%93%81%E4%B8%8A%E7%BA%BF_gid24318',
                    meta={'userdata': metadata},
                    callback=self.parse, errback=self.onerr)
            else:
                self.log(str.format('No data for {0}', region), log.WARNING)
