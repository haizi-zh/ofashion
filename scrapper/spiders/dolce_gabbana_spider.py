# coding=utf-8
import json
import re
import copy

from scrapy.http import Request
from scrapy.selector import Selector

from scrapper.items import ProductItem
from scrapper.spiders.mfashion_spider import MFashionSpider
import common as cm
from utils.utils import unicodify


__author__ = 'Zephyre'


class DolceSpider(MFashionSpider):
    spider_data = {'brand_id': 10109, 'gender_nav': {'male': 'home_U', 'female': 'home_D'},
                   'home_urls': {
                       region: str.format('http://store.dolcegabbana.com/{0}', region if region != 'uk' else 'gb') for
                       region in
                       {'cn', 'us', 'fr', 'it', 'uk', 'au', 'at', 'bh', 'be', 'bg', 'cz', 'dk', 'fi', 'fr', 'de', 'gr',
                        'hu', 'ie', 'lv', 'lt', 'lu', 'mx', 'nl', 'no', 'pl', 'pt', 'ro', 'ru', 'sa', 'sk', 'si', 'es',
                        'se', 'ch', 'tr', 'ae', 'jp'}}}

    @classmethod
    def get_supported_regions(cls):
        return DolceSpider.spider_data['home_urls'].keys()

    def __init__(self, region):
        super(DolceSpider, self).__init__('dolce', region)

    @classmethod
    def get_instance(cls, region=None):
        return cls(region)

    def parse(self, response):
        metadata = response.meta['userdata']
        sel = Selector(response)

        for node1 in sel.xpath('//div[@id="mainNav"]/div[@id and @class="genderNav"]'):
            m1 = copy.deepcopy(metadata)
            if node1._root.attrib['id'] == self.spider_data['gender_nav']['male']:
                m1['gender'] = ['male']
            elif node1._root.attrib['id'] == self.spider_data['gender_nav']['female']:
                m1['gender'] = ['female']

            for node2 in node1.xpath('.//ul/li/h3'):
                m2 = copy.deepcopy(m1)
                tag_text = self.reformat(unicodify(node2._root.text))
                m2['tags_mapping']['category-0'] = [{'name': tag_text.lower(), 'title': tag_text}]
                m2['category'] = [tag_text.lower()]

                for node3 in node2.xpath('../ul/li/a[@href]'):
                    m3 = copy.deepcopy(m2)
                    tag_text = self.reformat(unicodify(node3._root.text))
                    m3['tags_mapping']['category-1'] = [{'name': tag_text.lower(), 'title': tag_text}]

                    yield Request(url=self.process_href(node3._root.attrib['href'], response.url),
                                  callback=self.parse_list, errback=self.onerr,
                                  meta={'userdata': m3, 'filter-level': 0})

    def parse_list(self, response):
        metadata = response.meta['userdata']
        # 筛选器状态：0：全选，1：已选择分类，2：已选择颜色
        level = response.meta['filter-level']
        sel = Selector(response)

        # 处理筛选器的顺序：选择分类，然后选择颜色
        xpath_list = ({'tag_type': 'category-2',
                       'xpath': '//div[@id="microLink"]/ul[@id="filterMicro"]/li/a[@href and @id!="filter_MicroAll"]'},
                      {'tag_type': 'color',
                       'xpath': '//div[@id="colorLink"]/ul[@id="filterColor"]/li/a[@href and @id!="filter_ColorAll"]'})

        if level < len(xpath_list):
            # 如果该级别还有筛选器的话
            # 筛选项目：
            filter_nodes = sel.xpath(xpath_list[level]['xpath'])
            if filter_nodes:
                for node in filter_nodes:
                    m = copy.deepcopy(metadata)
                    tag_text = self.reformat(unicodify(node._root.text))
                    if xpath_list[level]['tag_type'] == 'category-2':
                        m['tags_mapping']['category-2'] = [{'name': tag_text.lower(), 'title': tag_text}]
                    elif xpath_list[level]['tag_type'] == 'color':
                        m['color'] = [tag_text.lower()]
                    yield Request(url=self.process_href(node._root.attrib['href'], response.url),
                                  callback=self.parse_list, errback=self.onerr,
                                  meta={'userdata': m, 'filter-level': level + 1})
            else:
                # 该筛选器不存在。直接进入下一级别
                response.meta['filter-level'] = level + 1
                for val in self.parse_list(response):
                    yield val
        else:
            #筛选级别已经到头，直接返回当前的所有单品
            for node in sel.xpath('//div[@id="sliderContent"]//div[@class="singleItem"]//a[@id and @href]'):
                m = copy.deepcopy(metadata)
                yield Request(url=self.process_href(node._root.attrib['href'], response.url), dont_filter=True,
                              callback=self.parse_details, errback=self.onerr, meta={'userdata': m})

    def parse_details(self, response):
        metadata = response.meta['userdata']
        sel = Selector(response)

        metadata['url'] = response.url
        mt = re.search(r'-([a-zA-Z0-9\-]+)_cod.+', response.url)
        if not mt:
            return
        metadata['model'] = mt.group(1).upper()

        tmp = sel.xpath('//div[@id="itemDescription"]/div[@id="descriptionContent"]/*[@id="catTitle"]')
        if tmp:
            metadata['name'] = self.reformat(unicodify(tmp[0]._root.text))

        tmp = sel.xpath('//div[@id="itemDescription"]/div[@id="descriptionContent"]//em[contains(@class,"price")]')
        if tmp:
            metadata['price'] = self.reformat(unicodify(tmp[0]._root.text))

        desc = ''
        tmp = sel.xpath('//div[@id="detailsContent"]//div[@id="alwaysVisible"]')
        if tmp:
            desc = self.reformat(unicodify(tmp[0]._root.text))
        if not desc:
            desc = ''
        tmp = sel.xpath('//div[@id="detailsContent"]//div[@id="alwaysVisible"]//ul/li')
        desc_list = [desc]
        desc_list.extend(self.reformat(unicodify(val._root.text)) for val in tmp)
        desc = '\r'.join(desc_list).strip()
        if desc:
            metadata['description'] = desc

        details_terms = [','.join(
            re.sub(r'\s+', ' ', self.reformat(unicodify(val)), flags=re.U) for val in
            (val._root.text, val._root.prefix, val._root.tail) if val) for val in
                         sel.xpath('//div[@id="detailsContent"]//div[@id="hideaway"]//*[not(@id)]')]
        details = '\r'.join(val for val in details_terms if val).strip()
        if details:
            metadata['details'] = details

        tmp = sel.xpath('//div[@id="colorsBoxContent"]//ul[@id="ColorsList"]/li[@title]')
        if tmp:
            metadata['color'] = [self.reformat(unicodify(val._root.attrib['title'])) for val in tmp]

        tmp = sel.xpath('//div[@id="sizesBoxContent"]//ul[@id="SizeWList"]/li[@title]')
        if tmp:
            metadata['tags_mapping']['size'] = [self.reformat(unicodify(val._root.attrib['title'])).lower() for val
                                                in tmp]

        tmp = sel.xpath('//img[@id="bigImage" and @src]')
        image_urls = []
        if tmp:
            url_template = tmp[0]._root.attrib['src']
            mt = re.search(r'var\s+jsinit_item\s*=', response.body)
            if mt:
                data = cm.extract_closure(response.body[mt.regs[0][0]:], '{', '}')[0]
                idx = data.find('"ALTERNATE"')
                if idx != -1:
                    try:
                        for alter in json.loads(cm.extract_closure(data[idx:], r'\[', r'\]')[0]):
                            mt = re.search(r'(\d)_([a-z])', alter)
                            if not mt:
                                continue
                            start_idx = int(mt.group(1))
                            postfix = mt.group(2)
                            image_urls.extend(
                                [re.sub(r'(.+)_\d+_[a-z](.+)', str.format(r'\1_{0}_{1}\2', val, postfix), url_template)
                                 for
                                 val in xrange(start_idx, 15)])
                    except (ValueError, TypeError):
                        pass

        item = ProductItem()
        item['image_urls'] = image_urls
        item['url'] = metadata['url']
        item['model'] = metadata['model']
        item['metadata'] = metadata
        return item





