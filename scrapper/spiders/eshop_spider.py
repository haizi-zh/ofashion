# coding=utf-8

__author__ = 'Ryan'

from scrapper.spiders.mfashion_spider import MFashionSpider

import global_settings as glob


class EShopSpider(MFashionSpider):
    spider_data = {}

    brand_list = {}

    def __init__(self, name, region):
        super(EShopSpider, self).__init__(name, region)

        for brand_id, brand_info in info.brand_info().items():
            brand_name_c = brand_info['brandname_c']
            brand_name_e = brand_info['brandname_e']
            brand_name_s = brand_info['brandname_s']

            if brand_name_c:
                brand_name_c = brand_name_c.lower()
            if brand_name_e:
                brand_name_e = brand_name_e.lower()
            if brand_name_s:
                brand_name_s = brand_name_s.lower()

            self.brand_list[unicode(brand_name_c)] = brand_id
            self.brand_list[brand_name_e] = brand_id
            self.brand_list[brand_name_s] = brand_id

    @classmethod
    def match_known_brand(cls, brand):
        temp = cls.reformat(brand)
        temp = temp.lower()

        if temp in cls.brand_list:
            return cls.brand_list[temp]
        if unicode(temp) in cls.brand_list:
            return cls.brand_list[unicode(temp)]

        return None
