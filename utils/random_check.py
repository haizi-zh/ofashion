# coding=utf-8
__author__ = 'Administrator'

from core import RoseVisionDb
import global_settings as gs
import datetime
import os
import re
import logging
from selenium import webdriver
from utils.utils_core import unicodify

logging.basicConfig(filename='ImagesCheck.log', level=logging.DEBUG)
import random


class RandomCheck(object):
    """
    单品随机抽检名称、价格
    @param param_dict:
    """

    @classmethod
    def run(cls, logger=None, **kwargs):
        #必须指定brand-id
        if 'brand_id' in kwargs.keys():
            id = kwargs['brand_id']
        else:
            return
        #todo  服务器需安装jre+selenium+phantomjs
        sel = webdriver.PhantomJS(
            executable_path=u'C:\phantomjs-1.9.7-windows\phantomjs-1.9.7-windows\phantomjs.exe')

        with RoseVisionDb(getattr(gs, 'DATABASE')['DB_SPEC']) as db:
            rs = db.query_match(['idproducts', 'brand_id', 'model', 'name', 'url', 'description', 'price'],
                                'products', {'brand_id': id, 'offline': '0'}).fetch_row(maxrows=0)
            db.start_transaction()
            #随机抽取500个单品
            rs = random.sample(rs, 500)

            try:
                for idproducts, brand_id, model, name, url, description, price in rs:
                    name_err = desc_err = price_err = False

                    brand_id = unicodify(brand_id)
                    model = unicodify(model)
                    name = unicodify(name)
                    url = unicodify(url)
                    description = unicodify(description)
                    price = unicodify(price)

                    sel.get(url)
                    content = sel.find_element_by_xpath("//*").get_attribute('outerHTML')
                    #check name
                    if name != None and name not in content:
                        name_err = True
                    #check description
                    if description != None and False in map(lambda x: x in content,
                                                            (word for word in seperate(description))):
                        desc_err = True
                    #check price
                    if price != None and price not in content:
                        price_err = True

                    if name_err or desc_err or price_err:
                        print 'error!! pk:%s' % idproducts, \
                            ',name error' if name_err else None, \
                            ',desc error' if desc_err else None, \
                            ',price error' if price_err else None
                    else:
                        print 'pass'

            except:
                raise


def seperate(text):
    return re.split(r'[\t\r, -!\.\?、，。！？]*', text)


def is_chinese(uchar):
    """判断一个unicode是否是汉字"""
    if u'\u4e00' <= uchar <= u'\u9fa5':
        return True
    else:
        return False


def is_number(uchar):
    """判断一个unicode是否是数字"""
    if u'\u0030' <= uchar <= u'\u0039':
        return True
    else:
        return False

def is_alphabet(uchar):
    """判断一个unicode是否是英文字母"""
    if (u'\u0041' <= uchar <= u'\u005a') or ( u'\u0061' <= uchar <= u'\u007a'):
        return True
    else:
        return False


if __name__ == '__main__':
    t = RandomCheck()
    t.run( brand_id =10106)