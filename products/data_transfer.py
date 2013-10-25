# coding=utf-8
import re

__author__ = 'Zephyre'


def category_mapping(brand_id, region, category):
    """
    从品牌自定义的category数据，转换到统一定义的单品类别。
    :param brand_id:
    :param region:
    :param category:
    """
    # category_map_data = {10266: {u'shoes': u'鞋履', u'handbags': u'手袋', }}
    # if brand_id==10266:
    #     # Louis Vuitton
    #     pass
    # pass
    return category


def price_extract(region, body):
    """
    从字符串中提取价格信息，包括货币和价格
    :param region:
    :param body:
    """
    currency_map = {u'￥': u'CNY', u'Ұ': u'CNY', u'$': u'USD', u'€': u'EUR', u'₩': u'KRW'}

    val = (body if isinstance(body, unicode) else body.decode('utf-8'))
    val = re.sub(ur'\s', u'', val).strip()
    m = re.search(ur'[￥Ұ\$€₩]', val)
    # 读取货币信息
    if m is not None:
        currency = currency_map[m.group()]
    else:
        currency = None

    # 去除货币符号
    val = re.sub(ur'[￥Ұ\$€₩]', u'', val)

    pass
