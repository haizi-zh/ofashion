# coding=utf-8

import copy
from utils import info

__author__ = 'Zephyre'


# 各种过滤器，用于对单品进行过滤，禁止不合格的单品进入后续流程。

def price_check(price_list, model, threshold=5, logger=None):
    """
    对价格进行分析，挑出可能有出错的价格。
    分析算法：如果对应三个或更多的价格，则去除差异超过threshold的数据。如果对应两个价格，且二者差异超过threshold，则这两个价格均不发布。
            如果只有一个价格，则不再过滤。
    @param threshold:
    @param logger:
    @param price_list:
    """
    price_list = copy.deepcopy(price_list)

    def price_conv(price, currency):
        return price * info.currency_info()[currency]['rate']

    def avg(data):
        return float(sum(data)) / len(data) if len(data) > 0 else float('nan')

    while True:
        if len(price_list) == 1:
            return price_list

        flag = True
        avg_price = avg([price_conv(tmp['price'], tmp['currency']) for tmp in price_list])
        for idx in xrange(len(price_list)):
            p = price_conv(price_list[idx]['price'], price_list[idx]['currency'])
            if ((p / avg_price) if p > avg_price else (avg_price / p)) > threshold:
                # 如果此时只有两个价格，则无法判断谁是正确价格，只能放弃该单品
                if len(price_list) == 2:
                    if logger:
                        logger.info(str.format('PRICE MISMATCH / 2-PRICES: model={0}, {5}: {1} {2} => {6}: {3} {4}',
                                               model, price_list[0]['price'], price_list[0]['currency'],
                                               price_list[1]['price'], price_list[1]['currency'],
                                               price_list[0]['code'], price_list[1]['code']))
                    return []

                if logger:
                    logger.info(str.format('PRICE MISMATCH / IGNORING: model={0}, {1}: {2} {3}',
                                           model, price_list[idx]['code'], price_list[idx]['price'],
                                           price_list[idx]['currency']))
                del price_list[idx]
                flag = False
                break

        if flag:
            return price_list


def release_filter(item, logger=None):
    item['price_list'] = price_check(item['price_list'], item['model'], logger=logger)
    return item