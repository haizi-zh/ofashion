# coding=utf-8

from utils.db import RoseVisionDb
import global_settings as gs

__author__ = 'Ryan'

db = RoseVisionDb()
db.conn(getattr(gs, 'DATABASE')['DB_SPEC'])
rs = db.query(str.format('select currency, rate from region_info'))
exchange_dic = {
    val['currency']: val['rate']
    for val in
    rs.fetch_row(maxrows=0, how=1)
}


def check_price_history_region_diff(price_dic, max_range=3):
    """
    price_dic = {
        'CNY': 1500.0
        'USD': 500.0
    }
    """
    stand_value = None
    for key, value in price_dic.items():
        rate = float(exchange_dic[key])
        if stand_value:
            rate_value = value * rate
            if rate_value > max_range * stand_value or rate_value < max_range * stand_value:
                return False
        else:
            if val:
                stand_value = value * rate

    return True


def check_price_history_range(price_list, max_range=1.5):
    """
    price_list = [
        {
            'currency': 'CNY'
            'price': 1799.0
            'price_discount': 799.0
        },
        {
            'currency': 'CNY'
            'price': 1799.0
        }
    ]
    """
    last_flat_price = None
    last_flat_discount = None
    for price_dic in price_list:
        currency = price_dic['currency']
        price = float(price_dic['price'])
        price_discount = None
        if 'price_discount' in price_dic:
            price_discount = float(price_dic['price_discount'])
        rate = float(exchange_dic[currency])
        flat_price = rate * price
        if price_discount:
            flat_discount = rate * price_discount

        if last_flat_price:
            if last_flat_price < flat_price / max_range or last_flat_price > flat_price * max_range:
                return False
        last_flat_price = flat_price

        if price_discount:
            if last_flat_discount:
                if last_flat_discount < flat_discount / max_range or last_flat_discount > flat_discount * max_range:
                    return False
            last_flat_discount = flat_discount

    return True
