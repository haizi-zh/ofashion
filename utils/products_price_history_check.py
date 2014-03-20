# coding=utf-8

from core import RoseVisionDb
import global_settings as gs

__author__ = 'Ryan'

db = RoseVisionDb()
db.conn(getattr(gs, 'DB_SPEC'))
rs = db.query(str.format('select currency, rate from region_info'))
exchange_dic = {
    val['currency']: val['rate']
    for val in
    rs.fetch_row(maxrows=0, how=1)
}

def check_price_history_region_diff(price_dic):
    stand_value = None
    for key, value in price_dic.items():
        rate = float(exchange_dic[key])
        if stand_value:
            rate_value = float(val) * rate
            if rate_value > 3*stand_value or rate_value < 3*stand_value:
                return False
        else:
            if val:
                stand_value = float(val) * rate

    return True


