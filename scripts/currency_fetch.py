# coding=utf-8

"""
该脚本从http://www.xe.com/symbols.php获得全球货币的信息
"""
from scrapy.selector import Selector

import common as cm
import global_settings
from utils.db import RoseVisionDb

__author__ = 'Zephyre'


def main():
    data = cm.get_data('http://www.xe.com/symbols.php', proxy={'url': '127.0.0.1:8087'})
    with RoseVisionDb(getattr(global_settings, 'DB_SPEC')) as db:
        for node in Selector(text=data['body']).xpath(
                '//table[@class="cSymbl_tbl"]/tr[@class="row1" or @class="row2"]'):
            tmp = node.xpath('./td/text()').extract()
            name = tmp[0].strip()
            code = tmp[1].upper().strip()
            symbol = tmp[3].strip() if len(tmp) > 3 else ''
            db.insert({'name': name, 'currency': code, 'symbol': symbol}, 'currency_info', replace=True)


if __name__ == '__main__':
    main()
