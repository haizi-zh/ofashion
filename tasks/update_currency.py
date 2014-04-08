# coding=utf-8

"""
该模块定义了一个定时任务类CurrencyUpdate，该类可以配合cron_task.py，定期更新货币的汇率信息。
"""
from datetime import datetime

from utils.utils_core import get_logger
from utils.db import RoseVisionDb
import global_settings as gs
import common as cm
import csv
from cStringIO import StringIO


class CurrencyUpdate(object):
    @classmethod
    def run(cls, logger=None, **kwargs):
        """
        更新货币的汇率信息
        @param param_dict:
        """
        logger = logger if 'logger' in kwargs else get_logger()
        logger.info('Update currency STARTED!!!!')

        with RoseVisionDb(getattr(gs, 'DB_SPEC')) as db:
            for currency in db.query_match('currency', 'currency_info').fetch_row(maxrows=0):
                currency = currency[0]
                try:
                    logger.debug(str.format('Fetching for currency data for {0}...', currency))
                    data = cm.get_data(url=str.format('http://download.finance.yahoo.com/d/quotes.csv?s={0}CNY=X'
                                                      '&f=sl1d1t1ba&e=.json', currency))
                    rate, d, t = [val for val in csv.reader(StringIO(data['body']))][0][1:4]
                    rate = float(rate)
                    timestamp = datetime.strptime(' '.join((d, t)), '%m/%d/%Y %I:%M%p').strftime('%Y-%m-%d %H:%M:%S')
                    db.update({'rate': rate, 'update_time': timestamp}, 'currency_info',
                              str.format('currency="{0}"', currency))
                except (ValueError, IOError):
                    continue
                except:
                    raise
        logger.info('Update currency ENDED!!!!')


if __name__ == '__main__':
    CurrencyUpdate.run()