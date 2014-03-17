# coding=utf-8
__author__ = 'Administrator'

from core import RoseVisionDb
import global_settings as gs
import datetime
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
        with RoseVisionDb(getattr(gs, 'DB_SPEC')) as db:
            rs = db.query_match(['iso_code', 'currency'], 'region_info').fetch_row(maxrows=0)
            db.start_transaction()
            try:
                for code, currency in rs:
                    print str.format('Fetching for currency data for {0}...', currency)
                    data = cm.get_data(url=str.format('http://download.finance.yahoo.com/d/quotes.csv?s={0}CNY=X'
                                                      '&f=sl1d1t1ba&e=.json', currency))
                    rdr = csv.reader(StringIO(data['body']))
                    line_data = [val for val in rdr][0]
                    timestamp = datetime.datetime.strptime(str.format('{0} {1}', line_data[2], line_data[3]),
                                                           '%m/%d/%Y %I:%M%p')
                    db.update({'rate': line_data[1], 'update_time': timestamp.strftime('%Y-%m-%d %H:%M:%S')},
                              'region_info', str.format('iso_code="{0}"', code))
                db.commit()
            except:
                db.rollback()
                raise


if __name__ == '__main__':
    t = CurrencyUpdate()
    t.run()