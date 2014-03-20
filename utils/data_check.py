# coding=utf-8
__author__ = 'Administrator'

from core import RoseVisionDb
import global_settings as gs
import datetime
import logging

logging.basicConfig(filename='DataCheck.log', level=logging.DEBUG)


class DataCheck(object):
    """
    图片信息检验
    @param param_dict:
    """

    @classmethod
    def run(cls, logger=None, **kwargs):
        with RoseVisionDb(getattr(gs, 'DB_SPEC')) as db:
            rs = db.query_match(['name', 'description', 'details'],
                                'products').fetch_row(maxrows=1000)
            db.start_transaction()

            try:
                for name, desc, details in rs:
                    print name, desc, details
                    # break
            except:
                pass


if __name__ == '__main__':
    t = DataCheck()
    t.run()