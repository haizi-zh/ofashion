# coding=utf-8
import re

import emiliopucci
import zegna
import debeers
import common
import zenithwatch
import donna_karan
import ysl
import samsonite
import y3
import shanghaitang

__author__ = 'Zephyre'


def calc(val):
    a = (0.1, 0.15, 0.4, 0.1, 0.15, 0.1)
    tot = map(lambda x, y: x * y, a, val)
    return sum(tot)


def test():
    common.load_rev_char()
    print common.html2plain('&lt;')


if __name__ == "__main__":
    # test()
    # db=common.StoresDb()
    # db.connect_db()
    # db.execute('INSERT INTO stores (brand_id, brandname_e) VALUES (1234, "你好Haizi")')
    # db.disconnect_db()


    # zenithwatch.fetch()
    # entries = emiliopucci.fetch()
    # entries = donna_karan.fetch('dnky')
    # entries = donna_karan.fetch('donnakaran')
    # entries = debeers.fetch()
    # entries = samsonite.fetch()
    # y3.fetch()
    # zegna.fetch()
    # ysl.fetch()
    # shanghaitang.fetch()
    # db_test()

    print 'DONE!'