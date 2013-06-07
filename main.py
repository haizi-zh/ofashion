# coding=utf-8

import common
import dunhill
import emiliopucci
import shanghaitang
import viktor_rolf
import y3
import zegna
import zenithwatch

__author__ = 'Zephyre'


def calc(val):
    a = (0.1, 0.15, 0.4, 0.1, 0.15, 0.1)
    tot = map(lambda x, y: x * y, a, val)
    return sum(tot)


def test():
    html = '<p>you can </p><li class="store"><div class="store-inner"><div class="store-title -h3a">Las Vegas</div>' \
           '<div class="store-address">Forum Shops at Caesars3500 Las Vegas Blvd So.<br />Las Vegas<br />' \
           'NV 89109<br />702.979.3936</div></div></li><div span="a">new</div><a href="google.com">Haha</a>'
    new_html, start, end = common.extract_closure(html, ur'<div\b', ur'</div>')

    return []


if __name__ == "__main__":
    test_flag = False
    if test_flag:
        test()
    else:
        # zenithwatch.fetch()
        # viktor_rolf.fetch()
        # shanghaitang.fetch()
        # emiliopucci.fetch()
        # zegna.fetch()
        # y3.fetch()
        dunhill.fetch(passwd='07996019')
        print 'DONE!'