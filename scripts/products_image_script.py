# coding=utf-8

import _mysql
import os
import re
import Image
import time

__author__ = 'Zephyre'


def image_check():
    # 检查products_image
    # 统计有多少是有效的图片，包括图片损坏，格式、大小不一致等情况。
    BASE_PATH = 'd:\\Users\\Zephyre\\Development\\mstore\\storage\\products\\images'
    db_spec = {'host': '127.0.0.1', 'username': 'root', 'password': '123456', 'port': 3306, 'schema': 'spider_stores'}
    db = _mysql.connect(host=db_spec['host'], port=db_spec['port'], user=db_spec['username'],
                        passwd=db_spec['password'], db=db_spec['schema'])
    db.query("SET NAMES 'utf8'")

    db.query('SELECT idproducts_image, path, width, height, format FROM products_image')
    rs = db.store_result()
    image_cnt = 0
    missing_cnt = 0
    size_mismatch = 0
    fmt_mismatch = 0
    tot_cnt = rs.num_rows()
    ts = time.time()
    processed = image_cnt
    t_delta = 30
    print 'STARTED'
    for i in xrange(tot_cnt):
        ts1 = time.time()
        if ts1 - ts > t_delta:
            p_delta = image_cnt - processed
            rate = p_delta / (float(ts1 - ts) / 60.0)
            print str.format(
                '{0} of {1} items processed ({2:.2%}), {5} missing, {6} size mismatch, {7} format mismatch. '
                'Rate: {3:.1f}/min. Est: {4:.1f}min', image_cnt, tot_cnt, float(image_cnt) / tot_cnt, rate,
                (tot_cnt - image_cnt) / rate, missing_cnt, size_mismatch, fmt_mismatch)
            ts = ts1
            processed = image_cnt

        pid, path, w, h, fmt = rs.fetch_row()[0]
        full_path = os.path.join(BASE_PATH, path)
        image_cnt += 1
        try:
            img = Image.open(full_path)
            if img.size != (int(w), int(h)):
                size_mismatch += 1
            if img.format != fmt:
                fmt_mismatch += 1
        except IOError:
            missing_cnt += 1

    print 'DONE'
    db.close()


image_check()