# coding=utf-8
__author__ = 'Administrator'

from core import RoseVisionDb
import global_settings as gs
import datetime
import hashlib
import os
from PIL import Image
import logging

logging.basicConfig(filename='ImagesCheck.log', level=logging.DEBUG)


class ImagesCheck(object):
    """
    图片信息检验
    @param param_dict:
    """

    @classmethod
    def run(cls, logger=None, **kwargs):

        storage_path = os.path.normpath(os.path.join(getattr(gs, 'STORAGE_PATH'), 'products/images'))
        with RoseVisionDb(getattr(gs, 'DB_SPEC')) as db:
            rs = db.query_match(['checksum', 'url', 'path', 'width', 'height', 'format', 'size'],
                                'images_store').fetch_row(
                maxrows=0)
            db.start_transaction()
            total = len(rs)
            count = 0
            try:
                for checksum, url, path, width, height, fmt, size in rs:
                    full_path = os.path.normpath(os.path.join(storage_path, path))
                    #check sum
                    with open(full_path, 'rb') as f:
                        cur_check = hashlib.md5(f.read()).hexdigest()
                    #check width height format,size
                    img = Image.open(full_path)
                    (cur_width, cur_height) = img.size
                    cur_format = img.format
                    cur_size = os.path.getsize(full_path)

                    if cur_check == checksum and cur_width == int(width) and cur_height == int(
                            height) and cur_format == fmt and cur_size == int(size):
                        pass
                    else:
                        logging.error((datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'), 'Detail:', (
                            checksum, url, path, width, height, fmt, size)))
                        pass
                    #运行脚本显示百分比进度
                    count += 1
                    percent = float(count) / total * 100
                    print '\r%.3f%% :%s>' % (percent, int(percent) * '='),

            except:
                # db.rollback()
                raise


if __name__ == '__main__':
    t = ImagesCheck()
    t.run()