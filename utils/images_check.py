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
                    #错误标志
                    url_err = checksum_err = path_err = width_err = height_err = fmt_err = size_err = False

                    #check
                    #todo url_err待测，需要打开连接验证是否正常，默认正常
                    #url空或者含有CJK字符
                    if url is None or True in map(lambda x: is_chinese(x), (i for i in unicode(url, 'utf-8'))):
                        url_err = True
                    else:
                        hash_url = hashlib.sha1(url).hexdigest()
                        if hash_url != os.path.splitext(os.path.split(path)[-1])[0]:
                            path_err = True


                    try:
                        f = open(full_path, 'rb')
                        cur_check = hashlib.md5(f.read()).hexdigest()
                        if cur_check != checksum:
                            checksum_err = True
                    except:
                        checksum_err = True
                        path_err = True

                    #check width height format,size
                    img = Image.open(full_path)
                    (cur_width, cur_height) = img.size
                    cur_format = img.format
                    cur_size = os.path.getsize(full_path)

                    if cur_width != int(width) or int(width) < 128:
                        width_err = True
                    if cur_height != int(height) or int(height) < 128:
                        height_err = True
                    if cur_format != fmt:
                        fmt_err = True
                    if cur_size != int(size):
                        size_err = True

                    if url_err or checksum_err or path_err or width_err or height_err or fmt_err or size_err:
                        logging.error((datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'), 'Detail:', (
                            checksum,
                            'url_err' if url_err else None,
                            'path_err' if path_err else None,
                            'width_err' if width_err else None,
                            'height_err' if height_err else None,
                            'fmt_err' if fmt_err else None,
                            'size_err' if size_err else None)))
                        pass
                    #运行脚本显示百分比进度
                    count += 1
                    percent = float(count) / total * 100
                    print '\r%.3f%% :%s>' % (percent, int(percent) * '='),

            except:
                # db.rollback()
                raise


def is_chinese(uchar):
    """判断一个unicode是否是汉字"""
    if u'\u4e00' <= uchar <= u'\u9fa5':
        return True
    else:
        return False


if __name__ == '__main__':
    t = ImagesCheck()
    t.run()