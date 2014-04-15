# coding=utf-8
__author__ = 'Administrator'
# import pydevd
#
# pydevd.settrace('127.0.0.1', port=33333, stdoutToServer=True, stderrToServer=True)
from utils.db import RoseVisionDb
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
        logging.info('IMAGE CHECK STARTED!!!!')
        #check flag for urls and images
        url_flag = True if 'url_flag' not in kwargs else kwargs['url_flag']
        img_flag = True if 'img_flag' not in kwargs else kwargs['img_flag']

        storage_path = os.path.normpath(os.path.join(getattr(gs, 'STORAGE_PATH'), 'products/images'))
        with RoseVisionDb(getattr(gs, 'DATABASE')['DB_SPEC']) as db:
            rs = db.query_match(['checksum', 'url', 'path', 'width', 'height', 'format', 'size'],
                                'images_store',).fetch_row(
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
                    """
                    可能出现的错误：height_err or width_err and size_err代表图片更新过，数据库size_err未更新
                                    height_err or width_err   无size_err代表图片过小，小于128X128
                                    checksum_err and path_err出现代表文件不存在，打不开

                    """
                    #todo url_err待测，需要打开连接验证是否正常，默认正常
                    #check url
                    if url_flag:
                        if not url or not url.strip():
                            url_err = True
                    # check img
                    if img_flag:
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
                            'size_err' if size_err else None,
                            '-------checked value:',
                            cur_check if cur_check else 'NA---',
                            cur_width if cur_width else 'NA---',
                            cur_height if cur_height else 'NA---',
                            cur_format if cur_format else 'NA---',
                            cur_size if cur_size else 'NA---',
                        )))
                        pass
                    #运行脚本显示百分比进度
                    count += 1
                    percent = float(count) / total * 100
                    print '\r%.3f%% :%s>' % (percent, int(percent) * '='),

            except:
                raise
        logging.info('IMAGE CHECK ENDED!!!!')

if __name__ == '__main__':
    t = ImagesCheck()
    t.run()