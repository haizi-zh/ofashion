#!/usr/bin/env python
# coding=utf-8
from email.mime.text import MIMEText
import json
import os
import re
import hashlib
import smtplib
import urlparse
from core import RoseVisionDb
import global_settings as gs

# import pydevd
# pydevd.settrace('localhost', port=7103, stdoutToServer=True, stderrToServer=True)

from utils.utils_core import unicodify, get_logger


class ImageCheckAlertTasker(object):
    """
    解决如下问题：有时候发布出来的release数据中，单品描述和单品图片不一致，产生错乱。我们需要检查release表中的每一条数据，
    比较一下brand_id字段和path中的品牌是否一致
    """

    def __init__(self, param=None):
        self.tot = 1
        self.progress = 0
        if param and 'brand' in param:
            self.brand_list = [int(val) for val in param['brand']]
        else:
            self.brand_list = None

    def get_msg(self):
        return str.format('{0}/{1}({2:.1%}) PROCESSED', self.progress, self.tot,
                          float(self.progress) / self.tot) if self.tot > 0 else 'IDLE'

    @classmethod
    def alert(cls, title, content):
        # 确定收信人
        try:
            group = getattr(gs, 'REPORTS')['ALERT']
            if not isinstance(group, list):
                group = [group]
            recipients = {}
            for g in group:
                for key, value in getattr(gs, 'EMAIL_GROUPS')[g].items():
                    recipients[key] = value
                    # recipent_addrs = gs.EMAIL_ADDR.values()  # ['haizi.zh@gmail.com', 'haizi.zh@qq.com']
        except (TypeError, AttributeError, KeyError):
            return

        msg = MIMEText(unicodify(content), _subtype='html', _charset='utf-8')
        # msg = MIMEMultipart('alternative')
        msg['Subject'] = u'MFashion图像监控报警：' + title
        msg['From'] = 'MStore Admin <buddy@mfashion.com.cn>'
        msg['To'] = ', '.join([unicode.format(u'{0} <{1}>', item[0], item[1]) for item in recipients.items()])

        server = smtplib.SMTP_SSL('smtp.exmail.qq.com', 465)
        server.login('buddy@mfashion.com.cn', 'rose123')
        server.sendmail('buddy@mfashion.com.cn', recipients.values(), msg.as_string())
        server.quit()

    @classmethod
    def run(cls, **kwargs):
        logger = kwargs['logger'] if 'logger' in kwargs else get_logger()
        logger.info('IMAGE CHECK ALERT STARTED')

        with RoseVisionDb(getattr(gs, 'DB_SPEC')) as db:
            rs = db.query('SELECT fingerprint, brand_id, image_list, cover_image FROM products_release',
                          use_result=True)
            while True:
                bulk = rs.fetch_row(maxrows=100)
                if not bulk:
                    break

                is_err = False
                for fingerprint, brand_id, jlist, jcover in bulk:
                    try:
                        image_list = json.loads(jlist)
                        for path in [tmp['path'] for tmp in image_list]:
                            if not re.search(str.format(r'^{0}_', brand_id), path):
                                content = str.format('fingerprint={0}, image_list={1}', fingerprint, jlist)
                                logger.error(content)
                                cls.alert(str.format('INVALID IMAGES: {0}!!!', fingerprint), content)
                                is_err = True
                                break

                        cover = json.loads(jcover)
                        if not re.search(str.format(r'^{0}_', brand_id), cover['path']):
                            content = str.format('fingerprint={0}, jcover={1}', fingerprint, jcover)
                            logger.error(content)
                            cls.alert(str.format('INVALID IMAGES: {0}!!!', fingerprint), content)
                            is_err = True
                            break
                    except:
                        cls.alert(str.format('INVALID IMAGES: {0}!!!', fingerprint),
                                  str.format('fingerprint={0}, jlist={1}, jcover={2}', fingerprint, jlist, jcover))
                        raise

                if is_err:
                    break

        logger.info('DONE!')


if __name__ == '__main__':
    ImageCheckAlertTasker.run()