# coding=utf-8
import datetime
import json
import os
from core import RoseVisionDb
import global_settings as gs
from scripts.push_utils import price_changed
from utils.utils_core import get_logger

__author__ = 'Zephyre'


class PriceChangeTasker(object):
    @classmethod
    def tag_outdated(cls, **kwargs):
        """
        将价格过期的单品在数据库中标记出来。
        @param kwargs: duration：多少天以后，单品的价格趋势信息就算是过期了？默认为7天。
        """
        duration = int(kwargs['duration'][0]) if 'duration' in kwargs else 7
        ts = (datetime.datetime.now() - datetime.timedelta(duration)).strftime('"%Y-%m-%d %H:%M:%S"')
        with RoseVisionDb(getattr(gs, 'DB_SPEC')) as db:
            db.query(str.format('''
            UPDATE products AS p1
            JOIN products_price_history AS p2 ON p1.idproducts=p2.idproducts
            SET p1.price_change='0'
            WHERE p1.price_change!='0' AND p2.date<'{0}'
            ''', ts))

    @classmethod
    def tag_changed(cls, **kwargs):
        """
        将价格发生变化的单品在数据库中标记出来，同时返回
        @param kwargs: brand: 需要处理的品牌。如果为None，则对所有的品牌进行处理。
                        start：价格变化检查的开始时间。如果为None，则为昨天凌晨。
                        end：价格变化检查的结束时间。如果为None，则为今天凌晨。
        """

        # 是否处于静默模式
        silent = 's' in kwargs

        # 如果没有指定brand，则对数据库中存在的所有brand进行处理
        brand_list = [int(val) for val in kwargs['brand']] if 'brand' in kwargs else None
        start_ts = kwargs['start'][0] if 'start' in kwargs else None
        end_ts = kwargs['end'][0] if 'end' in kwargs else None

        # 得到价格变化的信息列表
        change_detection = price_changed(brand_list, start_ts, end_ts)
        changes = {'U': [], 'D': []}
        for change_type in ['discount_down', 'price_down', 'discount_up', 'price_up']:
            for brand in change_detection[change_type]:
                for fingerprint, model_data in change_detection[change_type][brand].items():
                    for product in model_data['products']:
                        pid = product['idproducts']
                        c = '0'
                        if change_type in ['discount_down', 'price_down']:
                            c = 'D'
                        elif change_type in ['discount_up', 'price_up']:
                            c = 'U'
                        if c != '0':
                            changes[c].append(pid)

        with RoseVisionDb(getattr(gs, 'DB_SPEC')) as db:
            db.start_transaction()
            try:
                for change_type in ['U', 'D']:
                    db.update({'price_change': change_type}, 'products',
                              str.format('idproducts IN ({0})', ','.join(str(tmp) for tmp in changes[change_type])),
                              timestamps=['update_time'])
            except:
                db.rollback()
                raise
            finally:
                db.commit()

        return change_detection

    @classmethod
    def run(cls, **kwargs):
        logger = kwargs['logger'] if 'logger' in kwargs else get_logger(to_file=True)
        logger.info('PRICE-CHANGE DETECTION STARTED')

        cls.tag_outdated(**kwargs)
        result = cls.tag_changed(**kwargs)
        if not result:
            return

        dst = kwargs['dst'][0] if 'dst' in kwargs and kwargs['dst'] else '~/push_works/push.log'
        ssh_user, ssh_host, ssh_port = [None] * 3
        if 'ssh' in kwargs and kwargs['ssh']:
            ssh_str = kwargs['ssh'][0]
            ssh_user, ssh = ssh_str.split('@')
            if ':' in ssh:
                ssh_host, ssh_port = ssh.split(':')
            else:
                ssh_host = ssh
                # Default ssh port
                ssh_port = ''

        if not ssh_host:
            # 如果没有SSH信息，说明不需要通过SFTP将结果传输到远端服务器上
            return

        # 将变动结果写入临时目录
        file_name = str.format('/tmp/price_change_{0}.log', datetime.datetime.now().strftime('%Y%m%d%H%M%S'))
        with open(file_name, 'wb') as f:
            f.write(json.dumps(result, ensure_ascii=False).encode('utf-8'))

        # 指明了SSH信息，需要上传到远程服务器作为备份
        logger.info('UPLOADING...')
        ssh_port_str = str.format('-P {0}', ssh_port) if ssh_port else ''
        os.system(str.format('scp {0} {4} {1}@{2}:{3} > /dev/null', ssh_port_str, ssh_user, ssh_host, dst, file_name))
        os.remove(file_name)

        logger.info('DONE')
