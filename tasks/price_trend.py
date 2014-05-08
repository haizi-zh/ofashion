# coding=utf-8
import datetime
import json
import os
from scripts.dbman import PublishRelease
from utils.db import RoseVisionDb
import global_settings as gs
from scripts.push_utils import price_changed
from utils.utils_core import get_logger

__author__ = 'Zephyre'


class PriceTrendTasker(object):
    """
    功能：1. 将过期的价格趋势标签清除。
         2. 在指定时间范围内分析单品的价格变化趋势，并打上标签。
    """

    @classmethod
    def tag_outdated(cls, **kwargs):
        """
        将价格过期的单品在数据库中标记出来。
        @param kwargs: duration：多少天以后，单品的价格趋势信息就算是过期了？默认为7天。
        """
        duration = int(kwargs['duration'][0]) if 'duration' in kwargs else 7
        ts = (datetime.datetime.now() - datetime.timedelta(duration)).strftime('"%Y-%m-%d %H:%M:%S"')
        with RoseVisionDb(getattr(gs, 'DATABASE')['DB_SPEC']) as db:
            pid_list = [int(tmp[0]) for tmp in
                        db.query(str.format('''
            SELECT prod.idproducts FROM products AS prod
            JOIN products_price_history AS price ON prod.idproducts=price.idproducts
            WHERE prod.price_change!='0' AND price.date<'{0}'
            ''', ts)).fetch_row(maxrows=0)]

            max_bulk = 1000
            offset = 0
            while offset < len(pid_list):
                tmp_list = pid_list[offset:offset + max_bulk]
                offset += max_bulk
                db.query(str.format('''
                UPDATE products SET price_change='0', update_time='{1}' WHERE idproducts IN ({0})
                ''', ', '.join(str(tmp) for tmp in tmp_list), datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')))

            db.query('UPDATE products SET price_change="0" WHERE price_change!="0" AND offline!=0')

    @classmethod
    def tag_changed(cls, **kwargs):
        """
        将价格发生变化的单品在数据库中标记出来，同时返回
        @param kwargs: brand: 需要处理的品牌。如果为None，则对所有的品牌进行处理。
                        start：价格变化检查的开始时间。如果为None，则为昨天凌晨。
                        end：价格变化检查的结束时间。如果为None，则为今天凌晨。
                        start_delta: 调整时间（单位为天）。比如：delta为0.5，则表示在start/end的基础上，再往后延长半天。
                        end_delta
        """

        # 是否处于静默模式
        silent = 's' in kwargs

        # 如果没有指定brand，则对数据库中存在的所有brand进行处理
        brand_list = [int(val) for val in kwargs['brand']] if 'brand' in kwargs else None
        start_ts = kwargs['start'] if 'start' in kwargs else None
        end_ts = kwargs['end'] if 'end' in kwargs else None
        start_delta = datetime.timedelta(kwargs['start_delta']) if 'start_delta' in kwargs else datetime.timedelta(0)
        end_delta = datetime.timedelta(kwargs['end_delta']) if 'end_delta' in kwargs else datetime.timedelta(0)

        # 得到价格变化的信息列表
        change_detection = price_changed(brand_list, start_ts, end_ts, start_delta, end_delta)
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

        with RoseVisionDb(getattr(gs, 'DATABASE')['DB_SPEC']) as db:
            db.start_transaction()
            try:
                for change_type in ['U', 'D']:
                    if not changes[change_type]:
                        continue
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
        logger = kwargs['logger'] if 'logger' in kwargs else get_logger()
        logger.info('PRICE-CHANGE DETECTION STARTED')

        logger.info('CLEARING OUTDATED RECORDS')
        cls.tag_outdated(**kwargs)
        logger.info('GENERATING PRICE TRENDS')
        result = cls.tag_changed(**kwargs)
        if not result:
            logger.info('NO PRICE TRENDS DETECTED')
            return

        dst = kwargs['dst'] if 'dst' in kwargs and kwargs['dst'] else '~/push_works/push.log'
        ssh_user, ssh_host, ssh_port = [None] * 3
        if 'ssh' in kwargs and kwargs['ssh']:
            ssh_str = kwargs['ssh']
            ssh_user, ssh = ssh_str.split('@')
            if ':' in ssh:
                ssh_host, ssh_port = ssh.split(':')
            else:
                ssh_host = ssh
                # Default ssh port
                ssh_port = 22

        if ssh_host:
            # 如果没有SSH信息，说明不需要通过SFTP将结果传输到远端服务器上
            logger.info('UPLOADING PRICE TRENDS')
            # 将变动结果写入临时目录
            file_name = str.format('/tmp/price_change_{0}.log', datetime.datetime.now().strftime('%Y%m%d%H%M%S'))
            with open(file_name, 'wb') as f:
                f.write(json.dumps(result, ensure_ascii=False).encode('utf-8'))

            # 指明了SSH信息，需要上传到远程服务器作为备份

            ssh_port_str = str.format('-P {0}', ssh_port) if ssh_port else ''
            ssh_cmd = str.format('scp {0} {4} {1}@{2}:{3} > /dev/null', ssh_port_str, ssh_user, ssh_host, dst,
                                 file_name)
            logger.info(str.format('UPLOADING: {0}', ssh_cmd))
            os.system(ssh_cmd)
            os.remove(file_name)

        # 发布更新的商品
        updated_brands = set([])
        for k in ('discount_down', 'discount_up', 'price_down', 'price_up'):
            updated_brands = updated_brands.union(result['discount_down'].keys())

        for brand in updated_brands:
            PublishRelease(brand).run()

        logger.info('DONE')


if __name__ == '__main__':
    pass
    # PriceChangeTasker.run()
