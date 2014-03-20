# coding=utf-8
import datetime
from core import RoseVisionDb
import global_settings as glob
import os
from utils.utils_core import get_logger

__author__ = 'Zephyre'


class BackupTasker(object):
    @classmethod
    def run(cls, **kwargs):
        logger = kwargs['logger'] if 'logger' in kwargs else get_logger()
        logger.info('BACKUP STARTED')

        try:
            db_spec = getattr(glob, 'DATABASE')[kwargs['DATABASE']]
            # {"host": "127.0.0.1", "port": 1228, "schema": "editor_stores", "username": "rose", "password": "rose123"}
            host = db_spec['host'] if 'host' in db_spec else '127.0.0.1'
            port = db_spec['port'] if 'port' in db_spec else 3306
            schema = db_spec['schema']
            db_user = db_spec['username']
            db_pwd = db_spec['password']

            ssh_user, ssh_host, ssh_port, dst = (None, None, 22, '')
            if 'SSH_USER' in kwargs:
                ssh_user = kwargs['SSH_USER']
            if 'SSH_HOST' in kwargs:
                ssh_host = kwargs['SSH_HOST']
            if 'SSH_PORT' in kwargs:
                ssh_port = int(kwargs['SSH_PORT'])
            if 'SSH_DST' in kwargs:
                dst = kwargs['SSH_DST']
        except (AttributeError, KeyError):
            logger.exception('Invalid database specification.')
            return

        host_str = str.format('-h{0}', host) if host else ''
        port_str = str.format('-P{0}', port) if port else ''

        # 获得schema中的所有table
        with RoseVisionDb(db_spec) as db:
            tables = [tmp[0] for tmp in db.query(
                str.format('select TABLE_NAME from information_schema.tables where TABLE_SCHEMA="{0}"',
                           schema)).fetch_row(maxrows=0)]

        # tables = ['brand_info', 'brand_duration', 'translation', 'region_info', 'images_store', 'mfashion_tags',
        #           'original_tags', 'products', 'products_image', 'products_mfashion_tags', 'products_original_tags',
        #           'products_price_history', 'products_release', 'products_translate']
        for table in tables:
            logger.info(str.format('EXPORTING {0}...', table))
            os.system(
                str.format('mysqldump {3} {4} -u {0} -p{1} -c {2} {5} > /tmp/{5}.sql', db_user, db_pwd, schema,
                           host_str, port_str, table))

        # 将所有的sql文件打包
        logger.info('ZIPPING...')
        backup_name = os.path.join(getattr(glob, 'STORAGE_PATH'), 'backups',
                                   str.format('{0}_auto_backup.7z', datetime.datetime.now().strftime('%Y%m%d_%H%M%S')))
        os.system(str.format('7z a -mx7 {0} {1} > /dev/null', backup_name,
                             ' '.join(str.format('/tmp/{0}.sql', tmp) for tmp in tables)))

        # 移除临时sql文件
        logger.info('REMOVING TEMPORARY SQL FILES...')
        for rm_file in [str.format('/tmp/{0}.sql', tmp) for tmp in tables]:
            os.remove(rm_file)

        # SCP
        if ssh_user and ssh_host and ssh_port:
            # 指明了SSH信息，需要上传到远程服务器作为备份
            logger.info('UPLOADING...')
            ssh_port_str = str.format('-P {0}', ssh_port) if ssh_port else ''
            os.system(
                str.format('scp {0} {4} {1}@{2}:{3} > /dev/null', ssh_port_str, ssh_user, ssh_host, dst, backup_name))

        logger.info(str.format('AUTO BACKUP COMPLETED: {0}', backup_name))


if __name__ == "__main__":
    param = {"DATABASE": "DB_SPEC"}
    BackupTasker.run(DATABASE='DB_SPEC')