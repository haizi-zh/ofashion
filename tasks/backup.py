# coding=utf-8
import datetime
from utils.db import RoseVisionDb
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

        tmp_file = '/tmp/single_backup.sql'
        # single-transaction备份
        logger.info('EXPORTING...')
        os.system(
            str.format('mysqldump {3} {4} -u {0} -p{1} --single-transaction {2} > {5}', db_user,
                       db_pwd, schema, host_str, port_str, tmp_file))

        # 打包
        logger.info('ZIPPING...')
        backup_name = os.path.join(getattr(glob, 'STORAGE')['STORAGE_PATH'], 'backups',
                                   str.format('{0}_auto_backup.7z', datetime.datetime.now().strftime('%Y%m%d_%H%M%S')))
        os.system(str.format('7z a -mx7 {0} {1} > /dev/null', backup_name, tmp_file))

        # 移除临时sql文件
        logger.info('REMOVING TEMPORARY SQL FILES...')
        os.remove(tmp_file)

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