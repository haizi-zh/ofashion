#!/usr/bin/python
# coding=utf-8

import datetime
import logging
import os
import errno
import sys
import global_settings as glob
import re
# from utils import utils
from utils.utils_core import parse_args, unicodify, get_logger

__author__ = 'Zephyre'

logging.basicConfig(format='%(asctime)-24s%(levelname)-8s%(message)s', level='INFO')
logger = logging.getLogger()


def default_error(msg):
    logger.error(msg)


def make_sure_path_exists(path):
    try:
        os.makedirs(path)
    except OSError as exception:
        if exception.errno != errno.EEXIST:
            raise


def backup_all(param_dict):
    logger.info('AUTO BACKUP STARTED')
    storage_path = getattr(glob, 'STORAGE_PATH')
    original_path = os.getcwd()
    os.chdir(storage_path)

    user = param_dict['u'][0] if 'u' in param_dict and param_dict['u'] else None
    password = param_dict['p'][0] if 'p' in param_dict and param_dict['p'] else None
    host = param_dict['host'][0] if 'host' in param_dict and param_dict['host'] else None
    port = param_dict['port'][0] if 'port' in param_dict and param_dict['port'] else None
    db = param_dict['db'][0] if 'db' in param_dict and param_dict['db'] else None
    dst = param_dict['dst'][0] if 'dst' in param_dict and param_dict['dst'] else ''
    ssh_user, ssh_host, ssh_port = [None] * 3
    if 'ssh' in param_dict and param_dict['ssh']:
        ssh_str = param_dict['ssh'][0]
        ssh_user, ssh = ssh_str.split('@')
        if ':' in ssh:
            ssh_host, ssh_port = ssh.split(':')
        else:
            ssh_host = ssh
            # Default ssh port
            ssh_port = ''
    host_str = str.format('-h{0}', host) if host else ''
    port_str = str.format('-P{0}', port) if port else ''

    tables = ['brand_info', 'region_info', 'images_store', 'mfashion_tags', 'original_tags', 'products',
              'products_image', 'products_price_history', 'products_release']
    for table in tables:
        logger.info(str.format('EXPORTING {0}...', table))
        os.system(str.format('mysqldump {3} {4} -u {0} -p{1} -c {2} {5} > /tmp/{5}.sql',
                             user, password, db, host_str, port_str, table))

    # 将所有的sql文件打包
    logger.info('ZIPPING...')
    backup_name = os.path.join(storage_path, 'backups',
                               str.format('{0}_auto_backup.7z', datetime.datetime.now().strftime('%Y%m%d_%H%M%S')))
    os.system(str.format('7z a -mx7 {0} {1} > /dev/null', backup_name,
                         ' '.join(str.format('/tmp/{0}.sql', tmp) for tmp in tables)))

    # 移除临时sql文件
    logger.info('REMOVING TEMPORARY SQL FILES...')
    for rm_file in [str.format('/tmp/{0}.sql', tmp) for tmp in tables]:
        os.remove(rm_file)

    # 建立完成标志
    with open(backup_name + '.done', 'w') as f:
        f.write('DONE\n')

    # SCP
    if ssh_user and ssh_host and ssh_port:
        # 指明了SSH信息，需要上传到远程服务器作为备份
        logger.info('UPLOADING...')
        ssh_port_str = str.format('-P {0}', ssh_port) if ssh_port else ''
        os.system(str.format('scp {0} {4} {1}@{2}:{3} > /dev/null', ssh_port_str, ssh_user, ssh_host, dst, backup_name))
        os.system(
            str.format('scp {0} {4} {1}@{2}:{3} > /dev/null', ssh_port_str, ssh_user, ssh_host, dst,
                       backup_name + '.done'))

    logger.info(str.format('AUTO BACKUP COMPLETED: {0}', backup_name))
    os.chdir(original_path)


def restore(param_dict):
    """
    导入最近的一次数据库文件
    @param param:
    """
    user = param_dict['u'][0] if 'u' in param_dict and param_dict['u'] else None
    password = param_dict['p'][0] if 'p' in param_dict and param_dict['p'] else None
    host = param_dict['host'][0] if 'host' in param_dict and param_dict['host'] else None
    port = param_dict['port'][0] if 'port' in param_dict and param_dict['port'] else None
    db = param_dict['db'][0] if 'db' in param_dict and param_dict['db'] else None
    dst = param_dict['dst'][0] if 'dst' in param_dict and param_dict['dst'] else ''

    os.chdir(dst)

    # 查找最近的一次.done文件
    file_candates = sorted(filter(lambda val: re.search(r'\d{8}_\d{6}[^\\/]+\.done$', val), os.listdir(dst)))
    # 最近的一个.done文件对应的日期距今不应该超过24小时
    if not file_candates:
        return
    done_name = file_candates[-1]
    done_time = datetime.datetime.strptime(re.search(r'(\d{8}_\d{6})[^\\/]+\.done$', done_name).group(1),
                                           '%Y%m%d_%H%M%S')
    if datetime.datetime.now() - done_time >= datetime.timedelta(1):
        # 不早于24小时
        return
    # 找到数据文件的名字
    file_name = re.sub(r'(.+)\.done$', r'\1', done_name)
    if not os.path.isfile(file_name):
        return
    os.system(str.format('7z x {0}', file_name))

    pass


def my_import(name):
    tmp = name.split('.')
    if len(tmp) == 1:
        kclass = __import__(name)
    else:
        mod_name, mod_class = '.'.join(tmp[:-1]), tmp[-1]
        mod = __import__(mod_name, fromlist=[mod_class])
        kclass = getattr(mod, mod_class)

    return kclass


if __name__ == "__main__":
    ret = parse_args(sys.argv)

    for task_name, task_param in getattr(glob, 'CRON_TASK', {}).items():
        try:
            class_name = task_param['classname']
            func = getattr(my_import(class_name),'run')
            func(**task_param['param'])

        except (KeyError,):
            logger = get_logger(to_file=True).exception(unicode.format(u'Invalid task name: {0}',
                                                                       unicodify(task_name)).encode('utf-8'))