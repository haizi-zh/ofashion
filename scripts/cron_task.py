#!/usr/bin/python
# coding=utf-8

import datetime
import logging
import sys
import os
import errno
import global_settings as glob
import re
from utils.utils import parse_args

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


def test(param_dict):
    logger.info('TEST COMPLETED')


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
    os.system(str.format('7z a -mx7 {0} {1} > null', backup_name,
                         ' '.join(str.format('/tmp/{0}.sql', tmp) for tmp in tables)))

    # 移除临时sql文件
    logger.info('REMOVING TEMPORARY SQL FILES...')
    os.system(str.format('rm {0}', ' '.join(str.format('/tmp/{0}.sql', tmp) for tmp in tables)))

    # 建立完成标志
    with open(backup_name + '.done', 'w') as f:
        f.write('DONE\n')

    # SCP
    if ssh_user and ssh_host and ssh_port:
        # 指明了SSH信息，需要上传到远程服务器作为备份
        logger.info('UPLOADING...')
        ssh_port_str = str.format('-P {0}', ssh_port) if ssh_port else ''
        os.system(str.format('scp {0} {4} {1}@{2}:{3} > null', ssh_port_str, ssh_user, ssh_host, dst, backup_name))
        os.system(str.format('scp {0} {4} {1}@{2}:{3} > null', ssh_port_str, ssh_user, ssh_host, dst, backup_name + '.done'))

    logger.info(str.format('AUTO BACKUP COMPLETED: {0}', backup_name))
    os.chdir(original_path)


def argument_parser(args):
    if len(args) < 2:
        return lambda: default_error('Incomplete arguments.')

    cmd = args[1]

    # 解析命令行参数
    param_dict = {}
    q = Queue()
    for tmp in args[2:]:
        q.put(tmp)
    param_name = None
    param_value = None
    while not q.empty():
        term = q.get()
        if re.search(r'--(?=[^\-])', term):
            tmp = re.sub('^-+', '', term)
            if param_name:
                param_dict[param_name] = param_value
            param_name = tmp
            param_value = None
        elif re.search(r'-(?=[^\-])', term):
            tmp = re.sub('^-+', '', term)
            for tmp in list(tmp):
                if param_name:
                    param_dict[param_name] = param_value
                    param_value = None
                param_name = tmp
        else:
            if param_name:
                if param_value:
                    param_value.append(term)
                else:
                    param_value = [term]
    if param_name:
        param_dict[param_name] = param_value

    if 'debug' in param_dict or 'D' in param_dict:
        if 'P' in param_dict:
            port = int(param_dict['P'][0])
        else:
            port = getattr(glob, 'DEBUG_PORT')
        import pydevd

        pydevd.settrace('localhost', port=port, stdoutToServer=True, stderrToServer=True)
    for k in ('debug', 'D', 'P'):
        try:
            param_dict.pop(k)
        except KeyError:
            pass

    if cmd == 'test':
        func = test
    elif cmd == 'backup-all':
        func = backup_all
    elif cmd == 'sync':
        func = sync
    else:
        func = lambda msg: default_error(str.format('Unknown command: {0}', cmd))

    return lambda: func(param_dict)


if __name__ == "__main__":
    ret = parse_args(sys.argv)
    func_dict = {'test': test, 'backup-all': backup_all, 'sync': sync}
    if ret:
        cmd = ret['cmd']
        param = ret['param']
        if cmd not in func_dict:
            logger.error(str.format('INVALID COMMAND: {0}', cmd))
        else:
            func_dict[cmd](param)