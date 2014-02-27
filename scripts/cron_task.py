#!/usr/bin/python
# coding=utf-8

import logging
import sys
import os
import datetime
import errno
import global_settings as glob
from Queue import Queue
import re

sys.path.append('/home/rose/MStore/src')

__author__ = 'Zephyre'

logging.basicConfig(format='%(asctime)-24s%(levelname)-8s%(message)s', level='INFO')
logger = logging.getLogger()


def default_error(msg):
    logger.error(msg)


def mstore_help():
    print str.format('Available commands are: {0}', ', '.join(cmd_list))


def make_sure_path_exists(path):
    try:
        os.makedirs(path)
    except OSError as exception:
        if exception.errno != errno.EEXIST:
            raise


def test(param_dict):
    logger.debug('This is a debug test.')
    logger.info('This is a info test.')
    logger.warn('This is a warn test.')
    logger.error('This is a error test.')
    os.system('echo "aaaa"')
    os.system('echo "bbb" > /dev/null')
    os.system('echo "ccc"')

    print 'DONE'


def sync(param_dict):
    user = param_dict['u'][0]
    port = param_dict['p'][0]
    host = param_dict['h'][0]
    dst = param_dict['d'][0] if 'd' in param_dict else ''

    done_name=None
    if 'f' in param_dict:
        file_name = param_dict['f'][0]
    else:
        # 取得最后一个备份文件的路径
        storage_path = os.path.join(getattr(glob, 'STORAGE_PATH'), 'backups')
        tmp = sorted(filter(lambda val:re.search(r'^\d{8}_\d{6}[^\.]+\.7z$', val), os.listdir(storage_path)))
        if tmp:
            file_name = os.path.join(storage_path, tmp[-1])
        else:
            file_name = None

        done_name = file_name + '.done'
        with open(done_name, mode='w') as f:
            f.write('DONE\n')

    if file_name:
        cmd = str.format('scp -P {0} {1} {2}@{3}:{4}', port, file_name, user, host, dst)
        os.system(cmd)
        if done_name:
            cmd = str.format('scp -P {0} {1} {2}@{3}:{4}', port, done_name, user, host, dst)
            os.system(cmd)


def backup_all(param_dict):
    storage_path = getattr(glob, 'STORAGE_PATH')
    original_path = os.getcwd()
    os.chdir(storage_path)

    logger.info(str.format('{0}\tAUTO BACKUP STARTED', datetime.datetime.now().strftime('%Y%m%d_%H%M%S')))

    backup_name = str.format('{0}_auto_backup', datetime.datetime.now().strftime('%Y%m%d_%H%M%S'))
    path = os.path.join(storage_path, 'backups')
    make_sure_path_exists(path)
    template = str.format('{0}/{1}', path, backup_name)
    sys_cmd = str.format(
        'mysqldump -c -u rose -prose123 --databases editor_stores > {0}.sql', template)
    logger.debug(sys_cmd)
    os.system(sys_cmd)
    sys_cmd = str.format('7z a {0}.7z {0}.sql > /dev/null', template)
    logger.debug(sys_cmd)
    os.system(sys_cmd)
    sys_cmd = str.format('rm {0}.sql', template)
    logger.debug(sys_cmd)
    os.system(sys_cmd)

    os.chdir(original_path)
    logger.info(str.format('{0}\tAUTO BACKUP COMPLETED', datetime.datetime.now().strftime('%Y%m%d_%H%M%S')))


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
    argument_parser(sys.argv)()
