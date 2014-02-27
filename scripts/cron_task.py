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


def test():
    logger.debug('This is a debug test.')
    logger.info('This is a info test.')
    logger.warn('This is a warn test.')
    logger.error('This is a error test.')
    os.system('echo "aaaa"')
    os.system('echo "bbb" > /dev/null')
    os.system('echo "ccc"')

    print 'DONE'


def backup_all():
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
        return default_error()

    cmd = args[1]

    # 解析命令行参数
    param_dict = {}
    q = Queue()
    for tmp in args[2:]:
        q.put(tmp)
    param_name = None
    param_value = None
    while not q.empty():
        tmp = q.get()
        if re.search(r'--(?=[^\-])', tmp):
            tmp = re.sub('^-+', '', tmp)
            if param_name:
                param_dict[param_name] = param_value

            param_name = tmp
            param_value = None
        elif re.search(r'-(?=[^\-])', tmp):
            tmp = re.sub('^-+', '', tmp)
            if param_name:
                param_dict[param_name] = param_value

            for tmp in list(tmp):
                param_dict[tmp] = None
            param_name = None
            param_value = None
        else:
            if param_name:
                if param_value:
                    param_value.append(tmp)
                else:
                    param_value = [tmp]
    if param_name:
        param_dict[param_name] = param_value

    if cmd == 'test':
        return test
    elif cmd == 'backup-all':
        return backup_all
    else:
        return lambda msg: default_error(str.format('Unknown command: {0}', cmd))

if __name__ == "__main__":
    argument_parser(sys.argv)()
