import logging
import sys
import os
import datetime
import errno

sys.path.append('/home/rose/MStore/src')

import global_settings as glob

__author__ = 'Zephyre'

logging.basicConfig(format='%(asctime)-24s%(levelname)-8s%(message)s', level='INFO')
logger = logging.getLogger()


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
    storage_path = glob.STORAGE_PATH
    original_path = os.getcwd()
    os.chdir(storage_path)

    logger.info(str.format('{0}\tAUTO BACKUP STARTED', datetime.datetime.now().strftime('%Y%m%d_%H%M%S')))

    backup_name = str.format('{0}_spider_editor_release_auto_backup', datetime.datetime.now().strftime('%Y%m%d_%H%M%S'))
    path = os.path.join(storage_path, 'backups')
    make_sure_path_exists(path)
    template = str.format('{0}/{1}', path, backup_name)
    sys_cmd = str.format(
        'mysqldump -c -u rose -prose123 --databases spider_stores editor_stores release_stores > {0}.sql', template)
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


if __name__ == "__main__" and len(sys.argv) >= 2:
    cmd = sys.argv[1]

    if cmd == 'test':
        test()
    elif cmd == 'backup-all':
        backup_all()
    else:
        logger.error(str.format('Unknown command: {0}', cmd))
else:
    logger.error('No command specified.')



