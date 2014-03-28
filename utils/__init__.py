# coding=utf-8
import logging
import os
import datetime
import global_settings as glob

__author__ = 'Zephyre'


def init_logger(logger_name='rosevision', filename=None, to_file=False,
               log_format='%(asctime)-24s%(levelname)-8s%(message)s', level=logging.INFO):
    """
    返回日志处理器
    @param logger_name:
    @param to_file: 是否要写到文件中
    @param filename:
    @param log_format:
    @param level:
    @return:
    """
    if to_file and not filename:
        filename = os.path.join(getattr(glob, 'STORAGE_PATH'), 'log',
                                unicode.format(u'{0}_{1}.log', logger_name,
                                               datetime.datetime.now().strftime('%Y%m%d')))
    fh = logging.FileHandler(filename, encoding='utf-8') if filename else None
    fh.setFormatter(logging.Formatter(fmt=log_format))
    fh.setLevel(logging.INFO)
    logger = logging.getLogger(logger_name)
    logger.setLevel(logging.INFO)
    if fh:
        root_logger = logging.getLogger()
        root_logger.handlers = []
        logger.addHandler(fh)
    return logger

init_logger(logger_name='rosevision', to_file=True)
init_logger(logger_name='monitor', to_file=True)
