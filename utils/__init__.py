# coding=utf-8
import copy
import logging
import logging.handlers
import os
import datetime
import socket
import global_settings as glob

__author__ = 'Zephyre'


def init_rsyslogger(logger_name='rsyslog'):
    class RoseVisionAdapter(logging.LoggerAdapter):
        def process(self, msg, kwargs):
            extra = copy.copy(self.extra)
            for k, v in kwargs.items():
                extra[k] = v

            return msg, {'extra': extra}

    d = {'clientip': '192.168.0.1', 'user': 'fbloggs'}

    my_logger = logging.getLogger('MyLogger')
    my_logger.setLevel(logging.INFO)
    sh = logging.handlers.SysLogHandler(address=('127.0.0.1', 2515), socktype=socket.SOCK_STREAM,
                                        facility=logging.handlers.SysLogHandler.LOG_LOCAL1)
    sh1 = logging.handlers.SysLogHandler(address=('127.0.0.1', 1514), socktype=socket.SOCK_DGRAM,
                                         facility=logging.handlers.SysLogHandler.LOG_LOCAL1)

    ch = logging.StreamHandler()
    # formatter = logging.Formatter('%(message)s')
    formatter = logging.Formatter('%(clientip)s %(user)s %(message)s')

    ch.setFormatter(formatter)
    sh.setFormatter(formatter)
    sh1.setFormatter(formatter)
    # my_logger.addHandler(ch)
    my_logger.addHandler(sh)
    # my_logger.addHandler(sh1)

    my_adapter = RoseVisionAdapter(my_logger, extra={'clientip': 'zephyre-office', 'user': 'haizi'})


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
        filename = os.path.join(getattr(glob, 'STORAGE')['STORAGE_PATH'], 'log',
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
