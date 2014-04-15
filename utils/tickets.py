# coding=utf-8

"""
该模块的作用：采用中心数据库模型，生成全局唯一ID（跨越多个物理/逻辑的数据库）。
参考文献：
http://code.flickr.net/2010/02/08/ticket-servers-distributed-unique-primary-keys-on-the-cheap/
http://blog.csdn.net/bluishglc/article/details/7710738
"""

import global_settings
from utils.db import RoseVisionDb
import _mysql_exceptions

__author__ = 'Zephyre'


def static_var(varname, value):
    def decorate(func):
        setattr(func, varname, value)
        return func

    return decorate


class TicketsError(Exception):
    def __init__(self, message, cause=None):
        if cause:
            message = message + u' Caused by: ' + repr(cause)
        super(TicketsError, self).__init__(message)
        self.cause = cause


@static_var('ticket_idx', 0)
def get_ticket():
    # 所有的ticket server列表
    """
    生成全局唯一ID
    @rtype :
    @raise : TicketsError
    """
    ticket_servers = getattr(global_settings, 'TICKETS_SERVER')
    if not ticket_servers:
        raise TicketsError('No ticket server can be found in the system.')

    last_error = None
    for offset in xrange(len(ticket_servers)):
        idx = (getattr(get_ticket, 'ticket_idx') + offset) % len(ticket_servers)
        server = ticket_servers.values()[idx]
        with RoseVisionDb(spec=server) as db:
            try:
                db.query('REPLACE INTO tickets64 (stub) VALUES ("a")')
                return int(db.query('SELECT LAST_INSERT_ID()').fetch_row()[0][0])
            except _mysql_exceptions as e:
                last_error = TicketsError('Database server error.', cause=e)
            finally:
                setattr(get_ticket, 'ticket_idx', idx)

    raise last_error