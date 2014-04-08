# coding=utf-8
import global_settings
from utils.db import RoseVisionDb

__author__ = 'Zephyre'

"""
该模块的作用：采用中心数据库模型，生成全局唯一ID（跨越多个物理/逻辑的数据库）。
参考文献：
http://code.flickr.net/2010/02/08/ticket-servers-distributed-unique-primary-keys-on-the-cheap/
http://blog.csdn.net/bluishglc/article/details/7710738
"""


def static_var(varname, value):
    def decorate(func):
        setattr(func, varname, value)
        return func

    return decorate


@static_var('ticket_idx', 0)
def get_ticket():
    # 所有的ticket server列表
    ticket_servers = getattr(global_settings, 'TICKETS_SERVER')

    for offset in xrange(len(ticket_servers)):
        idx = (getattr(get_ticket, 'ticket_idx') + offset) % len(ticket_servers)
        server = ticket_servers.values()[idx]
        with RoseVisionDb(spec=server) as db:
            try:
                db.query('REPLACE INTO tickets64 (stub) VALUES ("a")')
                last_id = int(db.query('SELECT LAST_INSERT_ID()').fetch_row()[0][0])
                return last_id
            except:
                continue
            finally:
                setattr(get_ticket, 'ticket_idx', idx)

    raise Exception()