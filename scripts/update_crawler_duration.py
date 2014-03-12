from core import MySqlDb
import global_settings as gs

__author__ = 'Zephyre'

def foo():
    brand = 10006
    with MySqlDb(getattr(gs, 'DB_SPEC')) as db:
        db.update({'start_time':'', 'end_time':'', 'duration':''}, 'crawler_duration', str.format('brand_id={0}', brand))

        pass

    