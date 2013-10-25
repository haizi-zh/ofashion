# coding=utf-8
import codecs
import logging
import logging.config
import _mysql
import re
import datetime
import sys
from stores import geosense

__author__ = 'Zephyre'


def get_modified(db, logger):
    """
    读取modified_idstores.csv，获得修改过的品牌
    :param logger:
    """
    filename = u'../modified_idstores.csv'
    with open(filename) as f:
        id_list = tuple(int(re.search(r'\d+', temp).group().strip()) for temp in f.readlines())
        txt = ','.join(str(temp) for temp in id_list)
        db.query(
            str.format('SELECT DISTINCT brand_id, brandname_e FROM spider_stores.stores WHERE idstores IN ({0})', txt))
        results = db.store_result().fetch_row(maxrows=0)
        for r in results:
            logger.info(str.format('{0} {1}', *r))
    pass


def merge_bkp(dbn, dbb, extra_condition=None, logger=None):
    extra_condition = [] if not extra_condition else extra_condition
    logger = logging.getLogger() if not logger else logger

    # Get the union set of all brands
    if len(extra_condition) > 0:
        statement1 = unicode.format(u'{0} WHERE {1}',
                                    u'SELECT DISTINCT brand_id, brandname_e FROM spider_stores.stores',
                                    u' && '.join(unicode.format(u'({0})', temp) for temp in extra_condition))
        statement2 = unicode.format(u'{0} WHERE {1}',
                                    u'SELECT DISTINCT brand_id, brandname_e FROM backup_stores.stores',
                                    u' && '.join(unicode.format(u'({0})', temp) for temp in extra_condition))
    else:
        statement1 = u'SELECT DISTINCT brand_id, brandname_e FROM spider_stores.stores'
        statement2 = u'SELECT DISTINCT brand_id, brandname_e FROM backup_stores.stores'
    dbn.query(statement1.encode('utf-8'))
    brands_n = set((int(temp[0]), temp[1].decode('utf-8')) for temp in dbn.store_result().fetch_row(maxrows=0))
    dbb.query(statement2.encode('utf-8'))
    brands_b = set((int(temp[0]), temp[1].decode('utf-8')) for temp in dbb.store_result().fetch_row(maxrows=0))
    total_brands = brands_n.union(brands_b)

    dbn.query('SET AUTOCOMMIT=0')
    for brand_id, brandname in sorted(total_brands, key=lambda v: v[0]):
        logger.info(unicode.format(u'PROCESSING #{0} {1}', brand_id, brandname.upper()))

        dbn.query(str.format('SELECT idstores FROM spider_stores.stores WHERE brand_id={0}', brand_id))
        id_n = set(int(temp[0]) for temp in dbn.store_result().fetch_row(maxrows=0))

        # Cleanup
        dbn.query(str.format('UPDATE spider_stores.stores SET country_e=NULL,country_c=NULL,province_e=NULL,'
                             'province_c=NULL,city_e=NULL,city_c=NULL,lat=NULL,lng=NULL,update_time="{1}",modified=1 '
                             'WHERE brand_id={0}', brand_id, datetime.datetime.now()))

        dbb.query(str.format('SELECT idstores,country_e,country_c,province_e,province_c,city_e,city_c,lat,lng FROM '
                             'backup_stores.stores WHERE brand_id={0}', brand_id))
        record_set = dbb.store_result()
        for i in xrange(record_set.num_rows()):
            data_map = record_set.fetch_row(how=1)[0]
            idstores = int(data_map['idstores'])
            del data_map['idstores']

            for k in data_map.keys():
                v = data_map[k]
                del data_map[k]
                if not v:
                    data_map[k] = u'NULL'
                elif v.upper() == 'NAN':
                    data_map[k.decode('utf-8')] = '"NAN"'
                else:
                    try:
                        temp = float(v)
                    except ValueError:
                        v = re.sub(r'(?<!\\)"', r'\\"', v).decode('utf-8')
                        temp = unicode.format(u'"{0}"', v)
                    data_map[k.decode('utf-8')] = temp
            data_map[u'update_time'] = unicode.format(u'"{0}"', datetime.datetime.now())
            data_map[u'modified'] = 1
            data_map[u'addr_hash'] = u'NULL'
            data_map[u'geo_shift'] = u'NULL'

            if idstores not in id_n:
                # idstores missing in current database
                logger.warn(unicode.format(u'idstores:{0} not present in current database', idstores))
            else:
                # update current database
                txt = u','.join(unicode.format(u'{0}={1}', key, data_map[key]) for key in data_map)
                statement = unicode.format(u'UPDATE spider_stores.stores SET {0} WHERE idstores={1}', txt, idstores)
                dbn.query(statement.encode('utf-8'))
        dbn.commit()

    logger.info(u'Done')


def gen_big_cities():
    dbs = _mysql.connect(db='spider_stores', user='root', passwd='123456')
    dbc = _mysql.connect(db='world', user='root', passwd='123456')

    dbc.query('SELECT * FROM country')
    results = dbc.store_result().fetch_row(maxrows=0, how=1)
    country_map = dict((temp['Code'], (temp['Code2'], temp['Name'].upper())) for temp in results)

    dbc.query('SELECT * FROM city')
    results = dbc.store_result().fetch_row(maxrows=0, how=1)
    for item in results:
        city, region = (item[key].upper() for key in ('Name', 'District'))
        country_code = item['CountryCode']
        country_code2, country = country_map[country_code]
        print(str.format('PROCESSING {0}:{1}', item['ID'], city))

        ret = geosense.geocode2(addr=','.join((city, region, country)))
        if not ret:
            continue
        lat, lng = (ret[0]['geometry']['location'][key] for key in ('lat', 'lng'))

        value_list = [(str.format('"{0}"', temp) if temp else 'NULL') for temp in (city, region, country,
                                                                                   country_code, country_code2)]
        value_list.extend(str(temp) for temp in (lat, lng))

        dbs.query(str.format('INSERT INTO big_cities (city, region, country, country_code, country_code2, lat, lng)'
                             ' VALUES ({0})', ', '.join(value_list)))

    dbs.close()
    dbc.close()
    pass


if __name__ == "__main__":
    logging.config.fileConfig('sandbox.cfg')
    logger = logging.getLogger('firenzeLogger')

    host, database, user, passwd, port = '127.0.0.1', 'spider_stores', 'root', '', 3306
    for i in xrange(1, len(sys.argv), 2):
        token = sys.argv[i]
        if token[0] != '-':
            logger.error(unicode.format(u'Invalid parameter: {0}', token))
            exit(-1)
        cmd = token[1:]
        value = sys.argv[i + 1]
        if cmd == 'u':
            user = value
        elif cmd == 'P':
            port = int(value)
        elif cmd == 'p':
            passwd = value
        elif cmd == 'd':
            database = value
        elif cmd == 'h':
            host = value

    logger.info(u'sandbox STARTED')
    logger.info(u'================')

    db = _mysql.connect(host=host, port=port, user=user, passwd=passwd, db=database)
    db.query("SET NAMES 'utf8'")

    city_name_dict = {u'北京': u'BEIJING', u'上海': u'SHANGHAI', u'广州': u'GUANGZHOU', u'深圳': u'SHENZHEN'}

    # with open(u'../北上广深-重点店铺补全信息-0923xlsx.csv', mode='r') as f:
    with codecs.open(u'../北上广深-重点店铺补全信息-0923xlsx.csv', 'r', 'utf-8') as f:
        line_no = 0
        for line in f.readlines():
            line_no += 1
            entry = dict()
            entry['city_c'], entry['brand_id'], entry['brandname_e'], entry['brandname_c'], entry['name_c'], entry[
                'addr_c'], entry['tel'], entry['lat'], entry['lng'] = (temp.strip() for temp in line.split(','))
            time_stamp = unicode(datetime.datetime.now())
            entry['update_time'] = time_stamp
            entry['fetch_time'] = time_stamp
            entry['addr_e'] = entry['addr_c']
            entry['city_e'] = city_name_dict[entry['city_c']]
            entry['country_e'] = 'CHINA'

            key_list = list(entry.keys())
            col_str = u', '.join(key_list)
            val_str = u','.join((unicode.format(u'"{0}"', entry[key]) if entry[key] else u'NULL') for key in key_list)
            statement = unicode.format(u'INSERT INTO stores ({0}) VALUES ({1})', col_str, val_str)
            logger.info(statement)
            cm.insert_record(db, entry, 'stores')

    db.close()