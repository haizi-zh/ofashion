# coding=utf-8

import _mysql
import json
import re
import sys
import pydevd
import common as cm
import global_settings as glob

__author__ = 'Zephyre'

if glob.DEBUG_FLAG:
    pydevd.settrace('localhost', port=glob.DEBUG_PORT, stdoutToServer=True, stderrToServer=True)


def blank_splitter(body):
    it = re.finditer(ur'\s+', body)
    idx = 0
    while True:
        try:
            m = it.next()
            ret = body[idx:m.start()], m.group()
            idx = m.end()
            yield ret
        except StopIteration:
            yield body[idx:], None
            break
    raise StopIteration


def process_text(body):
    return body


if len(sys.argv) < 2:
    print 'Invalid syntax'
    sys.exit()

brand_id = sys.argv[1]

db_spec_editor = {'host': '127.0.0.1', 'username': 'rose', 'password': 'rose123', 'port': 3306,
                  'schema': 'editor_stores'}
db_spec_release = {'host': '127.0.0.1', 'username': 'rose', 'password': 'rose123', 'port': 3306,
                   'schema': 'release_stores'}
extra_cond = str.format('brand_id={0}', brand_id)

region_list = ['cn', 'us', 'fr', 'uk', 'hk', 'jp', 'it', 'au', 'ae', 'sg', 'de', 'ca', 'es', 'ch', 'ru', 'br', 'kr',
               'my', 'nl', 'tw', 'at', 'mo']
country_pref = {}
for i in xrange(len(region_list)):
    country_pref[region_list[i]] = 10 * i

# country_pref = {'cn': 10, 'us': 20, 'fr': 30, 'it': 40, 'de': 50}
# currency_map = {'cn': {'country': u'中国', 'currency': u'¥'}, 'us': {'country': u'美国', 'currency': '$'},
#                 'fr': {'country': u'法国', 'currency': u'€'}, 'uk': {'country': u'英国'},
#                 'it': {'country': u'意大利', 'currency': u'€'},
#                 'de': {'country': u'德国', 'currency': u'€'}, 'jp': {'country': u'日本', 'currency': u'¥'}}

country_map = {'cn': u'中国', 'us': u'美国', 'fr': u'法国', 'uk': u'英国', 'hk': u'香港', 'jp': u'日本', 'it': u'意大利',
               'au': u'澳大利亚', 'ae': u'阿联酋', 'sg': u'新加坡', 'de': u'德国', 'ca': u'加拿大', 'es': u'西班牙', 'ch': u'瑞士',
               'ru': u'俄罗斯', 'br': u'巴西', 'kr': u'韩国', 'my': u'马来西亚', 'nl': u'荷兰', 'tw': u'台湾', 'at': u'奥地利',
               'mo': u'澳门'}

db_editor = _mysql.connect(host=db_spec_editor['host'], port=db_spec_editor['port'], user=db_spec_editor['username'],
                           passwd=db_spec_editor['password'], db=db_spec_editor['schema'])
db_editor.query("SET NAMES 'utf8'")
db_editor.query(str.format('SELECT DISTINCT model FROM products WHERE {0}', extra_cond))
model_list = tuple(val[0] for val in db_editor.store_result().fetch_row(maxrows=0))

db_editor.query('DROP TEMPORARY TABLE IF EXISTS tmp_tbl')
db_editor.query('DROP TEMPORARY TABLE IF EXISTS tmp_tbl_img')
db_editor.query(str.format('CREATE TEMPORARY TABLE tmp_tbl (SELECT * FROM products WHERE {0})', extra_cond))
db_editor.query(str.format('CREATE TEMPORARY TABLE tmp_tbl_img (SELECT * FROM products_image WHERE {0})', extra_cond))

db = _mysql.connect(host=db_spec_release['host'], port=db_spec_release['port'], user=db_spec_release['username'],
                    passwd=db_spec_release['password'], db=db_spec_release['schema'])
db.query("SET NAMES 'utf8'")

db.query('LOCK TABLES products WRITE')
db.query('START TRANSACTION')

try:

    cnt = 0
    for model in model_list:
        db_editor.query(str.format('SELECT * FROM tmp_tbl WHERE model="{0}" AND update_flag!="X"', model))
        data = db_editor.store_result().fetch_row(maxrows=0, how=1)
        results = sorted(data, key=lambda x: country_pref[x['region']] if x['region'] in country_pref else sys.maxint)

        entry = {'model': model, 'name': results[0]['name'], 'brand_id': results[0]['brand_id'],
                 'brandname_e': results[0]['brandname_e'],
                 'brandname_c': results[0]['brandname_c'], 'url': results[0]['url'],
                 'description': results[0]['description'],
                 'details': results[0]['details'],
                 'category': results[0]['category'], 'color': results[0]['color'], 'tags': results[0]['tags'],
                 'texture': results[0]['texture']}

        if entry['details']:
            temp = list(blank_splitter(entry['details']))
            entry['details'] = ''.join(
                str.format('{0}{1}', val[0], '\n' if val[1] and ('\n' in val[1]) else ' ') for val in temp)
        if entry['description']:
            temp = list(blank_splitter(entry['description']))
            entry['description'] = ''.join(
                str.format('{0}{1}', val[0], '\n' if val[1] and ('\n' in val[1]) else ' ') for val in temp)

        # gender
        gender_set = set([])
        for val in results:
            if not val['gender']:
                continue
            for c in val['gender'].split('|'):
                gender_set.add(c)
        if len(gender_set) >= 2 or len(gender_set) == 0:
            entry['gender'] = None
        else:
            entry['gender'] = list(gender_set)[0]

        # 区域列表
        entry['region_list'] = json.dumps([val['region'] for val in results], ensure_ascii=False)

        # 获得价格列表
        price_list = [{'price': float(val['price_rev']) if val['price_rev'] else None, 'currency': val['currency_rev'],
                       'country': country_map[val['region']], 'code': val['region']} for val in results]

        entry['price_list'] = json.dumps(price_list, ensure_ascii=False)
        ret = filter(lambda val: val['currency'] == 'CNY', price_list)
        entry['price_cn'] = ret[0]['price'] if len(ret) > 0 else None

        # 获得图像和价格列表
        db_editor.query(
            str.format('SELECT path,width,height FROM tmp_tbl_img WHERE model="{0}"', entry['model']))
        results = db_editor.store_result().fetch_row(maxrows=0, how=1)
        image_list = []
        temp = set([])
        for val in results:
            if val['path'] in temp:
                continue
            else:
                val['width'] = int(val['width'])
                val['height'] = int(val['height'])
                image_list.append(val)
                temp.add(val['path'])
        entry['image_list'] = json.dumps(image_list, ensure_ascii=False)
        entry['cover_image'] = json.dumps(image_list[0]) if len(image_list) > 0 else None

        # db_editor.query(
        #     str.format('SELECT price_rev,currency_rev,region,gender FROM products WHERE model="{0}"',
        #                entry['model']))
        # results = db_editor.store_result().fetch_row(maxrows=0, how=1)


        # # color
        # color_set = set([])
        # for val in results:
        #     if not val['color_rev']:
        #         continue
        #     for c in json.loads(val['color_rev']):
        #         color_set.add(c)
        # entry['color'] = json.dumps(list(color_set), ensure_ascii=False)
        #
        # # texture
        # texture_set = set([])
        # for val in results:
        #     if not val['texture_rev']:
        #         continue
        #     for c in json.loads(val['texture_rev']):
        #         texture_set.add(c)
        # entry['texture'] = json.dumps(list(texture_set), ensure_ascii=False)



        # entry['brand_id'] = 10226
        cm.insert_record(db, entry, 'products')
        cnt += 1
        print str.format('Processed {0}, {1:.1%}', cnt, float(cnt) / len(model_list))

        if cnt % 50 == 0:
            db.query('COMMIT')
            db.query('START TRANSACTION')

    db.query('COMMIT')
except:
    db.query('ROLLBACK')
    raise
finally:
    db.query('UNLOCK TABLES')
    db.close()