# coding=utf-8
import datetime
from core import MySqlDb
import global_settings as gs

__author__ = 'Zephyre'


def price_changed(brand_list=None, start=None, end=None):
    """
    获得start到end时间区间内价格发生变化的单品记录。如果start和end中有任何一个为None，则默认采用过去一天的时间区间。
    假设2014/02/25 02:00调用该函数，则默认查找2014/02/24 00:00:00至2014/02/25 00:00:00之间新添加的数据。
    @param brand_list: 查找的品牌。如果为None，则默认对数据库中的所有品牌进行处理
    @param start: datetime.date或datetime.datetime对象
    @param end:
    """

    def price_check(old, new):
        """
        对两组价格进行有效性检查。该函数的主要目的是：通过检查，查找可能存在的代码bug
        检查策略：
        1. 如果两条记录一样
        2. 如果price为None，而price_discount不为None
        3. 如果price<=price_discount
        4. 如果old和new两项price的差别过大
        则可能存在bug或错误，需要返回warning。
        @param old:
        @param new:
        """
        warnings = {-1: 'EQUAL RECORDS',
                    -2: 'NULL PRICE',
                    -3: 'PRICE IS EQUAL OR LESS THAN PRICE_DISCOUNT',
                    -4: 'TOO MUCH GAP BETWEEN THE OLD AND THE NEW'}
        price1, discount1 = old
        price2, discount2 = new
        # 如果价格变化超过threshold，则认为old和new差异过大
        threshold = 5

        if price1 == price2 and discount1 == discount2:
            err_no = -1
        elif (not price1 and discount1) or (not price2 and discount2):
            err_no = -2
        elif (price1 and discount1 and price1 <= discount1) or (price2 and discount2 and price2 <= discount2):
            err_no = -3
        elif price1 > 0 and price2 > 0 and (price1 / price2 > threshold or price2 / price1 > threshold):
            err_no = -4
        else:
            err_no = 0

        if err_no != 0:
            return (err_no, warnings[err_no])
        else:
            return err_no

    db = MySqlDb()
    db.conn(gs.DB_SPEC)

    # if not brand_list:
    #     rs = db.query_match(['brand_id'], 'products', distinct=True)
    #     brand_list = [int(val[0]) for val in rs.fetch_row(maxrows=0)]

    if not (start and end):
        # 获得默认的时间区间
        start = (datetime.datetime.now() - datetime.timedelta(1)).date()
        end = datetime.datetime.now().date()

    results = {'warnings': [], 'price_up': {}, 'discount_up': {}, 'price_down': {}, 'discount_down': {}}
    processed = set({})

    brand_cond = str.format('p1.brand_id IN ({0})', ','.join(str(tmp) for tmp in brand_list)) if brand_list else 1
    rs_pid = db.query(str.format('''SELECT p1.brand_id,p1.model,p1.idproducts,p1.region,p1.fingerprint FROM products AS p1
    JOIN products_price_history AS p2 ON p1.idproducts=p2.idproducts
    WHERE p1.offline=0 AND p2.price IS NOT NULL AND {0} AND p2.date BETWEEN {1} AND {2}''',
                                 brand_cond,
                                 *map(lambda val: val.strftime('"%Y-%m-%d %H:%M:%S"'), (start, end)))).fetch_row(
        maxrows=0)
    for brand, model, pid, region, fingerprint in rs_pid:
        pid = int(pid)
        brand = int(brand)
        # 在某个特定的时间区间内，可能存在同一个pid的多条记录。每个pid我们只需要处理一次即可。
        if pid in processed:
            continue
        else:
            processed.add(pid)
        # 查找该pid的价格记录，并排序
        rs = sorted(
            db.query_match(['*'], 'products_price_history', {'idproducts': pid}).fetch_row(maxrows=0, how=1),
            key=lambda val: datetime.datetime.strptime(val['date'], '%Y-%m-%d %H:%M:%S'), reverse=True)
        # 查找最近两次的价格并比较
        if len(rs) < 2:
            continue

        def func(idx):
            rate = gs.currency_info()[rs[idx]['currency']]
            return (float(rs[idx]['price']) * rate if rs[idx]['price'] else None,
                    float(rs[idx]['price_discount']) * rate if rs[idx]['price_discount'] else None)

        price1, discount1 = func(0)
        price2, discount2 = func(1)

        # 是否可能有错误？
        ret = price_check((price2, discount2), (price1, discount1))
        if ret != 0:
            results['warnings'].append({'idproducts': pid, 'model': model, 'msg': ret[1]})
            continue

        if price1 and price2 and price1 < price2:
            key = 'price_down'
        elif price1 and price2 and price1 > price2:
            key = 'price_up'
        elif discount1 and discount2 and discount1 < discount2:
            key = 'discount_down'
        elif discount1 and discount2 and discount1 > discount2:
            key = 'discount_up'
        else:
            key = None

        if key:
            if fingerprint not in results[key]:
                results[key][fingerprint] = []
            results[key][fingerprint].append({'idproducts': pid, 'model': model, 'region': region, 'brand_id': brand,
                                              'fingerprint': fingerprint,
                                              'old_price': {'price': rs[1]['price'],
                                                            'price_discount': rs[1]['price_discount'],
                                                            'currency': rs[1]['currency']},
                                              'new_price': {'price': rs[0]['price'],
                                                            'price_discount': rs[0]['price_discount'],
                                                            'currency': rs[0]['currency']}
            })

    db.close()
    return results


def newly_fetched(brand_list=None, start=None, end=None):
    """
    获得start到end时间区间内新增加的单品记录。如果start和end中有任何一个为None，则默认采用过去一天的时间区间。
    假设2014/02/25 02:00调用该函数，则默认查找2014/02/24 00:00:00至2014/02/25 00:00:00之间新添加的数据。
    @param brand_list: 查找的品牌。如果为None，则默认对数据库中的所有品牌进行处理
    @param start: datetime.date或datetime.datetime对象
    @param end:
    """
    db = MySqlDb()
    db.conn(gs.DB_SPEC)

    if not brand_list:
        rs = db.query_match(['brand_id'], 'products', distinct=True)
        brand_list = [int(val[0]) for val in rs.fetch_row(maxrows=0)]

    if not (start and end):
        # 获得默认的时间区间
        start = (datetime.datetime.now() - datetime.timedelta(1)).date()
        end = datetime.datetime.now().date()

    results = {}
    processed = set({})
    for brand in brand_list:
        records = db.query_match(['*'], 'products', {'brand_id': brand},
                                 [str.format('fetch_time BETWEEN {0} AND {1}',
                                             *map(lambda val: val.strftime('"%Y-%m-%d %H:%M:%S"'),
                                                  (start, end))), 'offline=0']).fetch_row(maxrows=0, how=1)
        for r in records:
            fp = r['fingerprint']
            if fp not in processed:
                if brand not in results:
                    results[brand] = []
                results[brand].append({'model': r['model'], 'fingerprint': fp})
                processed.add(fp)
    db.close()
    return results