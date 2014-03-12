# coding=utf-8
import datetime
from core import RoseVisionDb
import global_settings as gs
from utils.utils_core import unicodify

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

        # if price1 == price2 and discount1 == discount2:
        #     err_no = -1
        if (not price1 and discount1) or (not price2 and discount2):
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

    # 主要国家列表。只监控这些国家的单品的价格变化过程。
    main_countries = [tmp[0] for tmp in filter(lambda val: val[1]['weight'] < 999999, gs.region_info().items())]
    with RoseVisionDb(getattr(gs, 'DB_SPEC')) as db:
        if not brand_list:
            rs = db.query_match(['brand_id'], 'products', distinct=True)
            brand_list = [int(val[0]) for val in rs.fetch_row(maxrows=0)]

        # 获得默认的时间区间
        if start:
            try:
                start = datetime.datetime.strptime(start, '%Y-%m-%d %H:%M:%S')
            except ValueError:
                start = datetime.datetime.strptime(start, '%Y-%m-%d')
        else:
            start = (datetime.datetime.now() - datetime.timedelta(1)).date()

        if end:
            try:
                end = datetime.datetime.strptime(end, '%Y-%m-%d %H:%M:%S')
            except ValueError:
                end = datetime.datetime.strptime(end, '%Y-%m-%d')
        else:
            end = datetime.datetime.now().date()

        results = {'warnings': [], 'price_up': {}, 'discount_up': {}, 'price_down': {}, 'discount_down': {}}
        for brand in brand_list:
            pid_list = db.query(str.format('''SELECT p1.model,p1.idproducts,p1.region,p1.fingerprint FROM products AS p1
            JOIN products_price_history AS p2 ON p1.idproducts=p2.idproducts
            WHERE p1.offline=0 AND p2.price IS NOT NULL AND brand_id={0} AND p1.region IN ({1}) AND p2.date BETWEEN {2} AND {3}''',
                                           brand, ','.join(str.format('"{0}"', tmp) for tmp in main_countries),
                                           *map(lambda val: val.strftime('"%Y-%m-%d %H:%M:%S"'),
                                                (start, end)))).fetch_row(maxrows=0)
            if not pid_list:
                continue

            tmp = db.query(str.format('''
            SELECT p1.idproducts,p1.model,p1.region,p1.fingerprint,p2.price,p2.price_discount,p2.currency,p2.date FROM products AS p1
            JOIN products_price_history AS p2 ON p1.idproducts=p2.idproducts
            WHERE p1.idproducts IN ({0}) ORDER BY p2.date DESC''', ','.join(tmp[1] for tmp in pid_list))).fetch_row(
                maxrows=0)

            rs = {}
            # 按照pid归并，即rs[pid] = [该pid所对应的价格历史]
            # 开始的时候，pid_set保留了所有需要处理的pid。归并的原则是，每个pid，取最近的最多两条有效记录。如果两条记录取满，
            # 该pid从pid_set中移除。今后，就算再次遇到这个pid，也不作处理了。
            pid_set = set([val[0] for val in tmp])
            for pid, model, region, fp, price, discount, currency, date in tmp:
                # 如果pid不在pid_set中，说明该pid对应的两条记录都已经取到。
                # 如果price为None，说明该记录不包含有效价格数据，跳过不处理。
                if pid not in pid_set or not price:
                    continue
                if int(pid) in rs and len(rs[int(pid)]) >= 2:
                    # 最近两条数据已满，跳过该pid
                    pid_set.remove(pid)
                    continue

                pid = int(pid)
                if pid not in rs:
                    rs[pid] = []
                rs[pid].append([model, region, fp, price, discount, currency, date])

            for pid, price_history in rs.items():
                if len(price_history) < 2:
                    continue

                def func(idx):
                    rate = gs.currency_info()[price_history[idx][-2]]
                    return (float(price_history[idx][-4]) * rate if price_history[idx][-4] else None,
                            float(price_history[idx][-3]) * rate if price_history[idx][-3] else None)

                price1, discount1 = func(0)
                price2, discount2 = func(1)

                # 是否可能有错误？
                ret = price_check((price2, discount2), (price1, discount1))
                if ret != 0:
                    results['warnings'].append({'idproducts': pid, 'model': price_history[0][0], 'msg': ret[1]})
                    continue

                if price1 and price2 and price1 < price2:
                    key = 'price_down'
                elif price1 and price2 and price1 > price2:
                    key = 'price_up'
                elif discount1 and discount2 and discount1 < discount2:
                    key = 'discount_down'
                elif not discount2 and discount1:
                    key = 'discount_down'
                elif discount1 and discount2 and discount1 > discount2:
                    key = 'discount_up'
                elif not discount1 and discount2:
                    key = 'discount_up'
                else:
                    key = None

                if key:
                    if brand not in results[key]:
                        results[key][brand] = {}

                    fp = price_history[0][2]
                    if fp not in results[key][brand]:
                        results[key][brand][fp] = {'model': price_history[0][0], 'brand_id': brand, 'fingerprint': fp,
                                                   'products': []}

                    # 获得单品的优先名称
                    region = price_history[0][1]
                    price_new = float(price_history[0][3]) if price_history[0][3] else None
                    price_old = float(price_history[1][3]) if price_history[1][3] else None
                    discount_new = float(price_history[0][4]) if price_history[0][4] else None
                    discount_old = float(price_history[1][4]) if price_history[1][4] else None
                    currency_new = price_history[0][5]
                    currency_old = price_history[1][5]
                    results[key][brand][fp]['products'].append(
                        {'idproducts': int(pid), 'region': region,
                         'old_price': {'price': price_old, 'price_discount': discount_old,
                                       'currency': currency_old},
                         'new_price': {'price': price_new, 'price_discount': discount_new,
                                       'currency': currency_new}
                        })

            # results中的记录，还需要单品名称信息。首先获得result中的所有fingerprint，并从数据库中查找对应的名称
            fp_list = []
            for change_type in ['price_up', 'price_down', 'discount_up', 'discount_down']:
                if brand in results[change_type]:
                    fp_list.extend(results[change_type][brand].keys())
            fp_list = list(set(fp_list))

            # 获得fingerprint和name的关系
            fp_name_map = {}
            if fp_list:
                for fp, name, region in db.query_match(['fingerprint', 'name', 'region'], 'products',
                                                       extra=str.format('fingerprint IN ({0})', ','.join(
                                                               str.format('"{0}"', tmp) for tmp in fp_list))).fetch_row(
                        maxrows=0):
                    if fp not in fp_name_map:
                        fp_name_map[fp] = {'name': unicodify(name), 'region': region}
                    elif gs.region_info()[region]['weight'] < gs.region_info()[fp_name_map[fp]['region']]['weight']:
                        # 更高优先级的国家，替换：
                        fp_name_map[fp] = {'name': unicodify(name), 'region': region}

                for change_type in ['price_up', 'price_down', 'discount_up', 'discount_down']:
                    if brand not in results[change_type]:
                        continue
                    for fp in results[change_type][brand]:
                        results[change_type][brand][fp]['name'] = fp_name_map[fp]['name']

        return results


def newly_fetched(brand_list=None, start=None, end=None):
    """
    获得start到end时间区间内新增加的单品记录。如果start和end中有任何一个为None，则默认采用过去一天的时间区间。
    假设2014/02/25 02:00调用该函数，则默认查找2014/02/24 00:00:00至2014/02/25 00:00:00之间新添加的数据。
    @param brand_list: 查找的品牌。如果为None，则默认对数据库中的所有品牌进行处理
    @param start: datetime.date或datetime.datetime对象
    @param end:
    """
    with RoseVisionDb(getattr(gs, 'DB_SPEC')) as db:
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

                    # 按照fingerprint进行归并，并按照国家的权重进行排序
                    tmp = sorted(filter(lambda val: val['fingerprint'] == fp, records),
                                 key=lambda val: gs.region_info()[val['region']]['weight'])
                    results[brand].append(
                        {'model': tmp[0]['model'], 'fingerprint': fp, 'name': unicodify(tmp[0]['name'])})
                    processed.add(fp)
        return results


if __name__ == '__main__':
    rec = price_changed(brand_list=[10006], start='2013-03-01 22:00:00', end='2013-03-03')
    print rec
    pass