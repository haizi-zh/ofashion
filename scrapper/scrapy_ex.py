# coding=utf-8
from scrapy.http import Request
import global_settings as gs
from utils import info

__author__ = 'Zephyre'


class ProxiedRequest(Request):
    @staticmethod
    def __fetch_proxy(region, idx=0):
        try:
            proxy_dict = getattr(gs, 'PROXY')
        except AttributeError:
            proxy_dict = {}

        try:
            if region in proxy_dict:
                return proxy_dict[region][idx]

            # 没有相应国家的代理节点
            if info.region_info()[region]['currency'] == 'EUR':
                # 欧元区，使用法国等代理替代
                for alt_region in ('fr', 'it', 'nl'):
                    if alt_region in proxy_dict:
                        return proxy_dict[alt_region][idx]

            return None
        except IndexError:
            return None

    def __init__(self, url, proxy_region=None, proxy_enabled=False, callback=None, method='GET', headers=None,
                 body=None, cookies=None, meta=None, encoding='utf-8', priority=0, dont_filter=False, errback=None):
        if not meta:
            meta = {}

        if proxy_region and proxy_enabled:
            # 启用代理
            proxy = self.__fetch_proxy(proxy_region)
            if proxy:
                meta['proxy'] = 'http://' + proxy

        super(ProxiedRequest, self).__init__(url=url, callback=callback, method=method, headers=headers, body=body,
                                             cookies=cookies, meta=meta, encoding=encoding, priority=priority,
                                             dont_filter=dont_filter, errback=errback)
