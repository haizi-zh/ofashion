# coding=utf-8
import json
import os
import re
import socket
from urllib2 import HTTPError, URLError
from exceptions import ValueError
import common as cm
import global_settings as glob

__author__ = 'Zephyre'

# base_path = '../products'
base_path = os.path.join(glob.STORAGE_PATH, 'products')
tags_mapping = {}


def get_image_path(brand_id):
    brand_name = cm.norm_brand_name(cm.fetch_brand_by_id(brand_id)['brandname_e'])
    return {
        'full': os.path.join(base_path, 'images', str.format('{0}_{1}', brand_id, brand_name), 'full'),
        'thumb': os.path.join(base_path, 'images', str.format('{0}_{1}', brand_id, brand_name), 'thumb')
    }


def get_data_path(brand_id):
    brand_name = cm.norm_brand_name(cm.fetch_brand_by_id(brand_id)['brandname_e'])
    return os.path.normpath(os.path.join(base_path, 'data', str.format('{0}_{1}', brand_id, brand_name)))


def update_tags_mapping(brand_id, region, tag_raw, tag_name, serialize=True):
    """
    更新tags_mapping映射机制。根据区域不同，在标签的源代码表象和标签的展示表象之间，建立映射关系。
    :param brand_id:
    :param region:
    :param tag_raw:
    :param tag_name:
    :param serialize: 是否更新数据文件
    """
    brand_name = cm.norm_brand_name(cm.fetch_brand_by_id(brand_id)['brandname_e'])
    data_dir = get_data_path(brand_id)
    region = region.lower()
    fname = os.path.normpath(
        os.path.join(data_dir, str.format('{0}_{1}_{2}_tags_mapping.json', brand_id, brand_name, region)))

    if brand_id not in tags_mapping:
        try:
            with open(fname, 'r') as f:
                data = json.load(f, encoding='utf-8')
        except ValueError:
            data = {}
        except IOError:
            data = {}
        tags_mapping[brand_id] = data
    else:
        data = tags_mapping[brand_id]

    tag_raw = tag_raw.encode('utf-8') if isinstance(tag_raw, unicode) else tag_raw
    tag_name = tag_name.encode('utf-8') if isinstance(tag_name, unicode) else tag_name
    data[tag_raw] = tag_name
    if serialize:
        cm.make_sure_path_exists(data_dir)
        with open(fname, 'w') as f:
            json.dump(data, f, ensure_ascii=False, encoding='utf-8')


def fetch_image(url, logger=None):
    def func(url):
        try:
            return cm.get_data(url, binary_data=True)
        except HTTPError as e:
            if e.code != 404:
                raise

    response = cm.retry_helper(func, param=url, logger=logger, except_class=(HTTPError, URLError, socket.timeout),
                               retry_delay=10,
                               retry_message=unicode.format(u'Failed to fetch URL: {0}', url),
                               abort_message=unicode.format(u'Abort to fetch URL: {0}', url))
    if response is not None and len(response['body']) > 0:
        ctype = response['headers']['content-type']
        if ctype.lower() == 'image/tiff':
            ext = 'tif'
        elif ctype.lower() == 'image/png':
            ext = 'png'
        elif ctype.lower() == 'image/gif':
            ext = 'gif'
        else:
            ext = 'jpg'
        response['image_ext'] = ext
    return response
