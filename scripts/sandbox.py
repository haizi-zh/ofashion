import json

__author__ = 'Zephyre'

try:
    with open(r'../test_old.csv', 'r') as f:
        old_data = dict([val.strip() for val in l.split(',')] for l in f.readlines())
except IOError:
    old_data = {}

with open(r'../../products/data/10226_louis_vuitton/10226_louis_vuitton_cn_tags_mapping.json', 'r') as f:
    data = json.load(f)

with open(r'../test.csv', 'w') as f:
    for k in data:
        if k in old_data:
            continue
        v = data[k]
        f.write(unicode.format(u'{0},\t{1}\n', k, v).encode('utf-8'))