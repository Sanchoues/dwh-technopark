#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""mr3_product_mapper.py"""

import sys


TABLE_NAME = 'product'

for line in sys.stdin:
    # Делит таблицу продукты по tab
    product_id, name = line.split('\t', 1)

    if name[-1] == '\n':
        name = name[:-1]

    # print(f'{product_id}\t{name}\t{TABLE_NAME}')
    print '{key}\t{value}\t{table}'.format(key=product_id, value=name, table=TABLE_NAME)