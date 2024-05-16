#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""mr3_price_mapper.py"""

import sys


TABLE_NAME = 'price'

for line in sys.stdin:
    # Деление по знаку ';'
    product_id, cost = line.split(';', 1)

    # Проверяем на возможность неправильной записи 
    # числа с дробной частью
    if ',' in cost:
        cost = cost.replace(',', '.')

    if cost[-1] == '\n':
        cost = cost[:-1]


    # print(f'{product_id}\t{cost}\t{TABLE_NAME}')
    print '{key}\t{value}\t{table}'.format(key=product_id, value=cost, table=TABLE_NAME)