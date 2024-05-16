#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""mr3_join_reducer.py"""

import sys

PRICE_TABLE_NAME = 'price'
PRODUCT_TABLE_NAME = 'product'

# Текущий обрабатываемый id
current_id = 0

# Продукты с данным id
current_description = []

# Цены с текущим id
current_prices = []

for line in sys.stdin:
    product_id, name, table_name = line.split('\t')
    product_id = int(product_id)

    if table_name[-1] == '\n':
        table_name = table_name[:-1]
    
    # В случае нового id обновляем массивы
    if current_id != product_id:
        current_id = product_id
        current_description = []
        current_prices = []
        
        if table_name == PRODUCT_TABLE_NAME:
            current_description.append(name)

        if table_name == PRICE_TABLE_NAME:
            current_prices.append(name)
                            
        continue
    
    # Строка из таблицы product
    if table_name == PRODUCT_TABLE_NAME:
        for price in current_prices:
            # print(f'{current_id}\t{name}\t{price}')
            print '{key}\t{value_product}\t{value_price}'.format(key=current_id, value_product=name, value_price=price)

        current_description.append(name)

    # Строка из таблицы price
    if table_name == PRICE_TABLE_NAME:
        for description in current_description:
            # print(f'{product_id}\t{description}\t{name}')
            print '{key}\t{value_product}\t{value_price}'.format(key=current_id, value_product=description, value_price=name)

        current_prices.append(name)
