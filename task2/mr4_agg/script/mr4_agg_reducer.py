#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""mr1_wc_reducer.py"""

import sys

previous_key = None
previous_value = 0

for line in sys.stdin:

    prepared_line = line.strip()

    # Парсим строку
    key, value = prepared_line.split('\t', 1)
    value = int(value)

    # Счёт количества одинаковых префиксов
    if key == previous_key:
        previous_value += value
    else:
        if previous_key:
            # print(f'{previous_key}-{previous_value}')
            print '{key}-{value}'.format(key=previous_key, value=previous_value)
        previous_key = key
        previous_value = value

# Последний элемент
if previous_key:
    # print(f'{previous_key}-{previous_value}')
    print '{key}-{value}'.format(key=previous_key, value=previous_value)
