#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""mr1_wc_mapper.py"""

import sys
import re

PREFIX_LEN = 3

regex = re.compile('[^a-z ]+')

for line in sys.stdin:
    # Оставляем только буквы.
    # Остальные знаки считаем как разделитель (пробел)
    prepared_line = regex.sub(' ', line.strip().lower())

    # Делим полученную строку на слова
    words = prepared_line.split()

    for word in words:
        # Игнорируем слово, если оно меньше PREFIX_LEN
        if len(word) < PREFIX_LEN:
            continue

        prefix = word[:PREFIX_LEN]

        # print(f'{prefix}\t{1}')
        print '{key}\t{value}'.format(key=prefix, value=1)

