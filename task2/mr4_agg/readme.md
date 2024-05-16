## Задание

Посчитать число слов начинающихся с одинаковых префиксов из `PREFIX_LEN` букв. Слова короче `PREFIX_LEN` - игнорировать - качестве примера взять PREFIX_LEN=3 - Задать в качестве константы в коде меппера и/или редьюсера. Символы и цифры считать разделителями. Формат вывода - `f"{prefix}-{count}"`.

## Описание

Маппер:

Делит текст по специальным символам и цифрам, выделяет префикс и если префикс больше или равен отправляяет его, иначе пропускает.
Длина префикса указывается в `PREFIX_LEN`.

Редьюсер:

Считает количество одинаковых префиксов.
Количество редьюсеров может быть любым.

## Команды
Отправка на сервер:

```
scp script.zip <login>@<ip>~
```

Распаковка на сервере:

```
unzip script.zip
```


Запуск:

Предполагается, что данные уже лежат в /home/$USER/data/data1/


```
mapred streaming \
-D mapred.reduce.tasks=1 \
-input /home/$USER/data/data1/books/book1/84-0.txt \
-output /home/$USER/data/output1/mr4_agg \
-mapper mr4_agg_mapper.py \
-reducer mr4_agg_reducer.py \
-file /home/$USER/script/mr4_agg_mapper.py \
-file /home/$USER/script/mr4_agg_reducer.py
```

Просмотр результатов:

```
hdfs dfs -cat /home/$USER/data/output1/mr4_agg/part-*
```

Очистить все:

```
hdfs dfs -rm -r /home/$USER/data/output1/mr4_agg
rm script.zip
rm -r script
```