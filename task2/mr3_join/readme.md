## Задание

Написать inner-join на MapReduce (ReduceSideJoin) для двух таблиц по полю product_id:
1. shop_product.csv содержит поля
product_id и description
2. shop_price.csv содержит поля product_id и
price
В результате должен получится файл из трех колонок: (product_id, description, price)

## Описание

Маппер (price и product):

Читает таблицу и к каждой строчке добавляет название таблицы из которой она была прочитана.


Маппер (join):

Отправляет текст дальше.


Редьюсер (join):
Каждой строке из таблицы product сопоставляет строку из таблицы price, если у них одинаковые id, и выводим их.
Количество редьюсеров может быть любым.

## Команды
Отправка на сервер:

```
scp script.zip <login>@<ip>:~
```

Распаковка на сервере:

```
unzip script.zip
```

Предполагается, что данные уже лежат в /home/$USER/data/data1/

### Первый MapReduce
Запуск маппера (price):

```
mapred streaming \
-D mapred.reduce.tasks=0 \
-input /home/$USER/data/data1/shop_price.csv \
-output /home/$USER/data/output1/mr3_map_prices \
-mapper mr3_price_mapper.py \
-file /home/$USER/script/mr3_price_mapper.py
```


Запуск маппера (product):

```
mapred streaming \
-D mapred.reduce.tasks=0 \
-input /home/$USER/data/data1/shop_product.csv \
-output /home/$USER/data/output1/mr3_map_product \
-mapper mr3_product_mapper.py \
-file /home/$USER/script/mr3_product_mapper.py
```

### Второй MapReduce


```
mapred streaming \
-D mapred.reduce.tasks=1 \
-input /home/$USER/data/output1/mr3_map_*/part-* \
-output /home/$USER/data/output1/mr3_join \
-mapper mr3_join_mapper.py \
-reducer mr3_join_reducer.py \
-file /home/$USER/script/mr3_join_mapper.py \
-file /home/$USER/script/mr3_join_reducer.py
```



Просмотр результатов:

```
hdfs dfs -cat /home/$USER/data/output1/mr3_join/part-*
```

Очистить все:

```
hdfs dfs -rm -r /home/$USER/data/output1/mr3_join
rm script.zip
rm -r script
```