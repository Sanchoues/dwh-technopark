### Переход в папку с пайплайном

```
cd luigi
```

### Настройка переменных окружения

```
export HADOOP_CONF_DIR=/etc/hadoop/conf
export HIVE_HOME=/usr/lib/hive
export METASTORE_PORT=9083
export LUIGI_CONFIG_PATH=luigi.conf
```

### Запуск пайплайна

```
PYTHONPATH='.' python3 -m luigi --module pipeline ProductStat
```

Примечание: имя пользоваля указывается в файле pipeline.py. По умолчанию берётся значение переменной окружения.

### Проверка результатов

```
hdfs dfs -cat /user/$USER/task4/price_stat/part-* | head

hdfs dfs -cat /user/$USER/task4/ok_dem/part-* | head

hdfs dfs -cat /user/$USER/task4/product_stat/part-* | head
```

### Удалить результаты

```
hdfs dfs -rm -r /user/$USER/task4/
``` 