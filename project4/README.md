# Проект 4. Анализ данных популярных песен и артистов

## Задание 1

Проанализируйте представленный физичский план и опишите возможный код, который его генерирует.

```
== Physical Plan ==

Exchange RoundRobinPartitioning(10), REPARTITION_BY_NUM, [id=#528]

+- SortAggregate(key=[country#226], functions=[count(1), max(name#225)])

   +- *(2) Sort [country#226 ASC NULLS FIRST], false, 0

      +- Exchange hashpartitioning(country#226, 20), ENSURE_REQUIREMENTS, [id=#523]

         +- SortAggregate(key=[country#226], functions=[partial_count(1), partial_max(name#225)])

            +- *(1) Sort [country#226 ASC NULLS FIRST], false, 0

               +- FileScan csv [name#225,country#226] Batched: false, DataFilters: [], Format: CSV, Location: InMemoryFileIndex(1 paths)[file:///path/to/file/country_info.csv, PartitionFilters: [], PushedFilters: [], ReadSchema: struct<name:string,country:string>
```

## Задание 2

Предоставлено два файла, один из них – файл с самыми популярными песнями ([Top_Songs_US.csv](src/main/resources/Top_Songs_US.csv)) и файл с информацией про артистов ([Artist.csv](src/main/resources/Artists.csv)). Напишите код, который выводит информацию по артистам, у которых более 50000000 подписчиков. Требуется вывести имя артиста и суммарную длительность его песен, которые попали в топ.

Опишите оптимизации, которые были выполнены Catalyst.

При выполнении задания требуется в коде указать следующие параметры:

```scala
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", -1) spark.conf.set("spark.sql.adaptive.enabled", false)
```

## Задание 3

Выполните [задание 2](#задание-2) с использование broadcast join для меньшей из двух таблиц. Сравните планы выполнения и производительность. Опишите изменения в плане и сравните скорость выполнения вычислений.
