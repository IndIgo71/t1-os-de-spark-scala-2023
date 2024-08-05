# Проект 0. Практика Hadoop в Docker 

## Обзор

Этот репозиторий содержит решения домашнего задания по настройке и использованию Apache Hadoop в Docker-контейнерах. Задание включает в себя создание и конфигурацию Hadoop HDFS, а также выполнение определенных операций с файлами в HDFS.

## Задание

1. Создать директорию `hwzero` в домашней директории на HDFS.
2. Изменить атрибуты директории для ограничения доступа.
3. Создать файл `hwzero.txt` на сервере, записать в него логин пользователя.
4. Скопировать файл `hwzero.txt` в директорию `hwzero` на HDFS.
5. Установить атрибуты файла для разрешения чтения и изменения пользователем, и только чтения для остальных.

## Файлы

- `docker-compose.yml`: Определяет конфигурацию сервисов Hadoop, запущенных в Docker.
- `config`: Содержит конфигурационные параметры для Hadoop, используемые в `docker-compose.yml`.

## Решение
- Запустите Docker-контейнеры из директории с `docker-compose.yml`
```bash
docker-compose up
```
- Подключитесь к контейнеру namenode
```bash
docker exec -it namenode /bin/bash
```
- Выполните внутри контейнера
```bash
hdfs dfs -mkdir -p /usr/student45/hwzero

hdfs dfs -chmod 744 /usr/student45/hwzero

cat 'student45' >> hwzero.txt

hdfs dfs -put ./hwzero.txt /usr/student45/hwzero/

hdfs dfs -chmod 744 /usr/student45/hwzero/hwzero.txt
```