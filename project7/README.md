# Эмуляция Data Skew

## Задание 1

Описать распредение памяти по областям executor для представленной конфигурации

```
spark.execution.memory=5g (5120mb)
spark.memory.fraction=0.6
spark.memory.storageFraction=0.5
```


<details><summary>Решение</summary>

```
reserved = 300mb
execution memory = (spark.execution.memory - reserved) * spark.memory.fraction * (1 - spark.memory.storageFraction) = (5120 - 300) * 0.6 * (1 - 0.5) = 1446mb (~1.44gb)
storage memory = (spark.execution.memory - reserved) * spark.memory.fraction * (1 - spark.memory.storageFraction) = (5120 - 300) * 0.6 * 0.5 = 1446mb (~1.44 GB)
user memory =  (spark.execution.memory - reserved) * (1 - spark.memory.fraction) = (5120 - 300) * (1 - 0.4) = 1928mb (~1.88gb)
```

</details>


## Задание 2

   - Напишите генератор DataFrame, который демонстрирует проблему с сильным перекосом данных по ключу.
   - Применение техники "соления" к ключам для улучшения распределения задач и сокращения времени выполнения агрегаций.



