## Домашнeе заданиe № 3. Введение в Spark + Гид по безопасному Бостону

Исходный код программы на scala расположен здесь: src/main/scala/boston-crimes-map/boston-crimes-map.scala

Для сборки jar-файла выполнялась команда:
```
sbt assembly
```

Для запуска spark-job вводим примерно такую команду:
```
spark-submit --master local[*] --class com.example.BostonCrimesMap target/scala-2.11/boston-crimes-map-assembly-0.1.0-SNAPSHOT.jar crimes-in-boston/crime.csv crimes-in-boston/offense_codes.csv ./result
```

Результат в виде паркет-файла будет лежать в папке ./result
