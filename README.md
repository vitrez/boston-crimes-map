## Домашнeе заданиe № 3. Введение в Spark + Гид по безопасному Бостону

### Задание: Гид по безопасному Бостону
В этом задании предлагается собрать статистику по криминогенной обстановке в разных районах Бостона. В качестве исходных данных используется датасет
https://www.kaggle.com/AnalyzeBoston/crimes-in-boston

С помощью Spark соберите агрегат по районам (поле district) со следующими метриками:
  * crimes_total - общее количество преступлений в этом районе
  * crimes_monthly - медиана числа преступлений в месяц в этом районе
  * frequent_crime_types - три самых частых crime_type за всю историю наблюдений в этом районе, объединенных через запятую с одним пробелом “, ” , расположенных в порядке убывания частоты
    * crime_type - первая часть NAME из таблицы offense_codes, разбитого по разделителю “-” (например, если NAME “BURGLARY - COMMERICAL - ATTEMPT”, то crime_type “BURGLARY”)
  * lat - широта координаты района, расчитанная как среднее по всем широтам инцидентов
  * lng - долгота координаты района, расчитанная как среднее по всем долготам инцидентов
Программа должна упаковываться в uber-jar (с помощью sbt-assembly), и запускаться командой:
```
spark-submit --master local[*] --class com.example.BostonCrimesMap /path/to/jar {path/to/crime.csv} {path/to/offense_codes.csv} {path/to/output_folder}
```
где {...} - аргументы, передаваемые пользователем.
Результатом её выполнения должен быть один файл в формате .parquet в папке _path/to/output_folder_.
Для джойна со справочником необходимо использовать broadcast.


### Решение
Исходный код программы на scala расположен здесь: _src/main/scala/boston-crimes-map/boston-crimes-map.scala_

Датасеты скопированы в папку _crimes-in-boston_

Для сборки jar-файла выполнялась команда:
```
sbt assembly
```

Для запуска spark-job вводим примерно такую команду:
```
spark-submit --master local[*] --class com.example.BostonCrimesMap target/scala-2.11/boston-crimes-map-assembly-0.1.0-SNAPSHOT.jar crimes-in-boston/crime.csv crimes-in-boston/offense_codes.csv ./result
```

Результат в виде паркет-файла будет лежать в папке ./result
