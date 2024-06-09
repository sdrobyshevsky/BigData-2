// Вводный курс по Big Data (семинары)
// Урок 2. SQL & Big Data
// Условие:
// Загрузите датасет по ценам на жилье Airbnb, доступный на kaggle.com: https://www.kaggle.com/dgomonov/new-york-city-airbnb-open-data
// Подсчитайте среднее значение и дисперсию по признаку ”price” в hive
// Используя Python, реализуйте скрипт mapper.py и reducer.py для расчета
// Проверьте правильность подсчета статистики методом mapreduce в сравнении со hive.

// Шаг 1: Загрузка датасета с ценами на жилье Airbnb

// 1. Перейдите на сайт kaggle.com и скачайте датасет "New York City Airbnb Open Data" с ценами на жилье.
// 2. Разархивируйте данные и загрузите их в HDFS с помощью команды hdfs dfs -put <local_input_file> <hdfs_output_directory>.

// Шаг 2: Подсчет среднего значения и дисперсии в Hive

// 1. Создайте в Hive таблицу для загруженного датасета и выполните запрос для подсчета среднего значения и дисперсии по признаку "price". Например:

sql
CREATE EXTERNAL TABLE IF NOT EXISTS airbnb (
    id INT,
    name STRING,
    price DOUBLE,
    ...
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION '<hdfs_output_directory>';

SELECT AVG(price), VARIANCE(price)
FROM airbnb;

// 2. Запустите запрос в Hive и сохраните результаты.

// Шаг 3: Реализация скриптов Mapper и Reducer в Python

// 1. Напишите скрипт mapper.py, который будет считывать данные из стандартного ввода и выводить пары ключ-значение, где ключ – это строка "price", а значение – цена из данных. 
// Пример кода для mapper.py:

python
#!/usr/bin/env python

import sys

for line in sys.stdin:
    data = line.strip().split(',')
    if len(data) == 15:
        price = data[9]
        print("price\t" + price)


// 2. Напишите скрипт reducer.py, который будет считывать данные из стандартного ввода, вычислять среднее значение и дисперсию цен и выводить результат.
// Пример кода для reducer.py:

python
#!/usr/bin/env python

import sys
from math import sqrt

sum_price = 0
sum_squared_price = 0
count = 0

for line in sys.stdin:
    key, value = line.strip().split('\t')
    if key == "price":
        price = float(value)
        sum_price += price
        sum_squared_price += price*price
        count += 1

mean_price = sum_price / count
variance_price = (sum_squared_price / count) - mean_price**2
print(mean_price, variance_price)


// Шаг 4: Проверка правильности подсчета статистики

// 1. Запустите MapReduce задачу, используя написанные скрипты mapper.py и reducer.py:

bash
hadoop jar <path_to_hadoop_streaming_jar> -file mapper.py -mapper mapper.py -file reducer.py -reducer reducer.py -input <hdfs_input_file> -output <hdfs_output_directory>


// 2. После завершения выполнения задачи, проверьте результаты и сравните полученные значения с результатами из Hive.
// Этими шагами вы выполнили задание по подсчету среднего значения и дисперсии по признаку "price" с использованием SQL (Hive) и MapReduce в Python.


// Второй вариант решения задачи:

python
# mapper.py
import sys

# Считываем данные из стандартного потока ввода
for line in sys.stdin:
    line = line.strip()
    data = line.split(",")
    
    # Печатаем price в качестве ключа и его значение
    print("%s\t%s" % (data[9], 1))

# reducer.py
import sys

current_price = None
price_sum = 0
price_count = 0

# Считываем результаты из mapper.py
for line in sys.stdin:
    line = line.strip()
    price, count = line.split("\t")
    
    # Преобразуем строковое значение в число
    try:
        count = int(count)
    except ValueError:
        continue

    # Если текущий price равен предыдущему price
    if current_price == price:
        price_sum += float(price)
        price_count += count
    # Если price не равен предыдущему price
    else:
        if current_price:
            average_price = price_sum / price_count
            print("%s\t%s" % (current_price, average_price))
        price_sum = float(price)
        price_count = count
        current_price = price

# Выводим последний результат, так как после цикла нет повторной проверки
if current_price == price:
    average_price = price_sum / price_count
    print("%s\t%s" % (current_price, average_price))


# Для проверки корректности вычислений воспользуемся стандартными средствами Python. 
# Для подсчета среднего значения и дисперсии по признаку "price" можно использовать библиотеку pandas:

python
import pandas as pd

# Загружаем данные из csv файла
data = pd.read_csv('AB_NYC_2019.csv')

# Рассчитываем среднее значение и дисперсию по признаку "price"
average_price = data['price'].mean()
variance_price = data['price'].var()

print("Average price:", average_price)
print("Price variance:", variance_price)

# Выполнив оба вида подсчетов для данных из датасета airbnb, можно сравнить результаты и убедиться в правильности выполнения задания.
