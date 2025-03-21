# final
Дипломная работа

## 01 - Настройка  

### Что сделано:  
- развернула сервер на Yandex Cloud, поставила PostgreSQL, Metabase:
  
  ![image](https://github.com/user-attachments/assets/863dc2db-6440-4f0e-b2b6-530f6cff5ab6)
    
- создала базу данных marketplace:

  ![image](https://github.com/user-attachments/assets/36bea35c-5aa6-46f2-b2a4-bee2df1449d0)  
  
- создала таблицу sales в базе данных marketplace:
  
  ![image](https://github.com/user-attachments/assets/53a9da85-243a-44a2-a864-5df4128da852)  
  
- создала внешнего пользователя teacher для подключения к базе данных marketplace и metabase:
  
  **PostgreSQL**
  Хост          158.160.166.108  
  Порт          5433  
  База данных   marketplace  
  Пользователь	teacher  
  Пароль 		    qweasd963

  **Metabase**
  http://158.160.166.108:3000/
  Пользователь  teacher@email.com
  Пароль        Y3re2pHWCu?b8g

- вынесла параметры подключения в [config.ini](/postgresql/config.ini)
  
  ![image](https://github.com/user-attachments/assets/f8767cfc-89cc-4c51-b757-e979debb497b)  

- данные для подключения базы данных marketplace и входа в metabase вынесены в отдельный файл [entry.txt](/entry.txt)  

## 02 - Написание скриптов для забора данных  

### Что сделано:  

- написала скрипт [getsalesdata.py](/python/getsalesdata.py), который забирает данные за предыдущий день с http://final-project.simulative.ru/data:  

  ![image](https://github.com/user-attachments/assets/856d59f5-0202-4306-af0c-aeb8c29bd7ea)

- в планировщике задач windows настроила расписание для запуска скрипта – ежедневно в 10:00:

  ![image](https://github.com/user-attachments/assets/af4034e2-81c0-45f4-988a-570c5eccad72)

- настроила логирование в файл [log.txt](/python/log.txt):

  ![image](https://github.com/user-attachments/assets/ec7182d6-4946-43e2-b85c-04329a9f4639)

- написала скрипт [getallsalesdata.py](/python/getallsalesdata.py), который забирает все исторические данные (начиная с 2024-01-01) с http://final-project.simulative.ru/data. Если данные за день уже были ранее загружены, повторная загрузка данных не происходит:

  ![image](https://github.com/user-attachments/assets/c6e29fde-d4c3-44a1-b633-7d00e6101881)  

- настроила логирование в файл [log.txt](/python/log.txt):  

  ![image](https://github.com/user-attachments/assets/097dcfcf-d68b-4a96-aa5d-be36343d5361)

## 03 - Создание дашборда в Metabase  

### Что сделано:  

- подключила базу данных marketplace к metabase:

  ![image](https://github.com/user-attachments/assets/7189d4e8-c4a8-41f2-98af-36a3b3813c6c)  

- написала SQL-запросы для построения визуализаций:

  ![image](https://github.com/user-attachments/assets/244d7ff5-e65b-480f-af40-5ab6d03e36e2)  

  ![image](https://github.com/user-attachments/assets/7cff3cce-3693-4494-86d3-032aecb0d00d)  

  ![image](https://github.com/user-attachments/assets/b6d76cd1-bb95-43f7-b297-3c1fc873ca35)  

- собрала дашборд ["Анализ продаж маркетплейса"](/metabase/sales_analysis_metabase.pdf) [(ссылка на дашборд в metabase)](http://158.160.166.108:3000/dashboard/1-analiz-prodazh-marketplejs?%25D0%25B4%25D0%25B8%25D0%25B0%25D0%25BF%25D0%25B0%25D0%25B7%25D0%25BE%25D0%25BD_%25D0%25B4%25D0%25B0%25D1%2582=)  

  ![image](https://github.com/user-attachments/assets/19034ba8-c60f-43a6-9706-4da9e2319d14)  

  ![image](https://github.com/user-attachments/assets/2d4d274d-69de-41e4-9e9c-79fcf1d96bff)  

  ![image](https://github.com/user-attachments/assets/80ae580e-1dfe-4ceb-9aa6-4eb824f4cbb1)  

  ![image](https://github.com/user-attachments/assets/4273573d-5598-4d2b-a81e-5d4a211c73f9)  

## 04 - Проведение исследования за 2024 год  

### Что сделано:  

- написала код для проведения исследования – расчет статистических метрик, тестирование гипотез, ABC-анализ:  

  ![image](https://github.com/user-attachments/assets/38998db7-c871-4ff0-a550-7484165a3825)

- метрики и результаты расчёта:  

  🟢 Общая сумма продаж: 1,653,881,141,530.00 руб.  
  🟢 Общее количество заказов: 1999176  
  🟢 Средняя сумма заказа: 827,281.41 руб.  
  🟢 Среднее количество товаров в заказе: 32.99  

  🟢 Средняя дневная выручка: 4,531,181,209.67 руб.  
  🟢 Средняя недельная выручка: 31,205,304,557.17 руб.  
  🟢 Средняя месячная выручка: 137,823,428,460.83 руб.  

  🟢 Среднее DAU: 5458.46  
  🟢 Среднее MAU: 153323.17  
  🟢 Среднее WAU: 36985.15  
  🟢 Rolling Retention по когортам  
  🟢 Средний LTV: 7,319,491.23 руб.  
  🟢 Динамика продаж по дням  
  🟢 Динамика продаж по месяцам  
  🟢 Количество заказов по месяцам  
  🟢 Количество заказов по декадам месяца  
  🟢 Топ-10 товаров по выручке  
  🟢 Топ-5 товаров с наименьшими продажами  
  🟢 Топ-10 клиентов по сумме заказов  
  🟢 Анализ скидок (группы скидок, сумма продаж, количество заказов)  
  
![image](https://github.com/user-attachments/assets/07750398-791e-426e-b52c-9062e41fab2a)

![image](https://github.com/user-attachments/assets/16312d73-4d4d-4759-8977-411f9707b467)

![image](https://github.com/user-attachments/assets/8dd85e13-cb17-4c58-8dcc-559b5a0ac6de)

![image](https://github.com/user-attachments/assets/817d72dc-089f-4b6e-8d46-379e1edacc64)

- сформулировала гипотезы и провела их тестирование, результаты тестирования:  

  🟢 Гипотеза 1. Среднее количество товаров в заказе зависит от наличия скидки  
  📊 Результаты t-теста:  
  Среднее количество товаров без скидки: 32.79  
  Среднее количество товаров со скидкой: 32.99  
  p-value = 0.89119  
  ❌ НЕ ОТВЕРГАЕМ H₀: Разница в количестве продаж не является статистически значимой  

  🟢 Гипотеза 2. Покупатели чаще делают заказы на товары со скидкой, чем без скидки  
  📊 Результаты z-теста для пропорций:  
  Количество проданных товаров без скидки: 7313  
  Количество проданных товаров со скидкой: 65948506  
  Доля проданных товаров со скидкой: 0.9999  
  Доля проданных товаров без скидки: 0.0001  
  z-статистика: 11482.7323  
  p-value = 0.00000  
  ✅ ОТВЕРГАЕМ H₀: Покупатели чаще заказывают товары со скидкой  

  🟢 Гипотеза 3. Среднее количество проданных единиц товара ниже у товаров с высокой ценой  
  📊 Результаты t-теста:  
  Среднее количество проданных товаров (дешёвые товары): 32.99  
  Среднее количество проданных товаров (дорогие товары): 33.00  
  p-value = 0.75641  
  ❌ НЕ ОТВЕРГАЕМ H₀: Разница в продажах дешёвых и дорогих товаров не является статистически значимой  

  🟢 Гипотеза 4. Женщины совершают покупки в среднем на большую сумму, чем мужчины  
  📊 Результаты t-теста:  
  Средний чек у мужчин: 827583.46  
  Средний чек у женщин: 826979.75  
  p-value = 0.65465  
  ❌ НЕ ОТВЕРГАЕМ H₀: Разница в среднем чеке между мужчинами и женщинами не является статистически значимой  

 - добавила визуализации расчетов [(ссылка на папку с изображениями)](/charts):  

   ![image](https://github.com/user-attachments/assets/896a7f03-11a0-4ada-8881-3a6ee0d57e6c)  

   ![image](https://github.com/user-attachments/assets/337341c3-a2e3-4697-8475-aa43ce6a8275)  

   ![image](https://github.com/user-attachments/assets/6b437efa-26b2-490a-8403-5147952a76e1)  

  - подготовила отчет [отчет](/research.pdf) о проведенном исследовании и сформулировала рекомендации 
