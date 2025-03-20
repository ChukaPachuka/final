# final
Дипломная работа

01 - Настройка  

Что сделано:  
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

- вынесла параметры подключения в config.ini
  ![image](https://github.com/user-attachments/assets/f8767cfc-89cc-4c51-b757-e979debb497b)  

02 - Написание скриптов для забора данных  

Что сделано:  

- написала скрипт getsalesdata, который забирает данные за предыдущий день с http://final-project.simulative.ru/data  

  ![image](https://github.com/user-attachments/assets/856d59f5-0202-4306-af0c-aeb8c29bd7ea)

- в планировщике задач windows настроила расписание для запуска скрипта – ежедневно в 10:00:

  ![image](https://github.com/user-attachments/assets/af4034e2-81c0-45f4-988a-570c5eccad72)

- настроила логирование в файл log:

  ![image](https://github.com/user-attachments/assets/ec7182d6-4946-43e2-b85c-04329a9f4639)

- написала скрипт getallsales data, который забирает все исторические данные (начиная с 2024-01-01) с http://final-project.simulative.ru/data. Если данные за день уже были ранее загружены, повторная загрузка данных не происходит:

  ![image](https://github.com/user-attachments/assets/c6e29fde-d4c3-44a1-b633-7d00e6101881)  

- настроила логирование в файл log:  

  ![image](https://github.com/user-attachments/assets/097dcfcf-d68b-4a96-aa5d-be36343d5361)

03 - Создание дашборда в Metabase  

Что сделано:  

- подключила базу данных marketplace к metabase:

  ![image](https://github.com/user-attachments/assets/7189d4e8-c4a8-41f2-98af-36a3b3813c6c)  

- написала SQL-запросы для построения визуализаций:

  ![image](https://github.com/user-attachments/assets/244d7ff5-e65b-480f-af40-5ab6d03e36e2)

  ![image](https://github.com/user-attachments/assets/7cff3cce-3693-4494-86d3-032aecb0d00d)

  ![image](https://github.com/user-attachments/assets/b6d76cd1-bb95-43f7-b297-3c1fc873ca35)

- собрала дашборд "Анализ продаж маркетплейса" (ссылка: http://158.160.166.108:3000/dashboard/1-analiz-prodazh-marketplejs?%25D0%25B4%25D0%25B8%25D0%25B0%25D0%25BF%25D0%25B0%25D0%25B7%25D0%25BE%25D0%25BD_%25D0%25B4%25D0%25B0%25D1%2582=)

  ![image](https://github.com/user-attachments/assets/19034ba8-c60f-43a6-9706-4da9e2319d14)

  ![image](https://github.com/user-attachments/assets/2d4d274d-69de-41e4-9e9c-79fcf1d96bff)

  ![image](https://github.com/user-attachments/assets/80ae580e-1dfe-4ceb-9aa6-4eb824f4cbb1)

  ![image](https://github.com/user-attachments/assets/4273573d-5598-4d2b-a81e-5d4a211c73f9)





- 



- 

    


    
