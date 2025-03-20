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

- 





3.	Создала базу данных marketplace
4.	Создала таблицу Sales в базе данных marketplace
5.	Создала нового пользователя teacher для внешнего подключения:
Хост			158.160.166.108
Порт			5433
База данных	marketplace
Пользователь	teacher
Пароль 		qweasd963 
