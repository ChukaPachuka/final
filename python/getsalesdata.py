import requests
import psycopg2
import os
from datetime import datetime, timedelta

# конфигурация API
API_URL = "http://final-project.simulative.ru/data"
DATE = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")  # дата вчерашнего дня

# конфигурация базы данных
DB_CONFIG = {
    "dbname": "marketplace",
    "user": "postgres",
    "password": "postgres",
    "host": "158.160.166.108",
    "port": "5433"
}

# путь к файлу логов
LOG_DIR = r"C:\Users\user\Desktop\project-final"
LOG_FILE = os.path.join(LOG_DIR, "log.txt")

def log_message(message):
    """Функция для записи логов в файл"""
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    log_entry = f"{timestamp} - {message}\n"

    print(log_entry.strip())
    with open(LOG_FILE, "a", encoding="utf-8") as log:
        log.write(log_entry)

def fetch_data():
    """Функция для запроса данных из API"""
    try:
        log_message("Запрос данных из API")
        params = {"date": DATE}
        response = requests.get(API_URL, params=params, timeout=10)
        if response.status_code == 200:
            data = response.json()
            log_message(f"Данные успешно получены. Количество записей: {len(data)}")
            return data
        else:
            log_message(f"Ошибка запроса: {response.status_code}, {response.text}")
            return None
    except Exception as e:
        log_message(f"Ошибка при запросе API: {e}")
        return None

def save_to_db(sales_data):
    """Функция для записи данных в PostgreSQL"""
    try:
        log_message("Запись данных в базу")
        conn = psycopg2.connect(**DB_CONFIG)
        cur = conn.cursor()

        insert_query = """
        INSERT INTO sales (client_id, gender, purchase_datetime, purchase_time_seconds, 
                           product_id, quantity, price_per_item, discount_per_item, total_price) 
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s);
        """

        count = 0
        for sale in sales_data:
            cur.execute(insert_query, (
                sale['client_id'],
                sale['gender'],
                sale['purchase_datetime'],
                sale['purchase_time_as_seconds_from_midnight'],
                sale['product_id'],
                sale['quantity'],
                sale['price_per_item'],
                sale['discount_per_item'],
                sale['total_price']
            ))
            count += 1

        conn.commit()
        cur.close()
        conn.close()
        log_message(f"Данные успешно записаны в базу. Всего записей: {count}")
    except Exception as e:
        log_message(f"Ошибка при записи в базу: {e}")

if __name__ == "__main__":
    log_message("Запуск скрипта")
    
    print("Запуск скрипта")  # выводим в консоль
    sales_data = fetch_data()

    if sales_data:
        print(f"Пример данных: {sales_data[:3]}")  # выводим 3 примера в консоль
        save_to_db(sales_data)
    else:
        print("Данные не получены")
        log_message("Данные не получены")
