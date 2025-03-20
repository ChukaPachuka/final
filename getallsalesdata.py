import requests
import psycopg2
import os
from datetime import datetime, timedelta

# конфигурация API
API_URL = "http://final-project.simulative.ru/data"

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

    print(log_entry.strip())  # выводим в консоль
    with open(LOG_FILE, "a", encoding="utf-8") as log:
        log.write(log_entry)

def fetch_data(date):
    """Функция для запроса данных из API за конкретную дату"""
    try:
        log_message(f"Запрос данных за {date}")
        response = requests.get(API_URL, params={"date": date}, timeout=10)

        if response.status_code == 200:
            data = response.json()
            log_message(f"Данные за {date} успешно получены. Количество записей: {len(data)}")
            return data
        else:
            log_message(f"Ошибка запроса за {date}: {response.status_code}, {response.text}")
            return None
    except Exception as e:
        log_message(f"Ошибка при запросе данных за {date}: {e}")
        return None

def save_to_db(sales_data, date):
    """Функция для записи данных в PostgreSQL"""
    try:
        log_message(f"Запись данных за {date} в базу")
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
        log_message(f"Данные за {date} успешно записаны в базу. Всего записей: {count}")
    except Exception as e:
        log_message(f"Ошибка при записи данных за {date} в базу: {e}")

def get_existing_dates():
    """Функция для получения списка уже загруженных дат из базы"""
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        cur = conn.cursor()

        cur.execute("SELECT DISTINCT purchase_datetime FROM sales;")
        existing_dates = {row[0].strftime("%Y-%m-%d") for row in cur.fetchall()}

        cur.close()
        conn.close()
        return existing_dates
    except Exception as e:
        log_message(f"Ошибка при получении загруженных дат: {e}")
        return set()

if __name__ == "__main__":
    log_message("Запуск загрузки всех данных")

    # определяем диапазон дат
    start_date = datetime(2024, 1, 1)
    end_date = datetime.now() - timedelta(days=1)  # до вчерашнего дня

    existing_dates = get_existing_dates()  # получаем уже загруженные даты

    current_date = start_date
    while current_date <= end_date:
        date_str = current_date.strftime("%Y-%m-%d")

        if date_str in existing_dates:
            log_message(f"Пропуск {date_str} (уже загружено)")
        else:
            sales_data = fetch_data(date_str)
            if sales_data:
                save_to_db(sales_data, date_str)

        current_date += timedelta(days=1)

    log_message("Загрузка всех данных заевршена")
