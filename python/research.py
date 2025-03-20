import configparser
import os
import pandas as pd
import psycopg2
import numpy as np
from scipy import stats
from statsmodels.stats.proportion import proportions_ztest
import matplotlib.pyplot as plt
import seaborn as sns

# определяем путь к текущей папке, где находится скрипт
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
config_path = os.path.join(BASE_DIR, "config.ini")

# загружаем конфигурацию
config = configparser.ConfigParser()
config.read(config_path)

# получаем параметры подключения
DB_CONFIG = {
    "dbname": config["database"]["dbname"],
    "user": config["database"]["user"],
    "password": config["database"]["password"],
    "host": config["database"]["host"],
    "port": config["database"]["port"]
}

print("\n🟢 Параметры подключения:")
print(DB_CONFIG) # выводим параметры подключения

# подключение к базе данных PostgreSQL
# DB_CONFIG = {
    # "dbname": "marketplace",
    # "user": "postgres",
    # "password": "postgres",
    # "host": "158.160.166.108",
    # "port": "5433"
# }

# SQL-запрос для выборки данных за 2024 год
query = """
SELECT 
    sale_id, client_id, gender, purchase_datetime, purchase_time_seconds, 
    product_id, quantity, price_per_item, discount_per_item, total_price
FROM sales
WHERE purchase_datetime BETWEEN '2024-01-01' AND '2024-12-31';
"""

# загружаем данные в dataframe
conn = psycopg2.connect(**DB_CONFIG)
df = pd.read_sql(query, conn)
conn.close()

# преобразуем дату в datetime
df["purchase_datetime"] = pd.to_datetime(df["purchase_datetime"])
df["month"] = df["purchase_datetime"].dt.to_period("M")  # месяц
df["day"] = df["purchase_datetime"].dt.to_period("D")    # день
df["week"] = df["purchase_datetime"].dt.to_period("W")   # неделя

# добавляем декаду месяца
df["decade"] = pd.cut(df["purchase_datetime"].dt.day, bins=[1, 10, 20, 31], labels=["1-10", "11-20", "21+"])

# 1. Общая сумма продаж
total_sales = df["total_price"].sum()
print(f"🟢 Общая сумма продаж: {total_sales:,.2f} руб.")

# 2. Общее количество заказов
total_orders = df["sale_id"].nunique()
print(f"🟢 Общее количество заказов: {total_orders}")

# 3. Средняя сумма заказа
avg_order_value = df["total_price"].mean()
print(f"🟢 Средняя сумма заказа: {avg_order_value:,.2f} руб.")

# 4. Среднее количество товаров в заказе
avg_items_per_order = df["quantity"].mean()
print(f"🟢 Среднее количество товаров в заказе: {avg_items_per_order:.2f}")

# 5. Выручка за день
daily_sales = df.groupby("day")["total_price"].sum()
print(f"\n🟢 Средняя дневная выручка: {daily_sales.mean():,.2f} руб.")

# 6. Выручка за неделю
weekly_sales = df.groupby("week")["total_price"].sum()
print(f"🟢 Средняя недельная выручка: {weekly_sales.mean():,.2f} руб.")

# 7. Выручка за месяц
monthly_sales = df.groupby("month")["total_price"].sum()
print(f"🟢 Средняя месячная выручка: {monthly_sales.mean():,.2f} руб.")

# 8. DAU (количество уникальных клиентов в день)
dau = df.groupby("day")["client_id"].nunique().mean()
print(f"\n🟢 Среднее DAU: {dau:.2f}")

# 9. MAU (количество уникальных клиентов в месяц)
mau = df.groupby("month")["client_id"].nunique().mean()
print(f"🟢 Среднее MAU: {mau:.2f}")

# 10. WAU (количество уникальных клиентов в неделю)
wau = df.groupby("week")["client_id"].nunique().mean()
print(f"🟢 Среднее WAU: {wau:.2f}")

# Rolling Retention 30 дней
# преобразуем даты
df["purchase_datetime"] = pd.to_datetime(df["purchase_datetime"])
df["cohort"] = df.groupby("client_id")["purchase_datetime"].transform("min").dt.to_period("M")

# рассчитываем разницу в днях от первой покупки
df["days_since_first"] = (df["purchase_datetime"] - df.groupby("client_id")["purchase_datetime"].transform("min")).dt.days

# группируем по когортам и считаем rolling retention
rolling_retention = df.groupby(["cohort", "days_since_first"])["client_id"].nunique().unstack(fill_value=0)

# рассчитываем удержание
cohort_sizes = rolling_retention.iloc[:, 0]  # количество клиентов в 0 день (первый день покупки)
retention_rates = rolling_retention.divide(cohort_sizes, axis=0) * 100

# оставляем нужные дни (0, 1, 7, 14, 30, 60, 90)
retention_rates = retention_rates[[0, 1, 7, 14, 30, 60, 90]]

# выводим результат
print("🟢 Rolling Retention по когортам (0, 1, 7, 14, 30, 60, 90 дней):")
print(retention_rates.round(2))

# визуализация
plt.figure(figsize=(12, 6))
sns.heatmap(retention_rates, annot=True, cmap="coolwarm", fmt=".2f", linewidths=0.5)
plt.title("Rolling Retention по когортам (0, 1, 3, 7, 14, 30, 60, 90 дней)")
plt.xlabel("День с момента первой покупки")
plt.ylabel("Когорта (месяц)")
plt.show()

# 12. LTV = (средний чек) * (средняя частота покупок) * (средний срок жизни клиента)
purchase_freq = df.groupby("client_id")["sale_id"].count().mean()
customer_lifetime = (df.groupby("client_id")["purchase_datetime"].max() - df.groupby("client_id")["purchase_datetime"].min()).dt.days.mean()

ltv = avg_order_value * purchase_freq * (customer_lifetime / 30)  # в месяцах
print(f"🟢 Средний LTV: {ltv:,.2f} руб.")

# 13. Динамика продаж по дням
print("\n🟢 Динамика продаж по дням:")
print(daily_sales)

# визуализация
plt.figure(figsize=(12, 5))
daily_sales.plot(kind='line', marker='o', linestyle='-')
plt.title("Динамика продаж по дням")
plt.xlabel("Дата")
plt.ylabel("Выручка RUB")
plt.grid(True)
plt.xticks(rotation=45)
plt.show()

# 14. Динамика продаж по месяцам
print("🟢 Динамика продаж по месяцам:")
print(monthly_sales)

# визулизация
monthly_sales.plot(kind='bar', figsize=(12, 5), color='skyblue', edgecolor='black')
plt.title("Динамика продаж по месяцам")
plt.xlabel("Месяц")
plt.ylabel("Выручка RUB")
plt.xticks(rotation=45)
plt.grid(axis='y')
plt.show()

# 15. Количество заказов по месяцам
monthly_orders = df.groupby("month")["sale_id"].count()
print("🟢 Количество заказов по месяцам:")
print(monthly_orders)

# 16. Количество заказов по декадам месяца
decade_orders = df.groupby("decade")["sale_id"].count()
print("🟢 Количество заказов по декадам месяца:")
print(decade_orders)

# визуализация
decade_orders.plot(kind='bar', figsize=(10, 5), color=['#ff9999','#66b3ff','#99ff99'])
plt.title("Количество заказов по декадам месяца")
plt.xlabel("Декада месяца")
plt.ylabel("Количество заказов")
plt.xticks(rotation=0)
plt.grid(axis='y')
plt.show()

# 17. Топ-10 товаров по сумме продаж
top_products = df.groupby("product_id")["total_price"].sum().nlargest(10)
print("\n🟢 Топ-10 товаров по выручке:")
print(top_products)

# визуализация
top_products.plot(kind='barh', figsize=(10, 5), color='lightcoral', edgecolor='black')
plt.title("Топ-10 товаров по выручке")
plt.xlabel("Выручка RUB")
plt.ylabel("ID товара")
plt.grid(axis='x')
plt.gca().invert_yaxis()
plt.show()

# 18. Топ-5 товаров с наименьшими продажами
low_products = df.groupby("product_id")["total_price"].sum().nsmallest(5)
print("🟢 Топ-5 товаров с наименьшими продажами:")
print(low_products)

# 19. Топ-10 клиентов по сумме заказов
top_clients = df.groupby("client_id")["total_price"].sum().nlargest(10)
print("🟢 Топ-10 клиентов по сумме заказов:")
print(top_clients)

# 20. Матрица выручки и количества заказов по группам скидок
df["discount_percent"] = (df["discount_per_item"] / df["price_per_item"]) * 100
df["discount_group"] = pd.cut(df["discount_percent"], bins=[0, 25, 50, 75, 100], labels=["0-25%", "25-50%", "50-75%", "75-100%"])

discount_analysis = df.groupby("discount_group").agg({"total_price": "sum", "sale_id": "count"})
print("\n🟢 Анализ скидок (группы скидок, сумма продаж, количество заказов):")
print(discount_analysis)

# визуализация
discount_analysis.plot(kind='bar', figsize=(10, 5), color=['#ffcc99','#99ccff'])
plt.title("Влияние скидок на сумму продаж")
plt.xlabel("Скидка")
plt.ylabel("Выручка RUB")
plt.xticks(rotation=0)
plt.grid(axis='y')
plt.show()

print("\n🟢 Тестирование гипотез:")
# 21. Гипотеза 1. Среднее количество товаров в заказе зависит от наличия скидки
# разделяем данные на 2 группы
no_discount = df[df["discount_per_item"] == 0]["quantity"]
with_discount = df[df["discount_per_item"] > 0]["quantity"]

# применяем t-тест
test_result = stats.ttest_ind(no_discount, with_discount, equal_var=False)

# выводим результаты
print(f"\n🟢 Гипотеза 1. Среднее количество товаров в заказе зависит от наличия скидки:")
print(f"📊 Результаты t-теста:")
print(f"Среднее количество товаров без скидки: {no_discount.mean():.2f}")
print(f"Среднее количество товаров со скидкой: {with_discount.mean():.2f}")
print(f"p-value = {test_result.pvalue:.5f}")

# делаем выводы
if test_result.pvalue < 0.05:
    print("✅ ОТВЕРГАЕМ H₀: Товары со скидками продаются в среднем в большем количестве")
else:
    print("❌ НЕ ОТВЕРГАЕМ H₀: Разница в количестве продаж не является статистически значимой")

# визуализация
plt.figure(figsize=(8, 5))
sns.boxplot(data=df, x=df["discount_per_item"] > 0, y="quantity", palette=["#ff9999", "#66b3ff"])
plt.title("Количество товаров в заказе (со скидкой vs без скидки)")
plt.xlabel("Есть скидка?")
plt.ylabel("Количество товаров")
plt.xticks([0, 1], ["Без скидки", "Со скидкой"])
plt.grid(axis="y", linestyle="--", alpha=0.7)
plt.show()

# 22. Гипотеза 2. Покупатели чаще делают заказы на товары со скидкой, чем без скидки
# разделяем данные по наличию скидки
total_items_no_discount = df[df["discount_per_item"] == 0]["quantity"].sum()  # Всего проданных товаров без скидки
total_items_with_discount = df[df["discount_per_item"] > 0]["quantity"].sum()  # Всего проданных товаров со скидкой

# общее количество проданных товаров
total_items = total_items_no_discount + total_items_with_discount

# доли проданных товаров для каждой группы
p1 = total_items_with_discount / total_items  # доля товаров, проданных со скидкой
p2 = total_items_no_discount / total_items  # доля товаров, проданных без скидки

# количество проданных товаров в каждой группе
counts = np.array([total_items_with_discount, total_items_no_discount])
nobs = np.array([total_items, total_items])

# применяем z-тест
z_stat, p_value = proportions_ztest(counts, nobs, alternative='larger')

# выводим результаты
print(f"\n🟢 Гипотеза 2. Покупатели чаще делают заказы на товары со скидкой, чем без скидки:")
print(f"📊 Результаты z-теста для пропорций:")
print(f"Количество проданных товаров без скидки: {total_items_no_discount}")
print(f"Количество проданных товаров со скидкой: {total_items_with_discount}")
print(f"Доля проданных товаров со скидкой: {p1:.4f}")
print(f"Доля проданных товаров без скидки: {p2:.4f}")
print(f"z-статистика: {z_stat:.4f}")
print(f"p-value = {p_value:.5f}")

# делаем выводы
if p_value < 0.05:
    print("✅ ОТВЕРГАЕМ H₀: Покупатели чаще заказывают товары со скидкой")
else:
    print("❌ НЕ ОТВЕРГАЕМ H₀: Разница в частоте заказов не является статистически значимой")

# визуализация
discount_counts = df["discount_per_item"].gt(0).value_counts()

plt.figure(figsize=(6, 6))
plt.pie(discount_counts, labels=["Без скидки", "Со скидкой"], autopct="%1.1f%%", colors=["#ff9999", "#66b3ff"], startangle=90)
plt.title("Доля заказов со скидкой")
plt.show()

# 23. Гипотеза 3. Среднее количество проданных единиц товара ниже у товаров с высокой ценой
# определяем медианную цену
median_price = df["price_per_item"].median()

# разделяем товары на 2 группы
low_price_group = df[df["price_per_item"] <= median_price]["quantity"]
high_price_group = df[df["price_per_item"] > median_price]["quantity"]

# применяем t-тест
test_result = stats.ttest_ind(low_price_group, high_price_group, equal_var=False)

# выводим результаты
print(f"\n🟢 Гипотеза 3. Среднее количество проданных единиц товара ниже у товаров с высокой ценой:")
print(f"📊 Результаты t-теста:")
print(f"Среднее количество проданных товаров (дешёвые товары): {low_price_group.mean():.2f}")
print(f"Среднее количество проданных товаров (дорогие товары): {high_price_group.mean():.2f}")
print(f"p-value = {test_result.pvalue:.5f}")

# делаем выводы
if test_result.pvalue < 0.05:
    print("✅ ОТВЕРГАЕМ H₀: Дорогие товары действительно продаются в меньшем количестве")
else:
    print("❌ НЕ ОТВЕРГАЕМ H₀: Разница в продажах дешёвых и дорогих товаров не является статистически значимой")

# визуализация
plt.figure(figsize=(8, 5))
sns.boxplot(data=df, x=df["price_per_item"] > df["price_per_item"].median(), y="quantity", palette=["#ffcc99", "#99ccff"])
plt.title("Продажи: дорогие vs дешевые товары")
plt.xlabel("Категория товара")
plt.ylabel("Количество проданных единиц")
plt.xticks([0, 1], ["Дешевые", "Дорогие"])
plt.grid(axis="y", linestyle="--", alpha=0.7)
plt.show()

# 24. Гипотеза 4. Женщины совершают покупки в среднем на большую сумму, чем мужчины
# разделяем данные по полу
male_total_price = df[df["gender"] == "M"]["total_price"]
female_total_price = df[df["gender"] == "F"]["total_price"]

# применяем t-тест
test_result = stats.ttest_ind(female_total_price, male_total_price, equal_var=False, alternative='greater')

# выводим результаты
print(f"\n🟢 Гипотеза 4. Женщины совершают покупки в среднем на большую сумму, чем мужчины:")
print(f"📊 Результаты t-теста:")
print(f"Средний чек у мужчин: {male_total_price.mean():.2f}")
print(f"Средний чек у женщин: {female_total_price.mean():.2f}")
print(f"p-value = {test_result.pvalue:.5f}")

# делаем выводы
if test_result.pvalue < 0.05:
    print("✅ ОТВЕРГАЕМ H₀: Женщины тратят в среднем больше, чем мужчины")
else:
    print("❌ НЕ ОТВЕРГАЕМ H₀: Разница в среднем чеке между мужчинами и женщинами не является статистически значимой")

# визуализация
plt.figure(figsize=(8, 5))
sns.boxplot(data=df, x="gender", y="total_price", palette=["#ffcc99", "#99ccff"])
plt.title("Сравнение среднего чека мужчин и женщин")
plt.xlabel("Пол")
plt.ylabel("Сумма заказа RUB")
plt.grid(axis="y", linestyle="--", alpha=0.7)
plt.show()

# 25. ABC-анализ
print("\n🟢 ABC-анализ:")
# группируем данные: считаем общую выручку и количество продаж на товар
sales_by_product = df.groupby("product_id").agg(
    revenue=("total_price", "sum"),  # общая выручка
    total_sold=("quantity", "sum")   # количество проданных товаров
).reset_index()

# сортируем по убыванию выручки
sales_by_product = sales_by_product.sort_values("revenue", ascending=False)

# выводим топ-5 товаров
print(sales_by_product.head())

# считаем кумулятивную выручку
sales_by_product["cum_revenue"] = sales_by_product["revenue"].cumsum()

# считаем долю от общей выручки
total_revenue = sales_by_product["revenue"].sum()
sales_by_product["cum_revenue_pct"] = sales_by_product["cum_revenue"] / total_revenue

def assign_abc_category(cum_pct):
    if cum_pct <= 0.80:
        return "A"  # 80% выручки = топ-20% товаров
    elif cum_pct <= 0.95:
        return "B"  # 15% выручки = средние 30%
    else:
        return "C"  # 5% выручки = последние 50%

sales_by_product["ABC_category"] = sales_by_product["cum_revenue_pct"].apply(assign_abc_category)

# выводим товары группы A (ключевые товары)
print("\n👍 Товары группы A (ключевые):")
print(sales_by_product[sales_by_product["ABC_category"] == "A"])

# выводим товары группы C (маловажные товары)
print("\n👎Товары группы C (маловажные):")
print(sales_by_product[sales_by_product["ABC_category"] == "C"])

# условия для вывода товаров
low_sales_filter = (
    (sales_by_product["revenue"] < 10_000) &
    (sales_by_product["total_sold"] < 20) &
    (sales_by_product["cum_revenue_pct"] < 0.001)
)

# выводим кандидатов на выход из ассортимента
to_remove = sales_by_product[low_sales_filter]
print("\n🔴 Товары, которые можно вывести из ассортимента:")
print(to_remove)

# визуализация ABC
colors = {'A': '#66b3ff', 'B': '#ffcc99', 'C': '#ff6666'}
plt.figure(figsize=(10, 6))
sns.countplot(data=sales_by_product, x="ABC_category", palette=colors.values())
plt.title("Распределение товаров по ABC-категориям")
plt.xlabel("ABC-категория")
plt.ylabel("Количество товаров")
plt.show()

# визуализация кумулятивной выручки
plt.figure(figsize=(12, 6))
sns.lineplot(x=range(len(sales_by_product)), y=sales_by_product["cum_revenue_pct"], color="blue", label="Кумулятивная выручка")
plt.axhline(y=0.80, color="green", linestyle="--", label="Граница A (80%)")
plt.axhline(y=0.95, color="orange", linestyle="--", label="Граница B (95%)")
plt.title("Кумулятивная выручка товаров (ABC-анализ)")
plt.xlabel("Товары (упорядочены по убыванию выручки)")
plt.ylabel("Кумулятивная доля выручки")
plt.legend()
plt.show()

# визуализация товаров на удаление
if not to_remove.empty:
    plt.figure(figsize=(10, 6))
    sns.barplot(y=to_remove["product_id"], x=to_remove["revenue"], color="red")
    plt.title("Товары на удаление: Низкие продажи и выручка")
    plt.xlabel("Выручка")
    plt.ylabel("ID товара")
    plt.show()
