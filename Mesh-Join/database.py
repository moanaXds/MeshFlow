
import csv
from datetime import datetime, timedelta
import mysql.connector
from getpass import getpass


connection = mysql.connector.connect(
    host="127.0.0.1",
    port=3306,
    user="root",
    password=getpass("MySQL password: "),
    database="retail_dw"
)

try:
    cursor = connection.cursor()

    # read customer master data

    with open("../Data/processed/customer_master_data.csv", "r", newline="") as file:
        reader = csv.DictReader(file)

        customers = []

        for row in reader:
            customers.append((
                row["customer_id"].strip(),
                row["gender"].strip() or None,
                row["age_group"].strip() or None,
                row["occupation"].strip() or None,
                row["city_category"].strip() or None,
                row["stay_in_current_city_years"].strip() or None,
                row["marital_status"].strip() or None
            ))

    # insert customers into dimension table
    query = """
        INSERT INTO dim_customer (
            customer_id,
            gender,
            age_group,
            occupation,
            city_category,
            stay_in_current_city_years,
            marital_status
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s)
        ON DUPLICATE KEY UPDATE
            gender = VALUES(gender),
            age_group = VALUES(age_group),
            occupation = VALUES(occupation),
            city_category = VALUES(city_category),
            stay_in_current_city_years = VALUES(stay_in_current_city_years),
            marital_status = VALUES(marital_status)
    """

    

    cursor.executemany(query, customers)
    connection.commit()

    # verify 

    cursor.execute("SELECT COUNT(*) FROM dim_customer")
    total_customers = cursor.fetchone()[0]

    print("\nCustomer dimension populated successfully.")
    print("CSV customer records:", len(customers))
    print("Total customers in dim_customer:", total_customers)





    # read supplier and product master data


    with open("../Data/processed/product_master_data.csv", "r", newline="") as file:
        reader = csv.DictReader(file)

        products = []
        suppliers = {}

        for row in reader:
            product_id = row["product_id"].strip()
            product_name = row["product_name"].strip() or None
            product_category = row["product_category"].strip() or None
            supplier_id = row["supplier_id"].strip()
            supplier_name = row["supplier_name"].strip()

            products.append((
                product_id,
                product_name,
                product_category
            ))

            if supplier_id:
                suppliers[supplier_id] = supplier_name

    # insert suppliers

    supplier_query = """
        INSERT INTO dim_supplier (
            supplier_id,
            supplier_name
        )
        VALUES (%s, %s)
        ON DUPLICATE KEY UPDATE
            supplier_name = VALUES(supplier_name)
    """

    supplier_records = [
        (supplier_id, supplier_name)
        for supplier_id, supplier_name in suppliers.items()
    ]

    cursor.executemany(supplier_query, supplier_records)

    # insert products

    product_query = """
        INSERT INTO dim_product (
            product_id,
            product_name,
            product_category
        )
        VALUES (%s, %s, %s)
        ON DUPLICATE KEY UPDATE
            product_name = VALUES(product_name),
            product_category = VALUES(product_category)
    """

    cursor.executemany(product_query, products)

    connection.commit()

    # verify 

    cursor.execute("SELECT COUNT(*) FROM dim_supplier")
    print("Total suppliers:", cursor.fetchone()[0])

    cursor.execute("SELECT COUNT(*) FROM dim_product")
    print("Total products:", cursor.fetchone()[0])





    # insert static stores ( Online + 4 physical UK stores )

    stores = [
        ("1", "Online", "Web"),
        ("2", "London Bridge", "London"),
        ("3", "Manchester Arndale", "Manchester"),
        ("4", "Birmingham Bullring", "Birmingham"),
        ("5", "Edinburgh Princes", "Edinburgh")
    ]

    store_query = """
        INSERT INTO dim_store (
            store_id,
            store_name,
            city
        )
        VALUES (%s, %s, %s)
        ON DUPLICATE KEY UPDATE
            store_name = VALUES(store_name),
            city = VALUES(city)
    """

    cursor.executemany(store_query, stores)

    # generate and insert time dimension ( from Dec 1, 2010 -> Dec 31, 2011 )

    time_records = []
    current_time = datetime(2010, 12, 1, 0, 0)
    end_time = datetime(2011, 12, 31, 23, 0)
    one_hour = timedelta(hours=1)

    def get_season(m):
        if m in (12, 1, 2):
            return "Winter"
        elif m in (3, 4, 5):
            return "Spring"
        elif m in (6, 7, 8):
            return "Summer"
        return "Autumn"

    while current_time <= end_time:
        month = current_time.month
        time_records.append((
            current_time.strftime("%Y-%m-%d"),
            current_time.hour,
            current_time.day,
            month,
            (month - 1) // 3 + 1,
            current_time.year,
            current_time.strftime("%A"),
            "Weekend" if current_time.weekday() >= 5 else "Weekday",
            get_season(month),
            1 if month <= 6 else 2
        ))
        current_time += one_hour

    time_query = """
        INSERT INTO dim_time (
            full_date,
            hour,
            day,
            month,
            quarter,
            year,
            day_of_week,
            weekday_weekend,
            season,
            half_year
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON DUPLICATE KEY UPDATE
            day = VALUES(day),
            month = VALUES(month),
            quarter = VALUES(quarter),
            year = VALUES(year),
            day_of_week = VALUES(day_of_week),
            weekday_weekend = VALUES(weekday_weekend),
            season = VALUES(season),
            half_year = VALUES(half_year)
    """

    cursor.executemany(time_query, time_records)

    connection.commit()

    # verify 

    cursor.execute("SELECT COUNT(*) FROM dim_store")
    print("Total stores:", cursor.fetchone()[0])

    cursor.execute("SELECT COUNT(*) FROM dim_time")
    print("Total time records:", cursor.fetchone()[0])



except Exception as error:
    connection.rollback()
    print("Error:", error)

finally:
    if connection.is_connected():
        cursor.close()
        connection.close()




