
import time
from collections import deque
from pathlib import Path
from getpass import getpass
import mysql.connector

from meshjoin import MeshJoin
from threads import (
    StreamProducerThread,
    PeriodicExtractorThread,
    MeshJoinWorkerThread,
    buffer_lock
)


#           SURROGATE KEY CACHES FOR FAST FACT INSERTION


def load_dimension_keys(cursor):
    print("\n--- LOADING DIMENSION KEYS FROM DATABASE ---")

    # customer key cache: customer_id -> customer_key

    cursor.execute("SELECT customer_id, customer_key FROM dim_customer")
    customer_keys = {str(row[0]): row[1] for row in cursor.fetchall()}

    # product key cache: product_id -> product_key

    cursor.execute("SELECT product_id, product_key FROM dim_product")
    product_keys = {str(row[0]): row[1] for row in cursor.fetchall()}

    # supplier key cache: supplier_id -> supplier_key
    cursor.execute("SELECT supplier_id, supplier_key FROM dim_supplier")
    supplier_keys = {str(row[0]): row[1] for row in cursor.fetchall()}

    # store key cache: store_id -> store_key
    cursor.execute("SELECT store_id, store_key FROM dim_store")
    store_keys = {str(row[0]): row[1] for row in cursor.fetchall()}

    # time key cache: (full_date_str, hour) -> date_key
    cursor.execute("SELECT date_format(full_date, '%Y-%m-%d'), hour, date_key FROM dim_time")
    time_keys = {(row[0], int(row[1])): row[2] for row in cursor.fetchall()}

    print(f"Loaded: {len(customer_keys)} customers, {len(product_keys)} products, {len(supplier_keys)} suppliers, {len(store_keys)} stores, {len(time_keys)} time slots.")

    return customer_keys, product_keys, supplier_keys, store_keys, time_keys



#           BATCH INSERT FACT RECORDS INTO TRANSACTION_FACT



def insert_fact_batch(cursor, connection, batch):
    if not batch:
        return 0

    query = """
        INSERT INTO transaction_fact (
            transaction_id,
            transaction_line_number,
            customer_key,
            product_key,
            supplier_key,
            store_key,
            date_key,
            quantity,
            unit_price,
            revenue,
            sales_mode,
            processing_mode
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON DUPLICATE KEY UPDATE
            revenue = VALUES(revenue)
    """

    cursor.executemany(query, batch)
    connection.commit()
    return len(batch)



#           MAIN PIPELINE EXECUTION



def main():
    print("\n   RETAIL DATA WAREHOUSE - MESHJOIN ETL   \n")

    # database connection
    password = getpass("MySQL password: ")

    connection = mysql.connector.connect(
        host="127.0.0.1",
        port=3306,
        user="root",
        password=password,
        database="retail_dw"
    )

    cursor = connection.cursor()

    # load dimension surrogate keys into memory
    customer_keys, product_keys, supplier_keys, store_keys, time_keys = load_dimension_keys(cursor)

    # file paths
    base_dir = Path(__file__).parent.parent / "Data" / "processed"
    customer_csv = base_dir / "customer_master_data.csv"
    product_csv = base_dir / "product_master_data.csv"
    stream_csv = base_dir / "online_sales_stream.csv"
    periodic_csv = base_dir / "periodic_sales_staging.csv"

    # initialize MESHJOIN operator
    print("\n    INITIALIZING MESHJOIN OPERATOR ")
    mj = MeshJoin(customer_csv, product_csv, chunk_size=500, stream_buffer_size=1000)

  

    enriched_output = deque()

    # create threads

    stream_thread = StreamProducerThread(stream_csv, mj, batch_rate=200, sleep_time=0.005)
    periodic_thread = PeriodicExtractorThread(periodic_csv, mj, interval_sec=0.02, total_hours=48)
    worker_thread = MeshJoinWorkerThread(mj, enriched_output, sleep_time=0.001)

    print("\n    STARTING ETL THREADS ")
    start_time = time.time()

   
    worker_thread.start()
    stream_thread.start()
    periodic_thread.start()

    # fact insertion loop while threads are running

    fact_batch = []
    total_facts_inserted = 0
    line_counters = {}

    while stream_thread.is_alive() or periodic_thread.is_alive() or enriched_output or not mj.is_finished():
       
        # pop records from enriched output queue
       
        while enriched_output:
            tx = enriched_output.popleft()

            tx_id = str(tx.get("transaction_id")).strip()
            cust_id = str(tx.get("customer_id")).strip()
            prod_id = str(tx.get("product_id")).strip()
            supp_id = str(tx.get("supplier_id")).strip()
            store_id = str(tx.get("store_id")).strip()

            # look up keys

            cust_key = customer_keys.get(cust_id)
            prod_key = product_keys.get(prod_id)
            supp_key = supplier_keys.get(supp_id)
            st_key = store_keys.get(store_id)

            # parse event_time to find date_key
            # event_time format: YYYY-MM-DD HH:MM:SS
            
            raw_time = str(tx.get("event_time", "")).strip()
            date_part = raw_time[:10] if len(raw_time) >= 10 else "2011-01-01"
            hour_part = int(raw_time[11:13]) if len(raw_time) >= 13 else 0

            dt_key = time_keys.get((date_part, hour_part), 1)

            # line number within transaction

            line_counters[tx_id] = line_counters.get(tx_id, 0) + 1
            line_no = line_counters[tx_id]

            # sales metrics

            quantity = int(tx.get("quantity", 1))
            unit_price = float(tx.get("unit_price", 0.0))
            revenue = round(float(tx.get("sale", quantity * unit_price)), 2)

            sales_mode = tx.get("sales_mode", "online")
            processing_mode = "meshjoin"

            if cust_key and prod_key and supp_key and st_key and dt_key:
                fact_record = (
                    tx_id,
                    line_no,
                    cust_key,
                    prod_key,
                    supp_key,
                    st_key,
                    dt_key,
                    quantity,
                    unit_price,
                    revenue,
                    sales_mode,
                    processing_mode
                )
                fact_batch.append(fact_record)

            # insert batch when 500 records are ready

            if len(fact_batch) >= 500:
                inserted = insert_fact_batch(cursor, connection, fact_batch)
                total_facts_inserted += inserted
                fact_batch = []

        time.sleep(0.01)

    # insert remaining fact records
    
    if fact_batch:
        inserted = insert_fact_batch(cursor, connection, fact_batch)
        total_facts_inserted += inserted

  
    stream_thread.join()
    periodic_thread.join()

    worker_thread.stop()
    worker_thread.join()

    elapsed_time = time.time() - start_time


    print("\n          ETL EXECUTION SUMMARY           \n")
    print(f"Total time elapsed: {elapsed_time:.2f} seconds")
    print(f"Online stream transactions pushed: {stream_thread.total_stream_pushed}")
    print(f"Periodic transactions pushed: {periodic_thread.total_periodic_pushed}")
    print(f"Periodic transactions shed: {periodic_thread.total_shed}")
    print(f"Periodic transactions recovered: {periodic_thread.total_recovered}")
    print(f"Total transactions enriched by MESHJOIN: {worker_thread.total_output}")
    print(f"Total fact records inserted into DW: {total_facts_inserted}")


    cursor.close()
    connection.close()


if __name__ == "__main__":
    main()

