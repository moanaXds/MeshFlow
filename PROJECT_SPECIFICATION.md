# DS3003 & DS3004: Data Warehousing & Business Intelligence
## Practical Project Specification Reference (Fall 2026)
**Institution:** National University of Computer and Emerging Sciences (FAST–NUCES)  
**Topic:** Building and Analysing a Near-Real-Time and Periodic Data Warehouse for a Retail Chain  

---

## 1. Project Architecture Overview

```
                      Raw Transactional Data
                      (Online Retail.xlsx)
                                │
                                ▼
                       Data Cleaning Step
         (Quantity > 0, UnitPrice > 0, CustomerID not null, Drop 'C' cancellations)
                                │
        ┌───────────────────────┴────────────────────────┐
        ▼                                                ▼
   Online Stream                               Periodic Sales Data
(70% of transactions)                       (30% of transactions)
  online_sales_stream.csv                     48 hourly groups (staging table)
        │                                                │
        ▼ (OnlineThread)                                 ▼ (PeriodicThread - every 60s)
  Stream Buffer (max: 1000) ◄────────────────────────────┘
        │
        ▼ (If overloaded in peak mode) ──► Shed Table (disk)
   MESHJOIN Thread                                │ (processed in non-peak)
  (Queue + Hash Table H + Disk Buffer: 500) ◄─────┘
        │
        ├── Probes Customer Master Data (partitions of 500 from DB/disk)
        ├── Probes Product Master Data (partitions of 500 from DB/disk)
        └── Enriches Store (in-memory lookup of 5 stores)
        │
        ▼
   Consolidation Step
  (revenue = quantity * unit_price, surrogate dimension keys, sales_mode, processing_mode)
        │
        ▼
   Data Warehouse (retail_dw)
  (transaction_fact + dim_customer, dim_product, dim_supplier, dim_store, dim_time)
        │
        ▼
   20 OLAP Analytical Queries (including STORE_QUARTERLY_SALES view)
```

---

## 2. Master Data Generation & Exact Rules

### 2.1 The `stable(text, n)` Function
All synthetic attributes use deterministic MD5 hashing:
```python
def stable(text, n):
    text = str(text)
    value = hashlib.md5(text.encode("utf-8")).hexdigest()[:8]
    return int(value, 16) % n
```

### 2.2 Online vs. Periodic Classification (Cart / Invoice Level)
* Must be applied to `transaction_id` (InvoiceNo), so all items in one cart share the same mode:
  ```python
  stable(transaction_id, 10)
  ```
  * `0, 1, 2, 3, 4, 5, 6` $\rightarrow$ **Online** (~70%)
  * `7, 8, 9` $\rightarrow$ **Periodic** (~30%)

### 2.3 Store Rules (Exactly 5 Stores)
* **Rule:** Use these 5 stores and **no others**:
  1. `1`: `Online`, City: `Web`
  2. `2`: `London Bridge`, City: `London`
  3. `3`: `Manchester Arndale`, City: `Manchester`
  4. `4`: `Birmingham Bullring`, City: `Birmingham`
  5. `5`: `Edinburgh Princes`, City: `Edinburgh`
* **Assignment Logic:**
  * Online cart $\rightarrow$ `store_id = 1`
  * Physical store cart $\rightarrow$ `store_id = 2 + (int(MD5(transaction_id)[:8], 16) % 4)` (i.e. `2 + stable(transaction_id, 4)`)
* **Enrichment:**
  * Store list stored in memory; enrichment performed outside MESHJOIN.

### 2.4 Customer Master Data (`dim_customer`)
* `customer_id`: cleaned CustomerID
* `gender`: `stable(customer_id, 2)` $\rightarrow$ `["Male", "Female"]`
* `age_group`: `stable(customer_id + "age", 7)` $\rightarrow$ `["0-17", "18-25", "26-35", "36-45", "46-50", "51-55", "55+"]`
* `occupation`: `stable(customer_id + "occ", 10)` $\rightarrow$ `["Student", "Engineer", "Teacher", "Executive", "Clerk", "Healthcare", "Retail", "Homemaker", "Artist", "Other"]`
* `city_category`: `stable(customer_id + country, 3)` $\rightarrow$ `["A", "B", "C"]` (using first transaction country for that customer)
* `stay_in_current_city_years`: `stable(customer_id + "stay", 5)` $\rightarrow$ `["0", "1", "2", "3", "4+"]`
* `marital_status`: `stable(customer_id + "mar", 2)` $\rightarrow$ `["Single", "Married"]`

### 2.5 Product Master Data (`dim_product` & `dim_supplier`)
* `product_id`: StockCode
* `product_name`: longest `Description` for that StockCode; if empty, `"UNKNOWN"`
* `product_category`: `stable(product_id + "cat", 10)` $\rightarrow$ `["Home Decor", "Kitchen", "Stationery", "Toys", "Seasonal Gifts", "Bags", "Jewelry", "Garden", "Party", "Other"]`
* `supplier_id` & `supplier_name`: `stable(product_id, 5)`:
  1. `SUP01`: `Northwind Supplies`
  2. `SUP02`: `Atlantic Gifts Ltd`
  3. `SUP03`: `Midlands Wholesale`
  4. `SUP04`: `Celtic Craft Co`
  5. `SUP05`: `EuroPack Partners`

### 2.6 Time Dimension (`dim_time`)
Derived from `event_time` (InvoiceDate):
* `full_date`: `YYYY-MM-DD`
* `hour`: `0`–`23`
* `day`: Day of month (`1`–`31`)
* `month`: Month (`1`–`12`)
* `quarter`: Calendar quarter (`1`–`4`)
* `year`: `2010`, `2011`
* `day_of_week`: `Monday`–`Sunday`
* `weekday_weekend`: `Weekday` (Mon–Fri) or `Weekend` (Sat–Sun)
* `season`:
  * Winter: December, January, February
  * Spring: March, April, May
  * Summer: June, July, August
  * Fall / Autumn: September, October, November
* `half_year`: `1` (Jan–Jun) or `2` (Jul–Dec)

---

## 3. Data Warehouse Star Schema

```
           ┌────────────────────────┐
           │      dim_customer      │
           ├────────────────────────┤
           │ customer_key (PK)      │◄────────┐
           │ customer_id (Unique)   │         │
           │ gender                 │         │
           │ age_group              │         │
           │ occupation             │         │
           │ city_category          │         │
           │ stay_years             │         │
           │ marital_status         │         │
           └────────────────────────┘         │
                                              │
           ┌────────────────────────┐         │
           │      dim_product       │         │
           ├────────────────────────┤         │
           │ product_key (PK)       │◄──────┐ │
           │ product_id (Unique)    │       │ │
           │ product_name           │       │ │
           │ product_category       │       │ │
           └────────────────────────┘       │ │
                                            │ │
           ┌────────────────────────┐       │ │
           │      dim_supplier      │       │ │
           ├────────────────────────┤       │ │
           │ supplier_key (PK)      │◄────┐ │ │
           │ supplier_id (Unique)   │     │ │ │
           │ supplier_name          │     │ │ │
           └────────────────────────┘     │ │ │
                                          │ │ │
                                ┌─────────┴─┴─┴──────────┐
                                │    transaction_fact    │
                                ├────────────────────────┤
                                │ fact_key (PK)          │
                                │ transaction_id         │
                                │ transaction_line_num   │
                                │ customer_key (FK)      │
                                │ product_key (FK)       │
                                │ supplier_key (FK)      │
                                │ store_key (FK)         │
                                │ date_key (FK)          │
                                │ quantity               │
                                │ unit_price             │
                                │ revenue                │
                                │ sales_mode             │
                                │ processing_mode        │
                                └─────────┬─┬────────────┘
                                          │ │
           ┌────────────────────────┐     │ │
           │       dim_store        │     │ │
           ├────────────────────────┤     │ │
           │ store_key (PK)         │◄────┘ │
           │ store_id (Unique: 1-5) │       │
           │ store_name             │       │
           │ city                   │       │
           └────────────────────────┘       │
                                            │
           ┌────────────────────────┐       │
           │        dim_time        │       │
           ├────────────────────────┤       │
           │ date_key (PK)          │◄──────┘
           │ full_date              │
           │ hour                   │
           │ day, month, quarter    │
           │ year, half_year        │
           │ day_of_week            │
           │ weekday_weekend        │
           │ season                 │
           └────────────────────────┘
```

---

## 4. MESHJOIN Algorithm & Settings

### 4.1 Key Parameters & Buffer Sizes
* **Master Data partition ($R_i$):** 500 tuples loaded into memory per step from disk/database.
* **Stream chunk size ($w$):** 500 transactions moved from stream buffer into Hash Table $H$ per iteration.
* **Stream buffer capacity:** 1,000 transactions.
* **Queue:** Maintains arrival order of chunks. Each chunk remains in memory for **one full cycle** of the Master Data partitions.
* **Hash table $H$:** Multi-map indexed on join key (`customer_id` for customer pass, `product_id` for product pass).
* **Speed:**
  * **Peak mode:** 80 transactions / sec.
  * **Non-peak mode:** 10 transactions / sec.
* **Time scale:** 1 simulated hour = 60 real seconds.
* **Periodic groups:** 48 groups sorted chronologically by `event_time`.

### 4.2 Multi-Threading Model
1. **`OnlineThread`**: Reads `online_sales_stream.csv` continuously and pushes transactions into the stream buffer.
2. **`PeriodicThread`**: Activates every 60 seconds (1 simulated hour), extracts the next group (from 0 to 47) from the staging table, and feeds it into the stream buffer.
3. **`MeshJoinThread`**: Executes the 4-step MESHJOIN loop:
   * Step 1: Read next partition of Master Data from disk into disk buffer.
   * Step 2: Read up to 500 transactions from stream buffer into $H$; record chunk pointer in Queue.
   * Step 3: Probe Master Data partition against $H$ and enrich matched records.
   * Step 4: Retire chunk when it has completed cycles against both master data tables.

### 4.3 Load Shedding & Priority Rules
* **Priority:** Online $\rightarrow$ Periodic $\rightarrow$ Shed transactions.
* When stream buffer overflows during peak arrival, shed periodic transactions to disk (`Shed_table`).
* When arrival rate slows down (non-peak mode), read shed transactions from disk back into stream buffer.

### 4.4 Interactive Command-Line Controls
While the application is running, it must accept these commands:
* `online` $\rightarrow$ Process online stream only; pause periodic extractions.
* `periodic` $\rightarrow$ Process periodic staging only; pause online stream.
* `both` $\rightarrow$ Process both forms (online prioritized).
* `peak` $\rightarrow$ Set speed to 80 txn/sec (shedding allowed).
* `non-peak` $\rightarrow$ Set speed to 10 txn/sec (process shed transactions).
* `status` $\rightarrow$ Display current mode, speed, stream buffer count, $H$ count, and shed disk count.
* `quit` $\rightarrow$ Gracefully stop threads, flush DW inserts, and exit.

---

## 5. Required Deliverables & Folder Structure

```
<StudentName_StudentID_Project>/
├── Create-DW/
│   └── create_dw.sql           # Schema definition (DDL + drop scripts + view)
├── Mesh-Join/
│   ├── main.py                 # CLI controller & thread orchestrator
│   ├── data_preparation.py     # Cleaning & master data CSV generator
│   ├── database.py             # DB connection, schema execution, dim loader
│   ├── meshjoin.py             # MESHJOIN algorithm (H, Queue, Disk Buffer)
│   ├── threads.py              # OnlineThread, PeriodicThread, MeshJoinThread
│   └── requirements.txt        # Python dependencies
├── Queries-DW/
│   └── analysis_queries.sql    # 20 OLAP queries & STORE_QUARTERLY_SALES
├── Data/
│   ├── raw/
│   │   └── Online Retail.xlsx
│   ├── processed/
│   │   ├── online_sales_stream.csv
│   │   ├── customer_master_data.csv
│   │   └── product_master_data.csv
│   └── README.md               # Dataset source, rules, cleaning counts, date
├── Project-Report/
│   └── project_report.pdf      # Complete academic report
└── README.md                   # Instructions to run the whole project
```

---

## 6. The 20 Required OLAP Queries Summary
* **Q1:** Top 5 revenue products on weekdays & weekends with monthly breakdown.
* **Q2:** Customer demographics (gender, age) by purchase amount with city category.
* **Q3:** Product category sales by customer occupation.
* **Q4:** Purchase trends by gender and age group across quarters.
* **Q5:** Top 5 occupations by product category sales.
* **Q6:** City category performance by marital status over latest 6 months.
* **Q7:** Average purchase amount by city stay duration and gender.
* **Q8:** Top revenue city categories (A, B, C) by product category.
* **Q9:** Month-over-month sales growth % by product category.
* **Q10:** Weekend vs. weekday sales comparison by age group.
* **Q11:** Top 5 revenue products on weekdays vs weekends grouped by month.
* **Q12:** Store revenue growth rate quarter-over-quarter.
* **Q13:** Supplier sales contribution by store and product name.
* **Q14:** Seasonal analysis (Spring, Summer, Fall, Winter) of product sales.
* **Q15:** Month-to-month revenue volatility % by store and supplier.
* **Q16:** Top 5 product pairs purchased together in the same cart (product bundling).
* **Q17:** Yearly revenue by store, supplier, and product using `ROLLUP`.
* **Q18:** Revenue & volume by product for H1 vs H2 vs full year.
* **Q19:** High revenue spikes (> 2x daily average for that product).
* **Q20:** View `STORE_QUARTERLY_SALES` (ordered by store name and quarter).

