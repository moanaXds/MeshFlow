# Dataset and Master Data Documentation

This folder contains the raw and processed datasets used in this project.

## Raw Dataset
- **File:** `Data/raw/Online Retail.xlsx`
- **Source:** Online Retail dataset containing transactions between 01/12/2010 and 09/12/2011 for a UK-based non-store online retail.
- **Initial Total Records:** 541,909 rows

## Data Cleaning Rules
Cleaning is handled programmatically in `Mesh-Join/data_preparation.py`. A transaction is kept only if:
1. `Quantity > 0`: Filters out cancellations, inventory write-offs, and damaged products.
2. `UnitPrice > 0`: Filters out zero-price adjustments, postage notes, and manual discounts.
3. `CustomerID is not null`: Filters out anonymous guest checkouts.
4. `InvoiceNo` does not start with `C`: Filters out cancelled invoices.

### Cleaning Statistics
- Transactions before cleaning: 541,909
- Transactions after cleaning: 397,884
- Transactions removed: 144,025
- Output file: `Data/processed/cleaned_transactions.csv`

## Master Data Generation (Deterministic MD5)
Because the raw data does not include customer demographics or full supplier tables, master data was generated using an MD5 hash function (`stable(text, n)`). No random values were used, so running the script produces the exact same data every time.

### 1. Customer Master Data (`Data/processed/customer_master_data.csv`)
- **Total Rows:** 4,339 unique customers
- **Attributes:**
  - `customer_id`: Unique customer ID
  - `gender`: Male, Female
  - `age_group`: 0-17, 18-25, 26-35, 36-45, 46-50, 51-55, 55+
  - `occupation`: 10 occupations (Student, Engineer, Teacher, Executive, Clerk, Healthcare, Retail, Homemaker, Artist, Other)
  - `city_category`: A, B, C
  - `stay_in_current_city_years`: 0, 1, 2, 3, 4+
  - `marital_status`: Single, Married

### 2. Product Master Data (`Data/processed/product_master_data.csv`)
- **Total Rows:** 3,666 unique products
- **Attributes:**
  - `product_id`: StockCode
  - `product_name`: Cleanest and longest description from the raw dataset
  - `product_category`: 10 categories (Home Decor, Kitchen, Stationery, Toys, Seasonal Gifts, Bags, Jewelry, Garden, Party, Other)
  - `supplier_id`: SUP01 to SUP05
  - `supplier_name`: Northwind Supplies, Atlantic Gifts Ltd, Midlands Wholesale, Celtic Craft Co, EuroPack Partners

## Sales Streams
Using `stable(transaction_id, 10)`:
1. **Online Sales Stream (`Data/processed/online_sales_stream.csv`)**:
   - Condition: Hash mod 10 in [0, 6] (70% of transactions)
   - Total records: 278,740
   - Mode: Near real-time stream
2. **Periodic Sales Staging (`Data/processed/periodic_sales_staging.csv`)**:
   - Condition: Hash mod 10 in [7, 9] (30% of transactions)
   - Total records: 119,144
   - Sorted chronologically by `event_time` and divided into 48 release periods.

