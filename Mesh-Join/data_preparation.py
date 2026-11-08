
import pandas as pd
from pathlib import Path


file_path = Path(__file__).parent.parent / "Data" / "raw" / "Online Retail.xlsx"

dataset = pd.read_excel(file_path)


print("\n        DATASET INFORMATION        ")



print("Total rows:", len(dataset))
print("Total columns:", len(dataset.columns))


print("\n--- COLUMN NAMES ---")
print(dataset.columns.tolist())


print("\n--- FIRST FIVE RECORDS ---")
print(dataset.head())


print("\n--- DATA TYPES ---")
print(dataset.dtypes)


print("\n--- MISSING VALUES ---")
print(dataset.isnull().sum())


print("\n--- DUPLICATE RECORDS ---")
print("Duplicates:", dataset.duplicated().sum())

#            Date range

print("\n--- DATE RANGE ---")
print("First date:", dataset["InvoiceDate"].min())
print("Last date:", dataset["InvoiceDate"].max())

#            Quantity and price inspection

print("\n--- QUANTITY AND PRICE ---")
print("Zero or negative quantities:", (dataset["Quantity"] <= 0).sum())
print("Zero or negative prices:", (dataset["UnitPrice"] <= 0).sum())

#            Cancellation inspection

print("\n--- CANCELLATIONS ---")
print("Cancellation records:", dataset["InvoiceNo"].astype(str).str.startswith("C").sum())

print("\nDataset inspection completed.")



#           DATA CLEANING 



print("\n            DATA CLEANING ")

before_cleaning = len(dataset)

#           keep only the not cancelled invoices

cleaned_data = dataset[
    ~dataset["InvoiceNo"].astype(str).str.startswith("C")
]

#           keep only the valid quantities

cleaned_data = cleaned_data[ cleaned_data["Quantity"] > 0 ]

#           keep only the valid prices

cleaned_data = cleaned_data[
    cleaned_data["UnitPrice"] > 0
]

#           keep only customer with IDs

cleaned_data = cleaned_data[
    cleaned_data["CustomerID"].notna()
]

after_cleaning = len(cleaned_data)

print("\nTransactions before cleaning :", before_cleaning)
print("Transactions after cleaning :", after_cleaning)
print("Transactions removed :", before_cleaning - after_cleaning)


#           Save cleaned data  

output_path = Path(__file__).parent.parent / "Data" / "processed" / "cleaned_transactions.csv"

output_path.parent.mkdir(parents=True, exist_ok=True)

cleaned_data.to_csv(output_path, index=False)

print("\nCleaned data saved successfully!")
print("File location:", output_path)





#           QUESTION 3: GENERATE MASTER DATA





import hashlib

print("\n--- GENERATING MASTER DATA ---")


#           same text = same hash


def stable(text, n):
    text = str(text)

    value = hashlib.md5(text.encode("utf-8")).hexdigest()[:8]

    return int(value, 16) % n



#       CUSTOMER MASTER DATA [ add detail =  unique customer ]


genders = ["Male", "Female"]

ages = ["0-17", "18-25", "26-35", "36-45",
        "46-50", "51-55", "55+"]

occupations = [
    "Student", "Engineer", "Teacher", "Executive",
    "Clerk", "Healthcare", "Retail", "Homemaker",
    "Artist", "Other"
]

city_categories = ["A", "B", "C"]

stay_years = ["0", "1", "2", "3", "4+"]

marital_status = ["Single", "Married"]



#       remove duplicate customers by keep first one


customers = cleaned_data.drop_duplicates(
    subset="CustomerID", keep="first"
)[["CustomerID", "Country"]].copy()



customer_master = []

for index, row in customers.iterrows():

    customer_id = str(int(row["CustomerID"]))
    country = str(row["Country"])

    customer = {
        "customer_id": customer_id,
        "gender": genders[stable(customer_id, 2)],
        "age_group": ages[stable(customer_id + "age", 7)],
        "occupation": occupations[stable(customer_id + "occ", 10)],
        "city_category": city_categories[
            stable(customer_id + country, 3)
        ],
        "stay_in_current_city_years": stay_years[
            stable(customer_id + "stay", 5)
        ],
        "marital_status": marital_status[
            stable(customer_id + "mar", 2)
        ]
    }


    customer_master.append(customer)


customer_master = pd.DataFrame(customer_master)




#                   PRODUCT MASTER DATA 


categories = [
    "Home Decor", "Kitchen", "Stationery", "Toys",
    "Seasonal Gifts", "Bags", "Jewelry", "Garden",
    "Party", "Other"
]

suppliers = [
    ("SUP01", "Northwind Supplies"),
    ("SUP02", "Atlantic Gifts Ltd"),
    ("SUP03", "Midlands Wholesale"),
    ("SUP04", "Celtic Craft Co"),
    ("SUP05", "EuroPack Partners")
]


# Replace missing descriptions =  empty text temporarily

products = cleaned_data[["StockCode", "Description"]].copy()

products["Description"] = products["Description"].fillna("").astype(str).str.strip()

# Find the longest description for each product

products["description_length"] = products["Description"].str.len()

products = products.sort_values( "description_length", ascending=False, kind="stable")

products = products.drop_duplicates( subset="StockCode", keep="first")



#        insert details of each product 


product_master = []

for index, row in products.iterrows():

    product_id = str(row["StockCode"])
    product_name = row["Description"]

    if product_name == "":
        product_name = "UNKNOWN"

    supplier_number = stable(product_id, 5)

    supplier_id, supplier_name = suppliers[supplier_number]

    product = {
        "product_id": product_id,
        "product_name": product_name,
        "product_category": categories[
            stable(product_id + "cat", 10)
        ],
        "supplier_id": supplier_id,
        "supplier_name": supplier_name
    }

    product_master.append(product)


product_master = pd.DataFrame(product_master)


#           SAVE BOTH MASTER DATA FILES

data_folder = Path(__file__).parent.parent / "Data" / "processed"

customer_master.to_csv(
    data_folder / "customer_master_data.csv", index=False
)

product_master.to_csv(
    data_folder / "product_master_data.csv", index=False
)




print("\nCustomer Master Data generated:", len(customer_master))
print("Product Master Data generated:", len(product_master))

print("\nCustomer Master Data sample:")
print(customer_master.head())

print("\nProduct Master Data sample:")
print(product_master.head())

print("\nMaster Data files saved successfully!")






#          GENERATE ONLINE + PERIODIC SALES STREAMS




print("\n      GENERATING SALES STREAMS   ")


#           Select columns from cleaned_transactions

sales_data = cleaned_data[
    ["InvoiceNo", "CustomerID", "StockCode",
     "Quantity", "UnitPrice", "InvoiceDate"]
].copy()


#           Rename columns


sales_data = sales_data.rename(columns={
    "InvoiceNo": "transaction_id",
    "CustomerID": "customer_id",
    "StockCode": "product_id",
    "Quantity": "quantity",
    "InvoiceDate": "event_time",
    "UnitPrice": "unit_price"
})

#           Convert IDs -> strings (text)

sales_data["transaction_id"] = sales_data["transaction_id"].astype(str)
sales_data["customer_id"] = sales_data["customer_id"].astype(int).astype(str)
sales_data["product_id"] = sales_data["product_id"].astype(str)

#          if transaction ID = 0–6 -> Online   &   7–9 -> Periodic

sales_data["sales_mode"] = sales_data["transaction_id"].apply(
    lambda x: "online" if stable(x, 10) < 7 else "periodic"
)



#           decide online + periodic


online_data = sales_data[
    sales_data["sales_mode"] == "online"
].copy()


periodic_data = sales_data[
    sales_data["sales_mode"] == "periodic"
].copy()

#            Define online columns

online_columns = [
    "transaction_id",
    "customer_id",
    "product_id",
    "quantity",
    "unit_price",
    "event_time",
    "sales_mode"
]

online_data = online_data[online_columns]

online_data.to_csv(
    data_folder / "online_sales_stream.csv",
    index=False
)

#           Sort periodic transactions in chronological order

periodic_data = periodic_data.sort_values(
    "event_time", kind="stable"
).reset_index(drop=True)

#           Divide periodic transactions into 48 cz 48 half hour periods = 24 hours
#           119,144 / 48 = 2482 per hour

total_periodic = len(periodic_data)

periodic_data["release_hour"] = [
    (i * 48) // total_periodic
    for i in range(total_periodic)
]

#           add new col as = released or not

periodic_data["released"] = False

#            Define periodic columns

periodic_columns = [
    "transaction_id",
    "customer_id",
    "product_id",
    "quantity",
    "unit_price",
    "event_time",
    "sales_mode",
    "release_hour",
    "released"
]

periodic_data = periodic_data[periodic_columns]

#           Save periodic staging data

periodic_data.to_csv(
    data_folder / "periodic_sales_staging.csv",
    index=False
)


print("\nOnline transactions:", len(online_data))
print("Periodic transactions:", len(periodic_data))

print("\nPeriodic groups:")
print(periodic_data["release_hour"].value_counts().sort_index())

print("\nSales streams generated successfully!")
