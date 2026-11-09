
import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from pathlib import Path
import mysql.connector
from mysql.connector import Error


#           PAGE CONFIG


st.set_page_config(
    page_title="Retail DW Dashboard",
    page_icon="🛒",
    layout="wide",
    initial_sidebar_state="expanded"
)


#           DATA PATHS


base_dir = Path(__file__).parent / "Data" / "processed"
customer_csv = base_dir / "customer_master_data.csv"
product_csv = base_dir / "product_master_data.csv"
online_csv = base_dir / "online_sales_stream.csv"
periodic_csv = base_dir / "periodic_sales_staging.csv"



#           DATABASE CONNECTION


@st.cache_resource
def get_connection(host, port, user, password, database):
    try:
        conn = mysql.connector.connect(
            host=host,
            port=int(port),
            user=user,
            password=password,
            database=database
        )
        return conn
    except Error as e:
        return None


@st.cache_data(ttl=120)
def run_query(_conn, query):
    try:
        return pd.read_sql(query, _conn)
    except Exception as e:
        return pd.DataFrame()



#           LOAD CSV DATA (cached)


@st.cache_data
def load_csv(path):
    return pd.read_csv(path)



#           SIDEBAR


st.sidebar.title("Retail DW Dashboard")
st.sidebar.markdown("DS3003 & DS3004 — Fall 2026")
st.sidebar.divider()

page = st.sidebar.radio(
    "Navigate",
    [
        "Overview",
        "Data Cleaning",
        "MESHJOIN Pipeline",
        "Customer Analysis",
        "Product Analysis",
        "Store & Supplier",
        "OLAP Queries"
    ]
)

st.sidebar.divider()
st.sidebar.subheader("MySQL Connection")
db_host = st.sidebar.text_input("Host", value="127.0.0.1")
db_port = st.sidebar.text_input("Port", value="3306")
db_user = st.sidebar.text_input("User", value="root")
db_pass = st.sidebar.text_input("Password", type="password")
db_name = st.sidebar.text_input("Database", value="retail_dw")

conn = None
if db_pass:
    conn = get_connection(db_host, db_port, db_user, db_pass, db_name)
    if conn and conn.is_connected():
        st.sidebar.success("Connected to MySQL")
    else:
        st.sidebar.error("Connection failed. Check credentials.")
else:
    st.sidebar.info("Enter MySQL password to load live DW data.")



# ==================================================================
#           PAGE 1: OVERVIEW
# ==================================================================

if page == "Overview":
    st.title("Retail Data Warehouse — Project Overview")
    st.caption("DS3003 & DS3004 | FAST-NUCES | Building and Analysing a Near-Real-Time and Periodic DW")

    st.divider()

    # pipeline stats from ETL run
    col1, col2, col3, col4 = st.columns(4)
    col1.metric("Total Cleaned Transactions", "397,884")
    col2.metric("Online Stream Transactions", "278,740")
    col3.metric("Periodic Transactions", "119,144")
    col4.metric("ETL Execution Time", "~10 sec")

    st.divider()

    col1, col2, col3, col4 = st.columns(4)
    col1.metric("Unique Customers", "4,338")
    col2.metric("Unique Products", "3,665")
    col3.metric("Suppliers", "5")
    col4.metric("Physical Stores", "5")

    st.divider()

    # pipeline overview diagram
    st.subheader("Pipeline Architecture")

    pipeline_steps = {
        "Step": [
            "Raw Dataset",
            "Data Cleaning",
            "Master Data Generation",
            "Stream Split",
            "MESHJOIN Enrichment",
            "Fact Table Loading",
            "OLAP Queries"
        ],
        "Description": [
            "Online Retail.xlsx (541,909 raw transactions)",
            "Drop cancellations, negatives, missing customers",
            "Customer (4,338) and Product (3,665) Master Data via MD5 hash",
            "70% Online Stream + 30% Periodic Staging (48 hourly groups)",
            "3-thread MESHJOIN with stream buffer, queue, hash table H",
            "397,884 enriched records into transaction_fact (MySQL)",
            "20 OLAP queries + STORE_QUARTERLY_SALES view"
        ],
        "Output": [
            "541,909 rows",
            "397,884 rows",
            "2 CSV files",
            "2 stream files",
            "397,884 enriched tuples",
            "transaction_fact",
            "query_results.txt"
        ]
    }

    st.dataframe(pd.DataFrame(pipeline_steps), use_container_width=True, hide_index=True)

    # DW stats from MySQL if connected
    if conn and conn.is_connected():
        st.divider()
        st.subheader("Live Data Warehouse Statistics")
        col1, col2, col3, col4, col5 = st.columns(5)
        for col, table, label in zip(
            [col1, col2, col3, col4, col5],
            ["transaction_fact", "dim_customer", "dim_product", "dim_store", "dim_time"],
            ["Fact Records", "Customers", "Products", "Stores", "Time Slots"]
        ):
            df = run_query(conn, f"SELECT COUNT(*) as cnt FROM {table}")
            count = df["cnt"][0] if not df.empty else "N/A"
            col.metric(label, f"{count:,}" if isinstance(count, int) else count)



# ==================================================================
#           PAGE 2: DATA CLEANING
# ==================================================================

elif page == "Data Cleaning":
    st.title("Data Cleaning")
    st.caption("Cleaning rules applied in data_preparation.py")

    # cleaning counts
    col1, col2, col3 = st.columns(3)
    col1.metric("Before Cleaning", "541,909")
    col2.metric("After Cleaning", "397,884")
    col3.metric("Removed", "144,025", delta="-26.6%", delta_color="inverse")

    st.divider()

    # cleaning rules
    st.subheader("Cleaning Rules")
    rules = {
        "Rule": [
            "Drop cancelled invoices",
            "Keep Quantity > 0",
            "Keep UnitPrice > 0",
            "Keep CustomerID not null"
        ],
        "Reason": [
            "InvoiceNo starts with 'C' = cancellation, not a sale",
            "Negative or zero quantities are return adjustments or errors",
            "Zero prices are test items, manual corrections, or postage notes",
            "Transactions without a customer ID cannot be linked to any customer"
        ]
    }
    st.dataframe(pd.DataFrame(rules), use_container_width=True, hide_index=True)

    st.divider()

    # cleaning funnel chart
    st.subheader("Cleaning Funnel")
    funnel_data = pd.DataFrame({
        "Stage": ["Raw Dataset", "Remove Cancellations", "Remove Invalid Qty", "Remove Invalid Price", "Remove Missing Customer"],
        "Records": [541909, 532618, 530104, 530103, 397884]
    })

    fig = go.Figure(go.Funnel(
        y=funnel_data["Stage"],
        x=funnel_data["Records"],
        textinfo="value+percent previous",
        marker_color=["#636EFA", "#636EFA", "#636EFA", "#636EFA", "#00CC96"]
    ))
    fig.update_layout(height=400, margin=dict(l=0, r=0, t=20, b=0))
    st.plotly_chart(fig, use_container_width=True)

    st.divider()

    # stream split
    st.subheader("Online vs Periodic Split")
    col1, col2 = st.columns(2)

    split_data = pd.DataFrame({
        "Mode": ["Online", "Periodic"],
        "Transactions": [278740, 119144]
    })

    with col1:
        fig = px.pie(
            split_data, values="Transactions", names="Mode",
            color_discrete_sequence=["#636EFA", "#EF553B"],
            hole=0.4
        )
        fig.update_layout(height=300, margin=dict(l=0, r=0, t=10, b=0))
        st.plotly_chart(fig, use_container_width=True)

    with col2:
        st.markdown("**Split Rule:**")
        st.code("stable(transaction_id, 10) < 7  ->  online\nstable(transaction_id, 10) >= 7 ->  periodic")
        st.markdown("The split is applied at invoice level so all items in one cart get the same mode.")
        st.metric("Online (70%)", "278,740 transactions")
        st.metric("Periodic (30%)", "119,144 transactions — split into 48 hourly release groups")



# ==================================================================
#           PAGE 3: MESHJOIN PIPELINE
# ==================================================================

elif page == "MESHJOIN Pipeline":
    st.title("MESHJOIN Pipeline")
    st.caption("Multi-threaded stream-relation join for near real-time ETL enrichment")

    # ETL run summary
    st.subheader("ETL Execution Summary (Last Run)")
    col1, col2, col3, col4 = st.columns(4)
    col1.metric("Online Pushed", "278,740")
    col2.metric("Periodic Pushed", "800")
    col3.metric("Shed", "118,344")
    col4.metric("Recovered", "118,344")

    col1, col2, col3, col4 = st.columns(4)
    col1.metric("Total Enriched", "397,884")
    col2.metric("Fact Records Inserted", "397,884")
    col3.metric("Execution Time", "~10 sec")
    col4.metric("MESHJOIN Iterations", "4,584")

    st.divider()

    # MESHJOIN config
    st.subheader("MESHJOIN Configuration")
    col1, col2 = st.columns(2)

    with col1:
        config_data = pd.DataFrame({
            "Parameter": [
                "Stream buffer capacity",
                "Chunk size (w)",
                "Partition (page) size",
                "Customer master data partitions",
                "Product master data partitions",
                "Hash Table H type"
            ],
            "Value": [
                "1,000 transactions",
                "500 transactions per iteration",
                "500 tuples per page",
                "9 pages",
                "8 pages",
                "Multi-map (dict of lists)"
            ]
        })
        st.dataframe(config_data, use_container_width=True, hide_index=True)

    with col2:
        # lifetime chart: how many pages a chunk sees
        lifetime_data = pd.DataFrame({
            "Pass": ["Customer Pass", "Product Pass"],
            "Pages": [9, 8],
            "Tuples": [4338, 3665]
        })
        fig = px.bar(
            lifetime_data, x="Pass", y="Pages",
            text="Pages",
            color="Pass",
            color_discrete_sequence=["#636EFA", "#EF553B"],
            labels={"Pages": "Number of Partitions"},
            title="Partitions Each Chunk Must See Before Eviction"
        )
        fig.update_layout(showlegend=False, height=300, margin=dict(l=0, r=0, t=40, b=0))
        st.plotly_chart(fig, use_container_width=True)

    st.divider()

    # four steps
    st.subheader("MESHJOIN Four-Step Iteration")
    steps = {
        "Step": ["Step 1", "Step 2", "Step 3", "Step 4"],
        "Name": ["Load Chunk", "Load Master Data Page", "Probe and Enrich", "Evict Completed Chunks"],
        "Detail": [
            "Move up to 500 transactions from stream buffer into Hash Table H. Add chunk pointer to Queue with lifetime = (9 customer pages, 8 product pages).",
            "Read the next customer page (500 tuples) and next product page (500 tuples) from DiskReader into disk buffer (cyclic).",
            "For each customer tuple, probe hash_customer and add demographics. For each product tuple, probe hash_product and add product name, category, supplier, store, and compute revenue = qty × price.",
            "Pop the oldest chunk from Queue only if its lifetime counters both reach 0. Remove its entries from hash_customer and hash_product. Send completed tuples to output queue."
        ]
    }
    st.dataframe(pd.DataFrame(steps), use_container_width=True, hide_index=True)

    st.divider()

    # load shedding
    st.subheader("Load Shedding and Recovery")
    col1, col2, col3 = st.columns(3)

    shed_data = pd.DataFrame({
        "Stage": ["Total Periodic", "Directly Pushed", "Shed to shed_queue", "Recovered Later"],
        "Count": [119144, 800, 118344, 118344]
    })

    with col1:
        fig = px.bar(
            shed_data, x="Stage", y="Count",
            color="Stage",
            color_discrete_sequence=["#636EFA", "#00CC96", "#EF553B", "#AB63FA"],
            title="Load Shedding Distribution"
        )
        fig.update_layout(showlegend=False, height=320, margin=dict(l=0, r=0, t=40, b=0))
        st.plotly_chart(fig, use_container_width=True)

    with col2:
        st.markdown("**When Shedding Happens:**")
        st.info("When the stream buffer reaches 1,000 transactions, the periodic extractor cannot push more records into it. Those periodic records go to the shed_queue in memory instead.")

        st.markdown("**When Recovery Happens:**")
        st.success("At the start of each hourly release cycle, the periodic thread checks if the buffer has free space. If it does, it pushes shed transactions back in before the new batch. Zero transactions are permanently lost.")

    with col3:
        # 3 threads diagram
        st.markdown("**Thread Roles:**")
        thread_data = pd.DataFrame({
            "Thread": ["StreamProducerThread", "PeriodicExtractorThread", "MeshJoinWorkerThread"],
            "Role": ["Pushes online CSV records into stream buffer", "Pushes hourly periodic batches, handles shedding/recovery", "Runs MESHJOIN iterations, enriches output"]
        })
        st.dataframe(thread_data, use_container_width=True, hide_index=True)



# ==================================================================
#           PAGE 4: CUSTOMER ANALYSIS
# ==================================================================

elif page == "Customer Analysis":
    st.title("Customer Analysis")

    if conn and conn.is_connected():
        # live from DW
        st.caption("Live data from retail_dw")

        # gender breakdown
        col1, col2 = st.columns(2)
        with col1:
            df = run_query(conn, """
                SELECT c.gender, round(sum(f.revenue), 2) as revenue
                FROM transaction_fact f
                JOIN dim_customer c ON f.customer_key = c.customer_key
                GROUP BY c.gender
            """)
            if not df.empty:
                fig = px.pie(df, values="revenue", names="gender",
                             title="Revenue by Gender", hole=0.4,
                             color_discrete_sequence=["#636EFA", "#EF553B"])
                fig.update_layout(height=300)
                st.plotly_chart(fig, use_container_width=True)

        with col2:
            df = run_query(conn, """
                SELECT c.marital_status, round(sum(f.revenue), 2) as revenue
                FROM transaction_fact f
                JOIN dim_customer c ON f.customer_key = c.customer_key
                GROUP BY c.marital_status
            """)
            if not df.empty:
                fig = px.pie(df, values="revenue", names="marital_status",
                             title="Revenue by Marital Status", hole=0.4,
                             color_discrete_sequence=["#AB63FA", "#FFA15A"])
                fig.update_layout(height=300)
                st.plotly_chart(fig, use_container_width=True)

        st.divider()

        # age group
        df = run_query(conn, """
            SELECT c.age_group, round(sum(f.revenue), 2) as revenue,
                   count(distinct f.transaction_id) as orders
            FROM transaction_fact f
            JOIN dim_customer c ON f.customer_key = c.customer_key
            GROUP BY c.age_group
            ORDER BY revenue DESC
        """)
        if not df.empty:
            fig = px.bar(df, x="age_group", y="revenue",
                         color="orders", color_continuous_scale="Blues",
                         title="Revenue by Age Group",
                         labels={"revenue": "Total Revenue", "age_group": "Age Group"})
            fig.update_layout(height=350)
            st.plotly_chart(fig, use_container_width=True)

        st.divider()

        # occupation
        df = run_query(conn, """
            SELECT c.occupation, round(sum(f.revenue), 2) as revenue
            FROM transaction_fact f
            JOIN dim_customer c ON f.customer_key = c.customer_key
            GROUP BY c.occupation
            ORDER BY revenue DESC
        """)
        if not df.empty:
            fig = px.bar(df, x="revenue", y="occupation", orientation="h",
                         title="Revenue by Occupation",
                         color="revenue", color_continuous_scale="Teal")
            fig.update_layout(height=400, yaxis=dict(categoryorder="total ascending"))
            st.plotly_chart(fig, use_container_width=True)

    else:
        # from CSV master data only
        st.info("Connect to MySQL for live DW query results. Showing master data preview below.")
        st.caption("From customer_master_data.csv")
        df = load_csv(customer_csv)
        col1, col2 = st.columns(2)

        with col1:
            fig = px.histogram(df, x="gender", color="gender",
                               title="Gender Distribution",
                               color_discrete_sequence=["#636EFA", "#EF553B"])
            st.plotly_chart(fig, use_container_width=True)

        with col2:
            fig = px.histogram(df, x="age_group", color="age_group",
                               title="Age Group Distribution")
            st.plotly_chart(fig, use_container_width=True)

        st.dataframe(df.head(50), use_container_width=True, hide_index=True)



# ==================================================================
#           PAGE 5: PRODUCT ANALYSIS
# ==================================================================

elif page == "Product Analysis":
    st.title("Product Analysis")

    if conn and conn.is_connected():
        st.caption("Live data from retail_dw")

        # product category revenue
        df = run_query(conn, """
            SELECT p.product_category, round(sum(f.revenue), 2) as revenue,
                   sum(f.quantity) as units_sold
            FROM transaction_fact f
            JOIN dim_product p ON f.product_key = p.product_key
            GROUP BY p.product_category
            ORDER BY revenue DESC
        """)
        if not df.empty:
            col1, col2 = st.columns(2)
            with col1:
                fig = px.bar(df, x="product_category", y="revenue",
                             title="Revenue by Product Category",
                             color="revenue", color_continuous_scale="Viridis")
                fig.update_layout(height=360, xaxis_tickangle=45)
                st.plotly_chart(fig, use_container_width=True)
            with col2:
                fig = px.pie(df, values="units_sold", names="product_category",
                             title="Units Sold by Category", hole=0.35)
                fig.update_layout(height=360)
                st.plotly_chart(fig, use_container_width=True)

        st.divider()

        # seasonal analysis (Q14)
        df_season = run_query(conn, """
            SELECT t.season, p.product_category,
                   round(sum(f.revenue), 2) as revenue
            FROM transaction_fact f
            JOIN dim_product p ON f.product_key = p.product_key
            JOIN dim_time t ON f.date_key = t.date_key
            GROUP BY t.season, p.product_category
            ORDER BY t.season, revenue DESC
        """)
        if not df_season.empty:
            st.subheader("Seasonal Revenue by Product Category")
            fig = px.bar(df_season, x="product_category", y="revenue",
                         color="season", barmode="group",
                         title="Seasonal Analysis",
                         labels={"revenue": "Revenue", "product_category": "Category"},
                         color_discrete_sequence=px.colors.qualitative.Set2)
            fig.update_layout(height=400, xaxis_tickangle=45)
            st.plotly_chart(fig, use_container_width=True)

        st.divider()

        # top products
        df_top = run_query(conn, """
            SELECT p.product_name, round(sum(f.revenue), 2) as revenue
            FROM transaction_fact f
            JOIN dim_product p ON f.product_key = p.product_key
            GROUP BY p.product_name
            ORDER BY revenue DESC
            LIMIT 15
        """)
        if not df_top.empty:
            st.subheader("Top 15 Products by Revenue")
            fig = px.bar(df_top, x="revenue", y="product_name", orientation="h",
                         color="revenue", color_continuous_scale="Bluered")
            fig.update_layout(height=500, yaxis=dict(categoryorder="total ascending"))
            st.plotly_chart(fig, use_container_width=True)

    else:
        st.info("Connect to MySQL to see live product analysis. Showing master data preview.")
        df = load_csv(product_csv)
        col1, col2 = st.columns(2)
        with col1:
            cat_counts = df["product_category"].value_counts().reset_index()
            cat_counts.columns = ["category", "count"]
            fig = px.bar(cat_counts, x="category", y="count",
                         title="Product Count per Category", color="count",
                         color_continuous_scale="Blues")
            fig.update_layout(xaxis_tickangle=45, height=360)
            st.plotly_chart(fig, use_container_width=True)
        with col2:
            sup_counts = df["supplier_name"].value_counts().reset_index()
            sup_counts.columns = ["supplier", "count"]
            fig = px.pie(sup_counts, values="count", names="supplier",
                         title="Products per Supplier", hole=0.4)
            fig.update_layout(height=360)
            st.plotly_chart(fig, use_container_width=True)



# ==================================================================
#           PAGE 6: STORE & SUPPLIER
# ==================================================================

elif page == "Store & Supplier":
    st.title("Store and Supplier Analysis")

    stores_df = pd.DataFrame({
        "Store ID": ["1", "2", "3", "4", "5"],
        "Store Name": ["Online", "London Bridge", "Manchester Arndale", "Birmingham Bullring", "Edinburgh Princes"],
        "City": ["Web", "London", "Manchester", "Birmingham", "Edinburgh"],
        "Type": ["Online", "Physical", "Physical", "Physical", "Physical"]
    })

    suppliers_df = pd.DataFrame({
        "Supplier ID": ["SUP01", "SUP02", "SUP03", "SUP04", "SUP05"],
        "Supplier Name": ["Northwind Supplies", "Atlantic Gifts Ltd", "Midlands Wholesale", "Celtic Craft Co", "EuroPack Partners"]
    })

    col1, col2 = st.columns(2)
    with col1:
        st.subheader("Store Locations")
        st.dataframe(stores_df, use_container_width=True, hide_index=True)

    with col2:
        st.subheader("Suppliers")
        st.dataframe(suppliers_df, use_container_width=True, hide_index=True)

    st.divider()

    if conn and conn.is_connected():
        st.caption("Live data from retail_dw")

        # store revenue
        df_store = run_query(conn, """
            SELECT s.store_name, round(sum(f.revenue), 2) as revenue,
                   count(distinct f.transaction_id) as orders
            FROM transaction_fact f
            JOIN dim_store s ON f.store_key = s.store_key
            GROUP BY s.store_name
            ORDER BY revenue DESC
        """)
        if not df_store.empty:
            col1, col2 = st.columns(2)
            with col1:
                fig = px.bar(df_store, x="store_name", y="revenue",
                             title="Revenue by Store",
                             color="revenue", color_continuous_scale="Teal")
                fig.update_layout(height=360)
                st.plotly_chart(fig, use_container_width=True)
            with col2:
                fig = px.pie(df_store, values="orders", names="store_name",
                             title="Order Share by Store", hole=0.35)
                fig.update_layout(height=360)
                st.plotly_chart(fig, use_container_width=True)

        st.divider()

        # quarterly store view (Q20)
        df_qtr = run_query(conn, """
            SELECT s.store_name, t.year, t.quarter,
                   round(sum(f.revenue), 2) as quarterly_sales
            FROM transaction_fact f
            JOIN dim_store s ON f.store_key = s.store_key
            JOIN dim_time t ON f.date_key = t.date_key
            GROUP BY s.store_name, t.year, t.quarter
            ORDER BY s.store_name, t.year, t.quarter
        """)
        if not df_qtr.empty:
            st.subheader("STORE_QUARTERLY_SALES View (Q20)")
            df_qtr["Period"] = df_qtr["year"].astype(str) + " Q" + df_qtr["quarter"].astype(str)
            fig = px.line(df_qtr, x="Period", y="quarterly_sales",
                          color="store_name", markers=True,
                          title="Quarterly Revenue per Store")
            fig.update_layout(height=400)
            st.plotly_chart(fig, use_container_width=True)

        st.divider()

        # supplier revenue
        df_sup = run_query(conn, """
            SELECT sup.supplier_name, round(sum(f.revenue), 2) as revenue
            FROM transaction_fact f
            JOIN dim_supplier sup ON f.supplier_key = sup.supplier_key
            GROUP BY sup.supplier_name
            ORDER BY revenue DESC
        """)
        if not df_sup.empty:
            st.subheader("Revenue by Supplier")
            fig = px.bar(df_sup, x="supplier_name", y="revenue",
                         color="revenue", color_continuous_scale="Purples",
                         title="Supplier Revenue Contribution")
            fig.update_layout(height=360)
            st.plotly_chart(fig, use_container_width=True)

    else:
        st.info("Connect to MySQL for live store and supplier revenue charts.")



# ==================================================================
#           PAGE 7: OLAP QUERIES
# ==================================================================

elif page == "OLAP Queries":
    st.title("OLAP Analytical Queries")
    st.caption("20 business intelligence queries from Queries-DW/analysis_queries.sql")

    if not (conn and conn.is_connected()):
        st.warning("Connect to MySQL in the sidebar to run the OLAP queries.")
        st.stop()

    query_choice = st.selectbox("Select a Query to Run", [
        "Q1  — Top 5 products by revenue on weekdays vs weekends",
        "Q2  — Customer demographics by purchase amount and city category",
        "Q3  — Product category sales by customer occupation",
        "Q4  — Purchase trends by gender and age group across quarters",
        "Q5  — Top 5 occupations by product category revenue",
        "Q6  — City category by marital status (latest 6 months)",
        "Q7  — Average purchase by city stay duration and gender",
        "Q8  — Top city categories by product category",
        "Q9  — Month-over-month sales growth by product category",
        "Q10 — Weekend vs weekday sales by age group",
        "Q12 — Store revenue growth quarter-over-quarter",
        "Q13 — Supplier sales by store and product",
        "Q14 — Seasonal product category analysis",
        "Q17 — Yearly revenue rollup by store, supplier, product",
        "Q18 — H1 vs H2 vs full year revenue by product",
        "Q19 — High revenue spikes (> 2x daily average)",
        "Q20 — STORE_QUARTERLY_SALES view"
    ])

    query_map = {
        "Q1  — Top 5 products by revenue on weekdays vs weekends": """
            SELECT t.month, t.weekday_weekend, p.product_name,
                   round(sum(f.revenue), 2) as total_revenue
            FROM transaction_fact f
            JOIN dim_product p ON f.product_key = p.product_key
            JOIN dim_time t ON f.date_key = t.date_key
            GROUP BY t.month, t.weekday_weekend, p.product_name
            ORDER BY total_revenue DESC LIMIT 20
        """,
        "Q2  — Customer demographics by purchase amount and city category": """
            SELECT c.city_category, c.gender, c.age_group,
                   round(sum(f.revenue), 2) as total_spending
            FROM transaction_fact f
            JOIN dim_customer c ON f.customer_key = c.customer_key
            GROUP BY c.city_category, c.gender, c.age_group
            ORDER BY c.city_category, total_spending DESC LIMIT 30
        """,
        "Q3  — Product category sales by customer occupation": """
            SELECT c.occupation, p.product_category,
                   round(sum(f.revenue), 2) as total_sales
            FROM transaction_fact f
            JOIN dim_customer c ON f.customer_key = c.customer_key
            JOIN dim_product p ON f.product_key = p.product_key
            GROUP BY c.occupation, p.product_category
            ORDER BY c.occupation, total_sales DESC
        """,
        "Q4  — Purchase trends by gender and age group across quarters": """
            SELECT t.quarter, c.gender, c.age_group,
                   round(sum(f.revenue), 2) as quarterly_revenue
            FROM transaction_fact f
            JOIN dim_customer c ON f.customer_key = c.customer_key
            JOIN dim_time t ON f.date_key = t.date_key
            GROUP BY t.quarter, c.gender, c.age_group
            ORDER BY t.quarter, quarterly_revenue DESC
        """,
        "Q5  — Top 5 occupations by product category revenue": """
            SELECT p.product_category, c.occupation,
                   round(sum(f.revenue), 2) as total_revenue
            FROM transaction_fact f
            JOIN dim_customer c ON f.customer_key = c.customer_key
            JOIN dim_product p ON f.product_key = p.product_key
            GROUP BY p.product_category, c.occupation
            ORDER BY total_revenue DESC LIMIT 25
        """,
        "Q6  — City category by marital status (latest 6 months)": """
            SELECT c.city_category, c.marital_status,
                   round(sum(f.revenue), 2) as recent_revenue
            FROM transaction_fact f
            JOIN dim_customer c ON f.customer_key = c.customer_key
            JOIN dim_time t ON f.date_key = t.date_key
            WHERE t.full_date >= (SELECT date_sub(max(full_date), interval 6 month) FROM dim_time)
            GROUP BY c.city_category, c.marital_status
            ORDER BY c.city_category, recent_revenue DESC
        """,
        "Q7  — Average purchase by city stay duration and gender": """
            SELECT c.stay_in_current_city_years, c.gender,
                   round(avg(f.revenue), 2) as avg_purchase,
                   round(sum(f.revenue), 2) as total_spending
            FROM transaction_fact f
            JOIN dim_customer c ON f.customer_key = c.customer_key
            GROUP BY c.stay_in_current_city_years, c.gender
            ORDER BY c.stay_in_current_city_years, c.gender
        """,
        "Q8  — Top city categories by product category": """
            SELECT p.product_category, c.city_category,
                   round(sum(f.revenue), 2) as category_revenue
            FROM transaction_fact f
            JOIN dim_customer c ON f.customer_key = c.customer_key
            JOIN dim_product p ON f.product_key = p.product_key
            GROUP BY p.product_category, c.city_category
            ORDER BY p.product_category, category_revenue DESC
        """,
        "Q9  — Month-over-month sales growth by product category": """
            SELECT p.product_category, t.year, t.month,
                   round(sum(f.revenue), 2) as monthly_revenue
            FROM transaction_fact f
            JOIN dim_product p ON f.product_key = p.product_key
            JOIN dim_time t ON f.date_key = t.date_key
            GROUP BY p.product_category, t.year, t.month
            ORDER BY p.product_category, t.year, t.month
        """,
        "Q10 — Weekend vs weekday sales by age group": """
            SELECT c.age_group,
                   round(sum(CASE WHEN t.weekday_weekend='Weekday' THEN f.revenue ELSE 0 END),2) as weekday_sales,
                   round(sum(CASE WHEN t.weekday_weekend='Weekend' THEN f.revenue ELSE 0 END),2) as weekend_sales,
                   round(sum(f.revenue), 2) as total_sales
            FROM transaction_fact f
            JOIN dim_customer c ON f.customer_key = c.customer_key
            JOIN dim_time t ON f.date_key = t.date_key
            GROUP BY c.age_group
            ORDER BY total_sales DESC
        """,
        "Q12 — Store revenue growth quarter-over-quarter": """
            SELECT s.store_name, t.year, t.quarter,
                   round(sum(f.revenue), 2) as quarterly_sales
            FROM transaction_fact f
            JOIN dim_store s ON f.store_key = s.store_key
            JOIN dim_time t ON f.date_key = t.date_key
            GROUP BY s.store_name, t.year, t.quarter
            ORDER BY s.store_name, t.year, t.quarter
        """,
        "Q13 — Supplier sales by store and product": """
            SELECT sup.supplier_name, s.store_name,
                   round(sum(f.revenue), 2) as total_revenue
            FROM transaction_fact f
            JOIN dim_supplier sup ON f.supplier_key = sup.supplier_key
            JOIN dim_store s ON f.store_key = s.store_key
            GROUP BY sup.supplier_name, s.store_name
            ORDER BY sup.supplier_name, total_revenue DESC
        """,
        "Q14 — Seasonal product category analysis": """
            SELECT t.season, p.product_category,
                   round(sum(f.revenue), 2) as seasonal_revenue,
                   sum(f.quantity) as units_sold
            FROM transaction_fact f
            JOIN dim_product p ON f.product_key = p.product_key
            JOIN dim_time t ON f.date_key = t.date_key
            GROUP BY t.season, p.product_category
            ORDER BY t.season, seasonal_revenue DESC
        """,
        "Q17 — Yearly revenue rollup by store, supplier, product": """
            SELECT t.year,
                   coalesce(s.store_name, 'All Stores') as store_name,
                   coalesce(sup.supplier_name, 'All Suppliers') as supplier_name,
                   round(sum(f.revenue), 2) as total_revenue
            FROM transaction_fact f
            JOIN dim_time t ON f.date_key = t.date_key
            JOIN dim_store s ON f.store_key = s.store_key
            JOIN dim_supplier sup ON f.supplier_key = sup.supplier_key
            JOIN dim_product p ON f.product_key = p.product_key
            GROUP BY t.year, s.store_name, sup.supplier_name WITH ROLLUP
            LIMIT 100
        """,
        "Q18 — H1 vs H2 vs full year revenue by product": """
            SELECT p.product_name,
                   round(sum(CASE WHEN t.half_year=1 THEN f.revenue ELSE 0 END),2) as h1_revenue,
                   round(sum(CASE WHEN t.half_year=2 THEN f.revenue ELSE 0 END),2) as h2_revenue,
                   round(sum(f.revenue), 2) as full_year_revenue
            FROM transaction_fact f
            JOIN dim_product p ON f.product_key = p.product_key
            JOIN dim_time t ON f.date_key = t.date_key
            GROUP BY p.product_name
            ORDER BY full_year_revenue DESC
            LIMIT 20
        """,
        "Q19 — High revenue spikes (> 2x daily average)": """
            SELECT p.product_name, t.full_date,
                   round(sum(f.revenue), 2) as daily_rev
            FROM transaction_fact f
            JOIN dim_product p ON f.product_key = p.product_key
            JOIN dim_time t ON f.date_key = t.date_key
            GROUP BY p.product_name, t.full_date
            HAVING daily_rev > 2 * (
                SELECT avg(x.daily) FROM (
                    SELECT sum(f2.revenue) as daily
                    FROM transaction_fact f2
                    WHERE f2.product_key = f.product_key
                    GROUP BY f2.date_key
                ) x
            )
            ORDER BY daily_rev DESC LIMIT 30
        """,
        "Q20 — STORE_QUARTERLY_SALES view": """
            SELECT s.store_name, t.year, t.quarter,
                   round(sum(f.revenue), 2) as quarterly_sales
            FROM transaction_fact f
            JOIN dim_store s ON f.store_key = s.store_key
            JOIN dim_time t ON f.date_key = t.date_key
            GROUP BY s.store_name, t.year, t.quarter
            ORDER BY s.store_name, t.year, t.quarter
        """
    }

    if st.button("Run Query"):
        q = query_map.get(query_choice, "")
        with st.spinner("Running query..."):
            df = run_query(conn, q)

        if df.empty:
            st.warning("Query returned no results. Make sure transaction_fact is populated.")
        else:
            st.success(f"Returned {len(df):,} rows")
            st.dataframe(df, use_container_width=True, hide_index=True)

            # auto chart if numeric columns exist
            numeric_cols = df.select_dtypes(include="number").columns.tolist()
            text_cols = df.select_dtypes(include="object").columns.tolist()

            if numeric_cols and text_cols:
                st.divider()
                st.subheader("Quick Chart")
                col_x = st.selectbox("X axis", text_cols)
                col_y = st.selectbox("Y axis", numeric_cols)

                if col_x and col_y:
                    fig = px.bar(df.head(30), x=col_x, y=col_y,
                                 color=col_y, color_continuous_scale="Blues",
                                 title=f"{col_y} by {col_x}")
                    fig.update_layout(height=400, xaxis_tickangle=45)
                    st.plotly_chart(fig, use_container_width=True)

