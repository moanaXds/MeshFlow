
-- data warehouse analytical queries
-- course: ds3003 & ds3004 data warehousing & bi

use retail_dw;



-- q1: top 5 revenue products on weekdays and weekends with monthly breakdown

select
    t.month,
    t.weekday_weekend,
    p.product_id,
    p.product_name,
    round(sum(f.revenue), 2) as total_revenue
from transaction_fact f
join dim_product p on f.product_key = p.product_key
join dim_time t on f.date_key = t.date_key
group by t.month, t.weekday_weekend, p.product_id, p.product_name
order by t.month, t.weekday_weekend, total_revenue desc
limit 5;



-- q2: customer demographics (gender, age) by purchase amount with city category

select
    c.city_category,
    c.gender,
    c.age_group,
    round(sum(f.revenue), 2) as total_spending,
    count(distinct f.transaction_id) as total_orders
from transaction_fact f
join dim_customer c on f.customer_key = c.customer_key
group by c.city_category, c.gender, c.age_group
order by c.city_category, total_spending desc;



-- q3: product category sales by customer occupation

select
    c.occupation,
    p.product_category,
    round(sum(f.revenue), 2) as total_sales,
    sum(f.quantity) as total_quantity
from transaction_fact f
join dim_customer c on f.customer_key = c.customer_key
join dim_product p on f.product_key = p.product_key
group by c.occupation, p.product_category
order by c.occupation, total_sales desc;



-- q4: purchase trends by gender and age group across quarters

select
    t.quarter,
    c.gender,
    c.age_group,
    round(sum(f.revenue), 2) as quarterly_revenue,
    count(distinct f.transaction_id) as total_transactions
from transaction_fact f
join dim_customer c on f.customer_key = c.customer_key
join dim_time t on f.date_key = t.date_key
group by t.quarter, c.gender, c.age_group
order by t.quarter, quarterly_revenue desc;



-- q5: top 5 occupations by product category sales

select
    p.product_category,
    c.occupation,
    round(sum(f.revenue), 2) as total_revenue
from transaction_fact f
join dim_customer c on f.customer_key = c.customer_key
join dim_product p on f.product_key = p.product_key
group by p.product_category, c.occupation
order by total_revenue desc
limit 5;



-- q6: city category performance by marital status over latest 6 months

select
    c.city_category,
    c.marital_status,
    round(sum(f.revenue), 2) as recent_revenue,
    count(distinct f.transaction_id) as orders_count
from transaction_fact f
join dim_customer c on f.customer_key = c.customer_key
join dim_time t on f.date_key = t.date_key
where t.full_date >= (select date_sub(max(full_date), interval 6 month) from dim_time)
group by c.city_category, c.marital_status
order by c.city_category, recent_revenue desc;



-- q7: average purchase amount by city stay duration and gender

select
    c.stay_in_current_city_years,
    c.gender,
    round(avg(f.revenue), 2) as average_purchase_amount,
    round(sum(f.revenue), 2) as total_spending
from transaction_fact f
join dim_customer c on f.customer_key = c.customer_key
group by c.stay_in_current_city_years, c.gender
order by c.stay_in_current_city_years, c.gender;



-- q8: top revenue city categories (a, b, c) by product category

select
    p.product_category,
    c.city_category,
    round(sum(f.revenue), 2) as category_revenue
from transaction_fact f
join dim_customer c on f.customer_key = c.customer_key
join dim_product p on f.product_key = p.product_key
group by p.product_category, c.city_category
order by p.product_category, category_revenue desc;



-- q9: month over month sales growth percentage by product category

with monthly_sales as (
    select
        p.product_category,
        t.year,
        t.month,
        sum(f.revenue) as revenue
    from transaction_fact f
    join dim_product p on f.product_key = p.product_key
    join dim_time t on f.date_key = t.date_key
    group by p.product_category, t.year, t.month
)
select
    product_category,
    year,
    month,
    round(revenue, 2) as current_month_revenue,
    round(lag(revenue) over (partition by product_category order by year, month), 2) as prev_month_revenue,
    round(
        (revenue - lag(revenue) over (partition by product_category order by year, month))
        / lag(revenue) over (partition by product_category order by year, month) * 100,
        2
    ) as growth_percentage
from monthly_sales
order by product_category, year, month;



-- q10: weekend vs weekday sales comparison by age group

select
    c.age_group,
    round(sum(case when t.weekday_weekend = 'Weekday' then f.revenue else 0 end), 2) as weekday_sales,
    round(sum(case when t.weekday_weekend = 'Weekend' then f.revenue else 0 end), 2) as weekend_sales,
    round(sum(f.revenue), 2) as total_sales
from transaction_fact f
join dim_customer c on f.customer_key = c.customer_key
join dim_time t on f.date_key = t.date_key
group by c.age_group
order by total_sales desc;



-- q11: top 5 revenue products on weekdays vs weekends grouped by month

select
    t.month,
    t.weekday_weekend,
    p.product_id,
    p.product_name,
    round(sum(f.revenue), 2) as total_revenue
from transaction_fact f
join dim_product p on f.product_key = p.product_key
join dim_time t on f.date_key = t.date_key
group by t.month, t.weekday_weekend, p.product_id, p.product_name
order by t.month, total_revenue desc
limit 5;



-- q12: store revenue growth rate quarter over quarter

with quarterly_store_sales as (
    select
        s.store_name,
        t.year,
        t.quarter,
        sum(f.revenue) as revenue
    from transaction_fact f
    join dim_store s on f.store_key = s.store_key
    join dim_time t on f.date_key = t.date_key
    group by s.store_name, t.year, t.quarter
)
select
    store_name,
    year,
    quarter,
    round(revenue, 2) as current_quarter_sales,
    round(lag(revenue) over (partition by store_name order by year, quarter), 2) as prev_quarter_sales,
    round(
        (revenue - lag(revenue) over (partition by store_name order by year, quarter))
        / lag(revenue) over (partition by store_name order by year, quarter) * 100,
        2
    ) as qoq_growth_percentage
from quarterly_store_sales
order by store_name, year, quarter;



-- q13: supplier sales contribution by store and product name

select
    sup.supplier_name,
    st.store_name,
    p.product_name,
    round(sum(f.revenue), 2) as total_revenue,
    sum(f.quantity) as total_units_sold
from transaction_fact f
join dim_supplier sup on f.supplier_key = sup.supplier_key
join dim_store st on f.store_key = st.store_key
join dim_product p on f.product_key = p.product_key
group by sup.supplier_name, st.store_name, p.product_name
order by sup.supplier_name, total_revenue desc;



-- q14: seasonal analysis (spring, summer, autumn, winter) of product sales

select
    t.season,
    p.product_category,
    round(sum(f.revenue), 2) as seasonal_revenue,
    sum(f.quantity) as units_sold
from transaction_fact f
join dim_product p on f.product_key = p.product_key
join dim_time t on f.date_key = t.date_key
group by t.season, p.product_category
order by t.season, seasonal_revenue desc;



-- q15: month to month revenue volatility percentage by store and supplier

with monthly_store_supplier as (
    select
        st.store_name,
        sup.supplier_name,
        t.year,
        t.month,
        sum(f.revenue) as monthly_revenue
    from transaction_fact f
    join dim_store st on f.store_key = st.store_key
    join dim_supplier sup on f.supplier_key = sup.supplier_key
    join dim_time t on f.date_key = t.date_key
    group by st.store_name, sup.supplier_name, t.year, t.month
)
select
    store_name,
    supplier_name,
    year,
    month,
    round(monthly_revenue, 2) as current_month_rev,
    round(
        abs(monthly_revenue - lag(monthly_revenue) over (partition by store_name, supplier_name order by year, month))
        / lag(monthly_revenue) over (partition by store_name, supplier_name order by year, month) * 100,
        2
    ) as volatility_percentage
from monthly_store_supplier
order by store_name, supplier_name, year, month;



-- q16: top 5 product pairs purchased together in the same cart (product bundling)

select
    f1.product_key as product_1_key,
    p1.product_name as product_1_name,
    f2.product_key as product_2_key,
    p2.product_name as product_2_name,
    count(distinct f1.transaction_id) as times_bought_together
from transaction_fact f1
join transaction_fact f2
    on f1.transaction_id = f2.transaction_id
    and f1.product_key < f2.product_key
join dim_product p1 on f1.product_key = p1.product_key
join dim_product p2 on f2.product_key = p2.product_key
group by f1.product_key, p1.product_name, f2.product_key, p2.product_name
order by times_bought_together desc
limit 5;



-- q17: yearly revenue by store, supplier, and product using rollup

select
    t.year,
    coalesce(st.store_name, 'All Stores') as store_name,
    coalesce(sup.supplier_name, 'All Suppliers') as supplier_name,
    coalesce(p.product_name, 'All Products') as product_name,
    round(sum(f.revenue), 2) as total_revenue
from transaction_fact f
join dim_time t on f.date_key = t.date_key
join dim_store st on f.store_key = st.store_key
join dim_supplier sup on f.supplier_key = sup.supplier_key
join dim_product p on f.product_key = p.product_key
group by t.year, st.store_name, sup.supplier_name, p.product_name with rollup;



-- q18: revenue and volume by product for h1 vs h2 vs full year

select
    p.product_id,
    p.product_name,
    round(sum(case when t.half_year = 1 then f.revenue else 0 end), 2) as h1_revenue,
    sum(case when t.half_year = 1 then f.quantity else 0 end) as h1_quantity,
    round(sum(case when t.half_year = 2 then f.revenue else 0 end), 2) as h2_revenue,
    sum(case when t.half_year = 2 then f.quantity else 0 end) as h2_quantity,
    round(sum(f.revenue), 2) as full_year_revenue,
    sum(f.quantity) as full_year_quantity
from transaction_fact f
join dim_product p on f.product_key = p.product_key
join dim_time t on f.date_key = t.date_key
group by p.product_id, p.product_name
order by full_year_revenue desc;



-- q19: high revenue spikes (> 2x daily average for that product)

with product_daily_sales as (
    select
        f.product_key,
        t.full_date,
        sum(f.revenue) as daily_revenue
    from transaction_fact f
    join dim_time t on f.date_key = t.date_key
    group by f.product_key, t.full_date
),
product_avg_sales as (
    select
        product_key,
        avg(daily_revenue) as avg_daily_revenue
    from product_daily_sales
    group by product_key
)
select
    p.product_id,
    p.product_name,
    d.full_date,
    round(d.daily_revenue, 2) as spike_revenue,
    round(a.avg_daily_revenue, 2) as avg_revenue,
    round(d.daily_revenue / a.avg_daily_revenue, 2) as spike_ratio
from product_daily_sales d
join product_avg_sales a on d.product_key = a.product_key
join dim_product p on d.product_key = p.product_key
where d.daily_revenue > (2 * a.avg_daily_revenue)
order by spike_ratio desc;



-- q20: view store_quarterly_sales (ordered by store name and quarter)

drop view if exists STORE_QUARTERLY_SALES;

create view STORE_QUARTERLY_SALES as
select
    s.store_name,
    t.year,
    t.quarter,
    round(sum(f.revenue), 2) as quarterly_sales
from transaction_fact f
join dim_store s on f.store_key = s.store_key
join dim_time t on f.date_key = t.date_key
group by s.store_name, t.year, t.quarter
order by s.store_name, t.year, t.quarter;

-- query the created view
select * from STORE_QUARTERLY_SALES;

