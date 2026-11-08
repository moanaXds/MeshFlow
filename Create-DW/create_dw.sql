
-- data warehouse schema

create database if not exists retail_dw;
use retail_dw;


-- remove old tables and view

drop view if exists STORE_QUARTERLY_SALES;

drop table if exists transaction_fact;
drop table if exists dim_time;
drop table if exists dim_store;
drop table if exists dim_product;
drop table if exists dim_supplier;
drop table if exists dim_customer;
drop table if exists shed_table;
drop table if exists periodic_sales_data;


-- customer dimension

create table if not exists dim_customer (
    customer_key int auto_increment primary key,
    customer_id varchar(30) not null unique,
    gender varchar(20),
    age_group varchar(20),
    occupation varchar(50),
    city_category varchar(10),
    stay_in_current_city_years varchar(10),
    marital_status varchar(20)
) engine=innodb;


-- supplier dimension

create table if not exists dim_supplier (
    supplier_key int auto_increment primary key,
    supplier_id varchar(30) not null unique,
    supplier_name varchar(100) not null
) engine=innodb;


-- product dimension

create table if not exists dim_product (
    product_key int auto_increment primary key,
    product_id varchar(50) not null unique,
    product_name varchar(255),
    product_category varchar(100)
) engine=innodb;


-- store dimension

create table if not exists dim_store (
    store_key int auto_increment primary key,
    store_id varchar(30) not null unique,
    store_name varchar(100) not null,
    city varchar(100) not null
) engine=innodb;


-- time dimension

create table if not exists dim_time (
    date_key int auto_increment primary key,
    full_date date not null,
    hour tinyint unsigned not null,
    day tinyint unsigned not null,
    month tinyint unsigned not null,
    quarter tinyint unsigned not null,
    year smallint unsigned not null,
    day_of_week varchar(12) not null,
    weekday_weekend varchar(10) not null,
    season varchar(10) not null,
    half_year tinyint unsigned not null,

    unique (full_date, hour)
) engine=innodb;


-- transaction fact table

create table if not exists transaction_fact (
    fact_key int auto_increment primary key,
    transaction_id varchar(30) not null,
    transaction_line_number int unsigned not null,

    customer_key int not null,
    product_key int not null,
    supplier_key int not null,
    store_key int not null,
    date_key int not null,

    quantity int not null,
    unit_price decimal(12,2) not null,
    revenue decimal(14,2) not null,
    sales_mode varchar(20) not null,
    processing_mode varchar(20) not null,

    unique (transaction_id, transaction_line_number),

    check (quantity > 0),
    check (unit_price > 0),
    check (sales_mode in ('online', 'periodic')),

    foreign key (customer_key)
        references dim_customer(customer_key),

    foreign key (product_key)
        references dim_product(product_key),

    foreign key (supplier_key)
        references dim_supplier(supplier_key),

    foreign key (store_key)
        references dim_store(store_key),

    foreign key (date_key)
        references dim_time(date_key),

    index (transaction_id),
    index (customer_key),
    index (product_key),
    index (supplier_key),
    index (store_key),
    index (date_key),
    index (sales_mode)

) engine=innodb;


-- periodic sales staging table

create table if not exists periodic_sales_data (
    staging_id int auto_increment primary key,
    transaction_id varchar(30) not null,
    customer_id varchar(30) not null,
    product_id varchar(50) not null,
    quantity int not null,
    unit_price decimal(12,2) not null,
    event_time datetime not null,
    sales_mode varchar(20) not null,
    release_hour int unsigned not null,
    released boolean not null default false,
    index (release_hour),
    index (released)
) engine=innodb;


-- shed transactions table

create table if not exists shed_table (
    shed_id int auto_increment primary key,
    transaction_id varchar(30) not null,
    customer_id varchar(30) not null,
    product_id varchar(50) not null,
    quantity int not null,
    unit_price decimal(12,2) not null,
    event_time datetime not null,
    sales_mode varchar(20) not null,
    shed_time datetime not null default current_timestamp
) engine=innodb;


show tables;
