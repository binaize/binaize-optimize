--Create tables to store information about invidual experiments

drop table if exists experiments;

create table if not exists experiments (
    experiment_id varchar(100) not null primary key,
    client_id varchar(50) not null,
    experiment_name varchar(100),
    page_type varchar(50),
    experiment_type varchar(50)
);

create index if not exists experiments_client_idx on experiments (client_id);
create index if not exists experiments_experiment_idx on experiments (experiment_id);

--Create tables to store information about invidual clients

drop table if exists clients;

create table if not exists clients (
    client_id varchar(100) not null primary key,
    full_name varchar(100) not null,
    company_name varchar(100) not null,
    hashed_password varchar(100) not null,
    disabled boolean not null,
    shopify_app_api_key varchar(100) ,
    shopify_app_password varchar(100) ,
    shopify_app_eg_url varchar(200) ,
    shopify_app_shared_secret varchar(100)
);

create index if not exists clients_idx on clients (client_id);

--Create tables to store information about invidual variations

drop table if exists variations;

create table if not exists variations (
    variation_id varchar(100) not null primary key,
    variation_name varchar(100) not null,
    client_id varchar(50) not null,
    experiment_id varchar(100) not null,
    traffic_percentage int,
    s3_bucket_name varchar(50),
    s3_html_location varchar(200)
);

create index if not exists variations_client_idx on variations (client_id);
create index if not exists variations_experiment_idx on variations (experiment_id);
create index if not exists variations_variation_idx on variations (variation_id);

drop table if exists events;

create table if not exists events (
    variation_id varchar(100) not null,
    client_id varchar(50) not null,
    experiment_id varchar(100) not null,
    session_id varchar(100),
    event_name varchar(50)
);

create index if not exists events_client_idx on events (client_id);
create index if not exists events_experiment_idx on events (experiment_id);
create index if not exists events_variation_idx on events (variation_id);
