--Create tables to store information about invidual experiments

drop table if exists experiments;

create table if not exists experiments
(
    experiment_id      varchar(100) not null primary key,
    client_id          varchar(50)  not null,
    experiment_name    varchar(100),
    page_type          varchar(50),
    experiment_type    varchar(50),
    status             varchar(50),
    creation_time      timestamptz  not null default now(),
    last_updation_time timestamptz  not null default now(),
    start_time         timestamptz  not null default now(),
    end_time           timestamptz  not null default now()
);

create
index if not exists experiments_client_id_idx on experiments (client_id);

--Create tables to store information about invidual clients

drop table if exists clients;

create table if not exists clients
(
    client_id                 varchar(100) not null primary key,
    full_name                 varchar(100) not null,
    company_name              varchar(100) not null,
    hashed_password           varchar(100) not null,
    disabled                  boolean      not null,
    shopify_app_api_key       varchar(100),
    shopify_app_password      varchar(100),
    shopify_app_eg_url        varchar(200),
    shopify_app_shared_secret varchar(100)
);


--Create tables to store information about invidual variations

drop table if exists variations;

create table if not exists variations
(
    variation_id       varchar(100) not null,
    variation_name     varchar(100) not null,
    client_id          varchar(50)  not null,
    experiment_id      varchar(100) not null,
    traffic_percentage int,
    s3_bucket_name     varchar(50),
    s3_html_location   varchar(200)
);

create
index if not exists variations_client_id_idx on variations (client_id);
create
index if not exists variations_experiment_id_idx on variations (experiment_id);
create
index if not exists variations_variation_id_idx on variations (variation_id);

drop table if exists events;

create table if not exists events
(
    variation_id  varchar(100) not null,
    client_id     varchar(50)  not null,
    experiment_id varchar(100) not null,
    session_id    varchar(100),
    event_name    varchar(50),
    creation_time timestamptz  not null
);

create
index if not exists events_client_id_idx on events (client_id);
create
index if not exists events_experiment_idx on events (experiment_id);
create
index if not exists events_variation_idx on events (variation_id);
create
index if not exists events_creation_time_idx on events (creation_time);

drop table if exists visits;

create table if not exists visits
(
    client_id     varchar(50) not null,
    session_id    varchar(100),
    event_name    varchar(50),
    creation_time timestamptz not null,
    url           varchar(200)
);

create
index if not exists visits_client_id_idx on visits (client_id);
create
index if not exists visits_event_name_idx on visits (event_name);
create
index if not exists visits_creation_time_idx on visits (creation_time);


drop table if exists products;

create table if not exists products
(
    client_id      varchar(50)  not null,
    product_id     bigint       not null,
    product_title  varchar(100) not null,
    product_handle varchar(100) not null,
    variant_id     bigint       not null,
    variant_title  varchar(100),
    variant_price  numeric(8, 2),
    updated_at     timestamptz  not null
);

create
index if not exists products_client_id_idx on products (client_id);
create
index if not exists products_product_handle_idx on products (product_handle);
create
index if not exists products_updated_at_idx on products (updated_at);

drop table if exists orders;

create table if not exists orders
(
    client_id        varchar(50)  not null,
    order_id         bigint       not null,
    email_id         varchar(100) not null,
    cart_token       varchar(100) not null,
    product_id       bigint       not null,
    variant_id       bigint       not null,
    variant_quantity int,
    variant_price    numeric(8, 2),
    updated_at       timestamptz  not null,
    payment_status   boolean
);

create
index if not exists orders_client_id_idx on orders (client_id);
create
index if not exists orders_order_id_idx on orders (order_id);
create
index if not exists orders_product_id_idx on orders (product_id);
create
index if not exists orders_variant_id_idx on orders (variant_id);
create
index if not exists orders_updated_at_idx on orders (updated_at);


drop table if exists cookie;

create table if not exists cookie
(
    client_id     varchar(50) not null,
    session_id    varchar(100),
    shopify_x     varchar(100),
    cart_token    varchar(100),
    creation_time timestamptz not null
);

create
index if not exists cookie_client_id_idx on cookie (client_id);
create
index if not exists cookie_session_id_idx on cookie (session_id);
create
index if not exists cookie_shopify_x_idx on cookie (shopify_x);
create
index if not exists cookie_cart_token_idx on cookie (cart_token);
create
index if not exists cookie_creation_time_idx on cookie (creation_time);