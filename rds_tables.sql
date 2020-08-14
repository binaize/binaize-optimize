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
    creation_time      timestamptz,
    last_updation_time timestamptz,
    start_time         timestamptz,
    end_time           timestamptz
);

create
index if not exists experiments_client_id_idx on experiments (client_id);

--Create tables to store information about invidual clients

drop table if exists shops;

create table if not exists shops
(
    shop_id              varchar(100) not null primary key,
    shop_domain          varchar(100),
    shop_owner           varchar(100),
    email_id             varchar(100),
    hashed_password      varchar(100),
    disabled             boolean,
    timezone             varchar(50),
    shopify_access_token varchar(100),
    city                 varchar(100),
    country              varchar(100),
    province             varchar(100),
    shopify_nonce        varchar(100),
    creation_time        timestamptz
);

create
index if not exists shop_id_idx on shops (shop_id);


--Create tables to store information about invidual variations

drop table if exists variations;

create table if not exists variations
(
    variation_id       varchar(100) not null,
    variation_name     varchar(100) not null,
    client_id          varchar(50)  not null,
    experiment_id      varchar(100) not null,
    traffic_percentage int
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
    url           varchar(700)
);

create
index if not exists visits_client_id_idx on visits (client_id);
create
index if not exists visits_event_name_idx on visits (event_name);
create
index if not exists visits_creation_time_idx on visits (creation_time);
create
index if not exists visits_session_idx on visits (session_id);



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
    updated_at     timestamptz  not null,
    tags           varchar(200)
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
    payment_status   boolean,
    landing_page     varchar(700)
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


drop table if exists cookies;

create table if not exists cookies
(
    client_id     varchar(50) not null,
    session_id    varchar(100),
    cart_token    varchar(100),
    creation_time timestamptz not null
);

create
index if not exists cookies_client_id_idx on cookies (client_id);
create
index if not exists cookies_session_id_idx on cookies (session_id);
create
index if not exists cookies_cart_token_idx on cookies (cart_token);
create
index if not exists cookies_creation_time_idx on cookies (creation_time);

drop table if exists visitors;

create table if not exists visitors
(
    client_id     varchar(50) not null,
    session_id    varchar(100),
    ip            varchar(20),
    city          varchar(50),
    region        varchar(50),
    country       varchar(50),
    lat           varchar(20),
    long          varchar(20),
    timezone      varchar(30),
    browser       varchar(30),
    os            varchar(30),
    device        varchar(30),
    fingerprint   varchar(30),
    creation_time timestamptz not null
);

create
index if not exists visitors_client_id_idx on visitors (client_id);
create
index if not exists visitors_os_idx on visitors (os);
create
index if not exists visitors_device_idx on visitors (device);
create
index if not exists visitors_browser_idx on visitors (browser);
create
index if not exists visitors_ip_idx on visitors (ip);
create
index if not exists visitors_country_idx on visitors (country);
create
index if not exists visitors_creation_time_idx on visitors (creation_time);
create
index if not exists visitors_session_idx on visitors (session_id);