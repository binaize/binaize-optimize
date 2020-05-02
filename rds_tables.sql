--Create tables to store information about invidual experiments

DROP TABLE IF EXISTS experiments;

CREATE TABLE IF NOT EXISTS experiments (
    experiment_id varchar(100) NOT NULL PRIMARY KEY,
    client_id varchar(50) NOT NULL,
    experiment_name varchar(100),
    page_type varchar(50),
    experiment_type varchar(50)
);

CREATE INDEX IF NOT EXISTS experiments_client_idx ON experiments (client_id);
CREATE INDEX IF NOT EXISTS experiments_experiment_idx ON experiments (experiment_id);

--Create tables to store information about invidual clients

DROP TABLE IF EXISTS clients;

CREATE TABLE IF NOT EXISTS clients (
    client_id varchar(100) NOT NULL PRIMARY KEY,
    full_name varchar(100) NOT NULL,
    company_name varchar(100) NOT NULL,
    hashed_password varchar(100) NOT NULL,
    disabled boolean NOT NULL,
    shopify_app_api_key varchar(100) ,
    shopify_app_password varchar(100) ,
    shopify_app_eg_url varchar(200) ,
    shopify_app_shared_secret varchar(100)
);

CREATE INDEX IF NOT EXISTS clients_idx ON clients (client_id);

--Create tables to store information about invidual variations

DROP TABLE IF EXISTS variations;

CREATE TABLE IF NOT EXISTS variations (
    variation_id varchar(100) NOT NULL PRIMARY KEY,
    client_id varchar(50) NOT NULL,
    experiment_id varchar(100) NOT NULL,
    traffic_percentage int,
    s3_html_location varchar(200)
);

CREATE INDEX IF NOT EXISTS variations_client_idx ON variations (client_id);
CREATE INDEX IF NOT EXISTS variations_experiment_idx ON variations (experiment_id);
CREATE INDEX IF NOT EXISTS variations_variation_idx ON variations (variation_id);
