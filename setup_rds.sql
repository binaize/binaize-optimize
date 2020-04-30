CREATE TABLE IF NOT EXISTS experiment (
    client_id varchar(50),
    experiment_id varchar(50),
    variation_list text[]);

CREATE INDEX IF NOT EXISTS experiment_idx ON experiment (client_id, experiment_id);

CREATE TABLE IF NOT EXISTS clients (
    client_id varchar(50) NOT NULL PRIMARY KEY,
    full_name varchar(50) NOT NULL,
    company_name varchar(50) NOT NULL,
    hashed_password varchar(100) NOT NULL,
    disabled boolean NOT NULL);

CREATE INDEX IF NOT EXISTS client_id_idx ON clients (client_id);
