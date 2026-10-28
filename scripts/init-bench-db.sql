-- Create benchmark database and user
CREATE USER benchuser WITH PASSWORD 'benchpass';
CREATE DATABASE benchdb OWNER benchuser;

-- Grant privileges
GRANT ALL PRIVILEGES ON DATABASE benchdb TO benchuser;

-- Connect to benchdb and set up schema
\c benchdb benchuser

-- Create benchmark tables
CREATE TABLE IF NOT EXISTS bench_insert_traditional (
    id SERIAL PRIMARY KEY,
    name VARCHAR(100),
    value INTEGER,
    created_at TIMESTAMP,
    data TEXT
);

CREATE TABLE IF NOT EXISTS bench_insert_adbc (
    id SERIAL PRIMARY KEY,
    name VARCHAR(100),
    value INTEGER,
    created_at TIMESTAMP,
    data TEXT
);

CREATE TABLE IF NOT EXISTS bench_source (
    id SERIAL PRIMARY KEY,
    product_name VARCHAR(100),
    category VARCHAR(50),
    price NUMERIC(10, 2),
    quantity INTEGER,
    created_at TIMESTAMP
);

CREATE TABLE IF NOT EXISTS bench_dest_traditional (
    id SERIAL PRIMARY KEY,
    product_name VARCHAR(100),
    category VARCHAR(50),
    price NUMERIC(10, 2),
    quantity INTEGER,
    created_at TIMESTAMP
);

CREATE TABLE IF NOT EXISTS bench_dest_adbc (
    id SERIAL PRIMARY KEY,
    product_name VARCHAR(100),
    category VARCHAR(50),
    price NUMERIC(10, 2),
    quantity INTEGER,
    created_at TIMESTAMP
);

CREATE TABLE IF NOT EXISTS bench_op_sql_traditional (
    id SERIAL PRIMARY KEY,
    product_name VARCHAR(100),
    category VARCHAR(50),
    price NUMERIC(10, 2),
    quantity INTEGER,
    created_at TIMESTAMP
);

CREATE TABLE IF NOT EXISTS bench_op_sql_adbc (
    id SERIAL PRIMARY KEY,
    product_name VARCHAR(100),
    category VARCHAR(50),
    price NUMERIC(10, 2),
    quantity INTEGER,
    created_at TIMESTAMP
);

CREATE TABLE IF NOT EXISTS bench_op_xfer_traditional (
    id SERIAL PRIMARY KEY,
    product_name VARCHAR(100),
    category VARCHAR(50),
    price NUMERIC(10, 2),
    quantity INTEGER,
    created_at TIMESTAMP
);

CREATE TABLE IF NOT EXISTS bench_op_xfer_adbc (
    id SERIAL PRIMARY KEY,
    product_name VARCHAR(100),
    category VARCHAR(50),
    price NUMERIC(10, 2),
    quantity INTEGER,
    created_at TIMESTAMP
);

-- Grant all privileges to benchuser
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA public TO benchuser;
GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA public TO benchuser;
