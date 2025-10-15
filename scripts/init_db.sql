-- Initialize Data Platform Database
-- This script runs automatically when PostgreSQL container starts

-- Create schemas for different data layers
CREATE SCHEMA IF NOT EXISTS raw_data;
CREATE SCHEMA IF NOT EXISTS staging;
CREATE SCHEMA IF NOT EXISTS analytics;
CREATE SCHEMA IF NOT EXISTS monitoring;

-- Create raw data tables
CREATE TABLE IF NOT EXISTS raw_data.sales_data (
    id SERIAL PRIMARY KEY,
    transaction_id VARCHAR(50),
    customer_id VARCHAR(50),
    product_id VARCHAR(50),
    quantity INTEGER,
    price DECIMAL(10,2),
    transaction_date TIMESTAMP,
    region VARCHAR(50),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Create staging tables
CREATE TABLE IF NOT EXISTS staging.sales_clean (
    id SERIAL PRIMARY KEY,
    transaction_id VARCHAR(50) NOT NULL,
    customer_id VARCHAR(50) NOT NULL,
    product_id VARCHAR(50) NOT NULL,
    quantity INTEGER CHECK (quantity > 0),
    price DECIMAL(10,2) CHECK (price > 0),
    transaction_date DATE NOT NULL,
    region VARCHAR(50) NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Create analytics tables
CREATE TABLE IF NOT EXISTS analytics.daily_sales_summary (
    id SERIAL PRIMARY KEY,
    date DATE NOT NULL,
    region VARCHAR(50) NOT NULL,
    total_revenue DECIMAL(12,2),
    total_transactions INTEGER,
    average_order_value DECIMAL(10,2),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(date, region)
);

-- Create monitoring tables for cost tracking
CREATE TABLE IF NOT EXISTS monitoring.pipeline_costs (
    id SERIAL PRIMARY KEY,
    pipeline_name VARCHAR(100) NOT NULL,
    execution_date TIMESTAMP NOT NULL,
    aws_cost_usd DECIMAL(10,4),
    compute_time_minutes INTEGER,
    records_processed INTEGER,
    cost_per_record DECIMAL(10,6),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Create indexes for performance
CREATE INDEX IF NOT EXISTS idx_sales_data_date ON raw_data.sales_data(transaction_date);
CREATE INDEX IF NOT EXISTS idx_sales_clean_date ON staging.sales_clean(transaction_date);
CREATE INDEX IF NOT EXISTS idx_daily_summary_date ON analytics.daily_sales_summary(date);

-- Insert sample data for testing
INSERT INTO raw_data.sales_data (transaction_id, customer_id, product_id, quantity, price, transaction_date, region) VALUES
('TXN001', 'CUST001', 'PROD001', 2, 25.99, '2024-01-15 10:30:00', 'North'),
('TXN002', 'CUST002', 'PROD002', 1, 149.99, '2024-01-15 11:45:00', 'South'),
('TXN003', 'CUST003', 'PROD001', 3, 25.99, '2024-01-15 14:20:00', 'East'),
('TXN004', 'CUST001', 'PROD003', 1, 75.50, '2024-01-16 09:15:00', 'North'),
('TXN005', 'CUST004', 'PROD002', 2, 149.99, '2024-01-16 16:30:00', 'West')
ON CONFLICT DO NOTHING;

-- Grant permissions
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA raw_data TO dataeng;
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA staging TO dataeng;
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA analytics TO dataeng;
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA monitoring TO dataeng;
GRANT USAGE ON ALL SEQUENCES IN SCHEMA raw_data TO dataeng;
GRANT USAGE ON ALL SEQUENCES IN SCHEMA staging TO dataeng;
GRANT USAGE ON ALL SEQUENCES IN SCHEMA analytics TO dataeng;
GRANT USAGE ON ALL SEQUENCES IN SCHEMA monitoring TO dataeng;

-- Drop old staging.customer_profiles if exists
DROP TABLE IF EXISTS staging.customer_profiles CASCADE;

-- Raw customers from API (no cleaning)
CREATE TABLE IF NOT EXISTS staging.raw_customers (
    customer_id INTEGER PRIMARY KEY,
    first_name VARCHAR(100),
    last_name VARCHAR(100),
    email VARCHAR(255),
    phone VARCHAR(50),
    birth_date VARCHAR(50),
    age INTEGER,
    gender VARCHAR(20),
    
    city VARCHAR(100),
    state VARCHAR(100),
    country VARCHAR(100),
    postal_code VARCHAR(50),
    full_address TEXT,
    
    company_name VARCHAR(255),
    job_title VARCHAR(255),
    department VARCHAR(100),
    university VARCHAR(255),
    
    card_type VARCHAR(50),
    card_last_4 VARCHAR(4),
    iban_country VARCHAR(2),
    
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Raw orders (flattened from carts)
CREATE TABLE IF NOT EXISTS staging.raw_orders (
    order_id SERIAL PRIMARY KEY,
    cart_id INTEGER,
    customer_id INTEGER,
    product_id INTEGER,
    product_name VARCHAR(255),
    product_category VARCHAR(100),
    quantity INTEGER,
    price NUMERIC(10,2),
    discount_percentage NUMERIC(5,2),
    line_total NUMERIC(10,2),
    
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    FOREIGN KEY (customer_id) REFERENCES staging.raw_customers(customer_id)
);

-- Fraud scores from Python
CREATE TABLE IF NOT EXISTS staging.fraud_scores (
    fraud_score_id SERIAL PRIMARY KEY,
    customer_id INTEGER,
    
    composite_risk_score NUMERIC(3,2),
    risk_level VARCHAR(20),
    
    velocity_risk NUMERIC(3,2),
    geographic_risk NUMERIC(3,2),
    behavioral_risk NUMERIC(3,2),
    profile_risk NUMERIC(3,2),
    amount_risk NUMERIC(3,2),
    temporal_risk NUMERIC(3,2),
    
    fraud_indicators_json JSONB,
    ml_features_json JSONB,
    
    analyzed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    FOREIGN KEY (customer_id) REFERENCES staging.raw_customers(customer_id)
);

-- Indexes for performance
CREATE INDEX IF NOT EXISTS idx_raw_orders_customer ON staging.raw_orders(customer_id);
CREATE INDEX IF NOT EXISTS idx_raw_orders_cart ON staging.raw_orders(cart_id);
CREATE INDEX IF NOT EXISTS idx_fraud_scores_customer ON staging.fraud_scores(customer_id);
CREATE INDEX IF NOT EXISTS idx_fraud_scores_risk_level ON staging.fraud_scores(risk_level);