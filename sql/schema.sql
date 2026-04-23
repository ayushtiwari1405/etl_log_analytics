CREATE TABLE IF NOT EXISTS etl_results (
    id SERIAL PRIMARY KEY,
    pipeline VARCHAR(50),
    query_name VARCHAR(10),
    run_id VARCHAR(50),
    batch_id INT,
    key TEXT,
    value1 FLOAT,
    value2 FLOAT,
    value3 FLOAT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
