CREATE TABLE IF NOT EXISTS market_data (
    time BIGINT PRIMARY KEY,
    open NUMERIC(14,8),
    high NUMERIC(14,8),
    low NUMERIC(14,8),
    close NUMERIC(14,8),
    volume NUMERIC(12,8)
);