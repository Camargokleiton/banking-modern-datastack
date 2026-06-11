--DROP TABLE customer CASCADE;
--DROP TABLE accounts CASCADE;
--DROP TABLE transactions CASCADE;

CREATE TABLE customers(
    customer_id SERIAL PRIMARY KEY,
    customer_first_name VARCHAR(100),
    customer_last_name VARCHAR(100),
    customer_email VARCHAR(100),
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

CREATE TABLE accounts (
    account_id SERIAL PRIMARY KEY,
    customer_id INT NOT NULL REFERENCES customers(customer_id) ON DELETE CASCADE,        
    account_type VARCHAR(50) NOT NULL,
    balance NUMERIC (18,2) NOT NULL DEFAULT 0 CHECK (balance >= 0),            
    currency CHAR(3) NOT NULL DEFAULT 'USD',
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()

);

CREATE TABLE transactions (
    transactions_id BIGSERIAL PRIMARY KEY,        
    account_id INT NOT NULL REFERENCES accounts(account_id) ON DELETE CASCADE,
    txn_type VARCHAR(50) NOT NULL,            
    amount NUMERIC (18,2) NOT NULL DEFAULT 0 CHECK (amount > 0),
	related_account_id INT NULL,
	status VARCHAR(50) NOT NULL DEFAULT 'COMPLETED',
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

select * from accounts;

