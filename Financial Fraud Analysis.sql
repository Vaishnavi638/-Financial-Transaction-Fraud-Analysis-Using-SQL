create database fraud_db;

use fraud_db;

drop table transactions;

CREATE TABLE transactions (
	id int auto_increment primary key,
    step VARCHAR(10),
    customer VARCHAR(50),
    age VARCHAR(10),
    gender VARCHAR(10),
    zipcodeOri VARCHAR(10),
    merchant VARCHAR(50),
    zipMerchant VARCHAR(10),
    category VARCHAR(50),
    amount VARCHAR(10),
    fraud VARCHAR(5)
);

set global local_infile = 1;

show variables like 'secure_file_priv';

LOAD DATA LOCAL INFILE 'C:/ProgramData/MySQL/MySQL Server 8.0/Uploads/Financial_Payment_dataset.csv'
INTO TABLE transactions
FIELDS TERMINATED BY ','
ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 ROWS
(step, customer, age, gender, zipcodeOri, merchant, zipMerchant, category, amount, fraud )  ; 

select count(*) from transactions;

select * from transactions limit 25;

  --    Data Cleaning  --    

-- real data is inconsistent and it have issue in data format
-- handling it may takes lot of time or may raised error 
-- best and smart way is to create new table

CREATE TABLE transactions_clean AS
SELECT 
    *,
    
    CASE 
        WHEN REPLACE(age, "'", "") REGEXP '^[0-9]+$' 
        THEN CAST(REPLACE(age, "'", "") AS UNSIGNED)
        ELSE NULL
    END AS age_clean,

    REPLACE(gender, "'", "") AS gender_clean,
    REPLACE(customer, "'", "") AS customer_clean,
    REPLACE(merchant, "'", "") AS merchant_clean,
    REPLACE(category, "'", "") AS category_clean,

    CAST(REPLACE(amount, "'", "") AS DECIMAL(10,2)) AS amount_clean,

CASE 
        WHEN REPLACE(fraud, "'", "") REGEXP '^[0-9]+$' 
        THEN CAST(REPLACE(fraud, "'", "") AS UNSIGNED)
        ELSE 0
    END AS fraud_clean
FROM transactions;

-- drop old columns
ALTER TABLE transactions_clean 
DROP COLUMN age,
DROP COLUMN gender,
DROP COLUMN customer,
DROP COLUMN merchant,
DROP COLUMN category,
DROP COLUMN amount,
DROP COLUMN fraud;

-- rename new columns
ALTER TABLE transactions_clean
CHANGE age_clean age INT,
CHANGE gender_clean gender CHAR(1),
CHANGE customer_clean customer VARCHAR(20),
CHANGE merchant_clean merchant VARCHAR(20),
CHANGE category_clean category VARCHAR(50),
CHANGE amount_clean amount DECIMAL(10,2),
CHANGE fraud_clean fraud TINYINT;

select * from transactions_clean limit 10;


    --      Analysis        --


-- Total Transaction and Fraud Rate
SELECT count(*) AS total_txns,
sum(fraud) AS fraud_txns,
ROUND(sum(fraud) * 100.0 / count(*), 2) AS fraud_rate
FROM transactions_clean ;
-- Result ->  total_txns: 594643, Fraud_txns: 7200,  fraud_rate: 1.21


-- Transaction distribution by category
SELECT category, COUNT(*) AS total_txns
FROM transactions_clean
GROUP BY category
ORDER BY total_txns DESC;
-- highest transactio categories are -> 'es_transportation','es_food','es_health', es_health


-- Fraud rate by category
SELECT category, COUNT(*) AS total_txns, 
SUM(fraud) AS fraud_txns,
ROUND(SUM(fraud)*100.0/COUNT(*),2) AS fraud_rate
FROM transactions_clean
GROUP BY category
ORDER BY fraud_rate DESC;
-- higher fraud rate categories are ->  'es_leisure':94.99, 'es_travel':79.40, 'es_sportsandtoys':49.53
-- leisure, travel, sportsandtoys are higher risk category





-- Fraud by gender
SELECT gender,
COUNT(*) AS total_txns,
SUM(fraud) AS fraud_txns,
ROUND(SUM(fraud)*100.0/COUNT(*),2) AS fraud_rate
FROM transactions_clean
GROUP BY gender;
-- Result -> M: 0.91 &  F: 1.47


-- Fraud by age group
SELECT 
age,
COUNT(*) AS total_txns,
SUM(fraud) AS fraud_txns,
ROUND(SUM(fraud)*100.0/COUNT(*),2) AS fraud_rate
FROM transactions_clean
GROUP BY age
ORDER BY fraud_rate DESC;
-- highest fraud rate  age ->   0: 1.96,  4: 1.29 , 2: 1.25
-- 0, 4, 2 age group is more vulnerable


-- fraud by amount
SELECT 
CASE 
    WHEN amount < 50 THEN 'Low'
    WHEN amount BETWEEN 50 AND 200 THEN 'Medium'
    ELSE 'High'
END AS amount_range,
COUNT(*) AS total_txns,
SUM(fraud) AS fraud_txns,
ROUND(SUM(fraud)*100.0/COUNT(*),2) AS fraud_rate
FROM transactions_clean
GROUP BY amount_range
ORDER BY fraud_rate DESC;
-- Fraud often happens in High amount ranges


-- Avg Amount (fraud vs non-fraud )
SELECT 
fraud,
AVG(amount) AS avg_amount
FROM transactions_clean
GROUP BY fraud;
-- 0: 31.847230  , 1: 530.926551
-- Fraudulent transactions are much more than legitimate transaction


-- High Risk Customers
SELECT 
customer,
COUNT(*) AS total_txns,
SUM(fraud) AS fraud_txns
FROM transactions_clean
GROUP BY customer
HAVING SUM(fraud) > 0
ORDER BY fraud_txns DESC
LIMIT 10;
-- C1350963410, C1849046345, C806399525, C2004941826, C1275518867  These are top 5 High Risk customers
-- Fraud often happens with these customers


-- Customers with high transaction frequency
SELECT 
customer,
COUNT(*) AS txn_count
FROM transactions_clean
GROUP BY customer
ORDER BY txn_count DESC
LIMIT 10;
-- Top 3 Customers which have high transaction frequency  ->  C1978250683: 265, C1275518867: 252, C806399525: 237


-- High-risk merchants
SELECT merchant,
COUNT(*) AS total_txns,
SUM(fraud) AS fraud_txns,
ROUND(SUM(fraud)*100.0/COUNT(*),2) AS fraud_rate
FROM transactions_clean
GROUP BY merchant
HAVING COUNT(*) > 100
ORDER BY fraud_rate DESC
LIMIT 10;
-- M1294758098, M3697346, M1873032707, M732195782, M980657600 These are high Risk Merchants 
-- Fraud rate is very high for these merchants


-- fraud trend over time
SELECT 
step,
COUNT(*) AS total_txns,
SUM(fraud) AS fraud_txns
FROM transactions_clean
GROUP BY step
ORDER BY step;
-- fraud is evenly distributed over time

-- Top 20% customers contributing fraud)
SELECT customer, SUM(fraud) AS fraud_count
FROM transactions_clean
GROUP BY customer
ORDER BY fraud_count DESC
LIMIT 20;


-- Customer Behavior Shift (Fraud Before vs After)
SELECT 
customer,
AVG(CASE WHEN fraud = 0 THEN amount END) AS avg_normal_amt,
AVG(CASE WHEN fraud = 1 THEN amount END) AS avg_fraud_amt,
COUNT(*) AS total_txns
FROM transactions_clean
GROUP BY customer
HAVING COUNT(*) > 50
ORDER BY avg_fraud_amt DESC;



-- Rapid Transaction Pattern
SELECT 
customer,
step,
COUNT(*) AS txn_count,
SUM(fraud) AS fraud_txns
FROM transactions_clean
GROUP BY customer, step
HAVING COUNT(*) > 5
ORDER BY txn_count DESC;
-- No customer made > 5 transactions in the same step



-- Fraud Contribution Analysis
SELECT 
merchant,
SUM(fraud) AS fraud_count
FROM transactions_clean
GROUP BY merchant
ORDER BY fraud_count DESC
LIMIT 20;
-- M480139044, M980657600 these 2 merchants contribute majority of fraud.



