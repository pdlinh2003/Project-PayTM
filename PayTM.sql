/* 1. Retrieve a report that includes the following information: customer_id, transaction_id, scenario_id, transaction_type, category, payment_method. 
These transactions must meet the following conditions: 
Were created from Jan to June 2019
Had category type is shopping
Were paid by Bank accounl*/

SELECT customer_id
    , transaction_id
    , fact19.scenario_id
    , transaction_type
    , category
    , payment_method
    , Transaction_Time 
FROM fact_transaction_2019 AS fact19 
LEFT JOIN dim_scenario AS sce
ON fact19.scenario_id = sce.scenario_id
LEFT JOIN dim_payment_channel AS channel
ON fact19.payment_channel_id = channel.payment_channel_id
WHERE Month(Transaction_Time ) BETWEEN 01 AND 06
    AND Category = 'Shopping'
    AND payment_method = 'Bank account'

/* 2. In the fact_transaction_2019 data table, each successful transaction corresponds to a different category. 
Calculate the number of transactions of each category to make reporting easier.
*/

SELECT category
    , count(transaction_id) AS number_trans
FROM fact_transaction_2019 AS fact19
LEFT JOIN dim_scenario AS sce
ON fact19.scenario_id = sce.scenario_id
LEFT JOIN dim_status AS stt
ON fact19.status_id = stt.status_id
WHERE status_description = 'success'
GROUP BY category
 
/* 3. Please indicate the total number of transactions of each transaction_type, provided: successful transaction, transaction time in the first 3 months of 2019
Calculate the ratio of the number of transactions of each type to the total number of transactions in 3 months.*/

WITH cte AS(
    SELECT transaction_type
        , count(transaction_id) as num_trans
        , (SELECT count(transaction_id) FROM fact_transaction_2019 WHERE month(transaction_time) <=3
        AND status_id = 1) total
    FROM fact_transaction_2019 as fact19
    LEFT JOIN dim_scenario as sce
    ON fact19.scenario_id = sce.scenario_id
    WHERE month(transaction_time) <=3
        AND status_id = 1
    GROUP BY transaction_type
)
SELECT transaction_type
    , FORMAT( num_trans *1.0 / total, 'p') pct
FROM cte


/* 4. Retrieve a more detailed report with following information: transaction type, category, number of transactions and proportion of each category in the total of that transaction type. 
These transactions must meet the following conditions: 
Were created in 2019 
Were paid successful*/

WITH trans_table AS(
    SELECT transaction_id
    , transaction_type
    , category
FROM fact_transaction_2019 AS fact_19
LEFT JOIN dim_scenario AS sce 
ON fact_19.scenario_id = sce.scenario_id
LEFT JOIN dim_status AS status 
ON fact_19.status_id = status.status_id
WHERE status_description = 'success'
)
, total_trans_category AS(
    SELECT transaction_type
        , category
        , COUNT(transaction_id) AS num_trans_category
    FROM trans_table
    GROUP BY transaction_type, category
) 
, total_trans_type AS (
    SELECT transaction_type
    , COUNT(transaction_id) AS num_trans_type
FROM trans_table
GROUP BY transaction_type
)
SELECT total_trans_category.*
    , num_trans_type
    , CONVERT(nvarchar, CONVERT (decimal(10,2) , num_trans_category * 100.00/ num_trans_type))+ '%' as pct
FROM total_trans_category
LEFT JOIN total_trans_type
ON total_trans_category.transaction_type = total_trans_type.transaction_type 
ORDER BY transaction_type, pct 

--5.  Hãy tính xem số lượng giao dịch bị nghi ngờ gian lận của nhóm khách hàng nhận được ưu đãi là bao nhiêu


WITH customer_promoion_table AS (
    SELECT DISTINCT customer_id
    FROM fact_transaction_2019
    WHERE promotion_id <> '0'
    UNION 
    SELECT DISTINCT customer_id
    FROM fact_transaction_2020
    WHERE promotion_id <> '0'
)
, trans_suspect_table AS( 
    SELECT pro.customer_id
    ,COUNT(CASE WHEN status_description LIKE '%fraud%' THEN transaction_id END) AS number_trans_suspect
    ,COUNT(transaction_id) as total_trans_by_customer
FROM (
    SELECT transaction_id, customer_id, status_id FROM fact_transaction_2019
    UNION 
    SELECT transaction_id, customer_id, status_id FROM fact_transaction_2020) AS trans_table
LEFT JOIN dim_status sta ON trans_table.status_id = sta.status_id
JOIN customer_promoion_table pro ON trans_table.customer_id = pro.customer_id
GROUP BY pro.customer_id
)
SELECT customer_id
    ,number_trans_suspect
    ,total_trans_by_customer
    ,FORMAT(number_trans_suspect*1.0/total_trans_by_customer,'p') as suspect_pct
FROM trans_suspect_table
WHERE number_trans_suspect != 0


/* 6.  Phân loại khách hàng giao dịch thành công  thành 3 nhóm: 
Tổng số tiền giao dịch trên 5.000.000 là “New Customer”
Tổng số tiền giao dịch trên 10.000.000 là “Potential Customer”
Tổng số tiền giao dịch trên 50.000.000 là “Loyal Customer”
*/

WITH fact_table AS(
    SELECT * 
    FROM fact_transaction_2019 AS fact19
    WHERE fact19.status_id = 1
    UNION
    SELECT fact20.*
    FROM fact_transaction_2020 AS fact20
    WHERE fact20.status_id = 1
)
, charged_table AS (
    SELECT customer_id
        , sum(charged_amount* 1.0) AS total_charged
    FROM fact_table
    GROUP BY customer_id
) 
, label_table AS(
    SELECT *
    , CASE WHEN total_charged >= 5000000 AND total_charged < 10000000  THEN 'New Customer'
        WHEN total_charged > 10000000 AND total_charged < 50000000 THEN 'Potential Customer'
        ELSE  'Loyal Customer'
        END
    AS label_customer
    FROM charged_table
) 
SELECT label_customer
    , COUNT(customer_id) AS number_customer
    , (SELECT COUNT(DISTINCT customer_id) FROM fact_table) AS total_customer
    , CONVERT(varchar, COUNT(customer_id) * 100/ (SELECT COUNT(DISTINCT customer_id) FROM fact_table)) + '%'
FROM label_table
GROUP BY label_customer


-- 7. Find out the TOP 3 months with the most failed transactions of each year (using window function)

WITH failed_table AS(
    SELECT DISTINCT YEAR(transaction_time) AS [Year]
            , MONTH(transaction_time) AS [Month]
            , COUNT(transaction_id) OVER (PARTITION BY YEAR(transaction_time), MONTH(transaction_time) ) AS number_failed_trans
    FROM (SELECT * FROM fact_transaction_2019 WHERE status_id <> 1
            UNION
            SELECT * FROM fact_transaction_2020 WHERE status_id <> 1) AS fact_table
)
, Rank_table AS(
    SELECT * 
        , RANK() OVER(PARTITION BY [Year] ORDER BY number_failed_trans DESC) AS Rank
    FROM failed_table
)
SELECT * 
FROM Rank_table 
WHERE Rank < 4


-- 8. Calculate the average distance between successful payments per customer in Telecom group 2019.

WITH fact_table AS (
    SELECT customer_id, transaction_id, transaction_time as date
        , LAG(transaction_time, 1) OVER(PARTITION BY customer_id ORDER BY transaction_time) AS next_date
    FROM fact_transaction_2019 as fact_19
    inner join dim_scenario as sce
    On fact_19.scenario_id = sce.scenario_id
    WHERE category = 'Telco'
        AND status_id = 1
) 
SELECT DISTINCT customer_id
    , avg(datediff(day, next_date, date)) OVER(PARTITION BY customer_id) AS avg_gap_day
FROM fact_table
ORDER BY customer_id

-- 9. You know that there are many sub-categories of the Billing group. After reviewing the above result, you should break down the trend into each sub-categories.

WITH billing_ss AS(
    SELECT fact_table.*, sub_category
    FROM (SELECT* FROM fact_transaction_2019
        UNION 
        SELECT * FROM fact_transaction_2020) AS fact_table
    LEFT JOIN dim_scenario AS sce 
    ON fact_table.scenario_id = sce.scenario_id
    WHERE category = 'Billing'
        AND status_id = 1
)
SELECT DISTINCT Year(transaction_time) AS [Year]
    , Month(Transaction_time) AS [Month]
    , sub_category
    , COUNT(transaction_id) OVER (PARTITION BY Year(transaction_time), Month(Transaction_time), sub_category) AS number_trans
FROM billing_ss
ORDER BY Year, Month, sub_category


-- 8.2. Then modify the result as the following table: Only select the sub-categories belong to list (Electricity, Internet and Water)

WITH billing_ss AS(
    SELECT fact_table.*, sub_category
    FROM (SELECT* FROM fact_transaction_2019
        UNION 
        SELECT * FROM fact_transaction_2020) AS fact_table
    LEFT JOIN dim_scenario AS sce 
    ON fact_table.scenario_id = sce.scenario_id
    WHERE category = 'Billing'
        AND status_id = 1
        AND sub_category IN('Electricity', 'Internet','Water')
)
SELECT DISTINCT year(transaction_time) AS [Year]
    , MONTH(transaction_time) AS [Month]
    , COUNT(IIF(sub_category = 'Electricity', transaction_id, null)) OVER (PARTITION BY  year(transaction_time), MONTH(transaction_time)) AS Electricity_trans
    , COUNT(IIF(sub_category = 'Internet', transaction_id, null)) OVER (PARTITION BY  year(transaction_time), MONTH(transaction_time)) AS Internet_trans
    , COUNT(IIF(sub_category = 'Water', transaction_id, null)) OVER (PARTITION BY  year(transaction_time), MONTH(transaction_time)) AS Water_trans
FROM billing_ss
ORDER BY Year, Month

-- 9. Hãy tính trung bình cộng số lượng KH thành công đó mỗi 4 tuần (trung bình cộng của 4 tuần gần nhất)


WITH billing_ss AS(
    SELECT year(transaction_time) AS [Year]
        , DATEPART(Week, transaction_time) AS [Week]
        , customer_id
    FROM (SELECT* FROM fact_transaction_2019
        UNION 
        SELECT * FROM fact_transaction_2020) AS fact_table
    LEFT JOIN dim_scenario AS sce 
    ON fact_table.scenario_id = sce.scenario_id
    WHERE category = 'Billing'
        AND status_id = 1
)
, week_trans AS(
    SELECT [Year], [Week]
        , COUNT( distinct customer_id) AS number_cus
    FROM billing_ss
    GROUP BY [Year], [Week]
) 
SELECT year, week
    , avg(number_cus) OVER (ORDER BY [year], [week] ROWS BETWEEN 3 PRECEDING AND CURRENT ROW ) AS avg_last_4_week
FROM week_trans

/* 10. You want to evaluate the quality of user acquisition category Billing in Jan 2019 by the
retention metric. First, you need to know how many users are retained in each subsequent month
from the first month (Jan 2019) they pay the successful transaction (only get data of 2019).
*/

WITH fact_table AS(
    SELECT fact_19.*
    FROM fact_transaction_2019 fact_19
    JOIN dim_scenario AS sce 
    ON fact_19.scenario_id = sce.scenario_id
    WHERE category = 'Billing'
        AND status_id = '1'
)
, customer_in_Jan AS(
    SELECT DISTINCT customer_id 
    FROM fact_table
    WHERE MONTH(transaction_time) = 1
)
, customer_year AS(
    SELECT MONTH(transaction_time) AS [Month], customer_id
    FROM fact_table
    WHERE customer_id IN (SELECT customer_id FROM customer_in_Jan)
)
SELECT IIF( LAG([Month], 1) OVER (ORDER BY month) IS NULL, 0, LAG([Month], 1) OVER (ORDER BY month) ) AS subsequent_month
    , COUNT( DISTINCT customer_id) AS retained_users
FROM customer_year
GROUP BY month

-------------- cach 2


WITH fact_table AS(
SELECT customer_id, transaction_id, transaction_time
    , MIN ( MONTH (transaction_time) ) OVER ( PARTITION BY customer_id ) AS first_month
    , DATEDIFF ( month, MIN ( transaction_time ) OVER ( PARTITION BY customer_id ) , transaction_time ) AS subsequent_month
FROM fact_transaction_2019 fact_19
JOIN dim_scenario AS sce
ON fact_19.scenario_id = sce.scenario_id
WHERE category = 'Billing'
    AND status_id = 1
)
SELECT subsequent_month
    , COUNT (DISTINCT customer_id) AS retained_customers
FROM fact_table
WHERE first_month = 1
GROUP BY subsequent_month

/* 11.
You realize that the number of retained customers has decreased over time. Let’s calculate retention
= number of retained customers / total users of the first month.
*/

WITH fact_table AS(
    SELECT fact_19.*
    FROM fact_transaction_2019 fact_19
    JOIN dim_scenario AS sce 
    ON fact_19.scenario_id = sce.scenario_id
    WHERE category = 'Billing'
        AND status_id = '1'
)
, customer_in_Jan AS(
    SELECT customer_id 
    FROM fact_table
    WHERE MONTH(transaction_time) = 1
)
, customer_year AS(
    SELECT fact_table.*
    FROM customer_in_Jan
    JOIN fact_table
    ON customer_in_Jan.customer_id = fact_table.customer_id
)
SELECT (month(transaction_time)-1) AS subsequent_month
    , COUNT( DISTINCT customer_id) AS retained_users
    , (SELECT COUNT(DISTINCT customer_id) FROM customer_in_Jan) AS original_users
    , FORMAT( COUNT( DISTINCT customer_id) * 1.0 / (SELECT COUNT( DISTINCT customer_id) FROM customer_in_Jan), 'p') AS pct
FROM customer_year
GROUP BY  (month(transaction_time)-1)


/* 12. Cohorts Derived from the Time Series Itself
Task A: Expand your previous query to calculate retention for multi attributes from the acquisition
month (first month) (from Jan to December).
*/


WITH fact_table AS(
    SELECT fact_19.*
    FROM fact_transaction_2019 fact_19
    JOIN dim_scenario AS sce 
    ON fact_19.scenario_id = sce.scenario_id
    WHERE category = 'Billing'
        AND status_id = '1'
)
, cus_table AS(
    SELECT DISTINCT customer_id
        , MIN( MONTH(transaction_time)) OVER (PARTITION BY customer_id) AS acquisition_month
        , DATEDIFF( month, MIN(transaction_time) OVER (PARTITION BY customer_id) , transaction_time) AS subsequent_month
    FROM fact_table
)
, retained_table AS(
    SELECT DISTINCT acquisition_month, subsequent_month
        , COUNT(customer_id) OVER (PARTITION BY acquisition_month, subsequent_month ) AS  retained_users
    FROM cus_table
)
SELECT *
    , MAX(retained_users) OVER(PARTITION BY acquisition_month) AS original_users
    , FORMAT(retained_users * 1.0 / MAX(retained_users) OVER(PARTITION BY acquisition_month), 'p') AS pct
FROM retained_table
-- ORDER BY acquisition_month, subsequent_month


--13: pivot table


WITH fact_table AS(
    SELECT fact_19.*
    FROM fact_transaction_2019 fact_19
    JOIN dim_scenario AS sce 
    ON fact_19.scenario_id = sce.scenario_id
    WHERE category = 'Billing'
        AND status_id = '1'
)
, cus_table AS(
    SELECT DISTINCT customer_id
        , MIN( MONTH(transaction_time)) OVER (PARTITION BY customer_id) AS acquisition_month
        , DATEDIFF( month, MIN(transaction_time) OVER (PARTITION BY customer_id) , transaction_time) AS subsequent_month
    FROM fact_table
)
, retained_table AS(
    SELECT DISTINCT acquisition_month, subsequent_month
        , COUNT(customer_id) OVER (PARTITION BY acquisition_month, subsequent_month ) AS  retained_users
    FROM cus_table
)
, pct_table AS(
    SELECT *
        , MAX(retained_users) OVER(PARTITION BY acquisition_month) AS original_users
        , FORMAT(retained_users * 1.0 / MAX(retained_users) OVER(PARTITION BY acquisition_month) , 'p') AS pct
    FROM retained_table
)
SELECT acquisition_month, original_users
    , [0], [1], [2], [3], [4], [5], [6], [7], [8], [9], [10], [11]
FROM ( 
   SELECT acquisition_month, original_users, pct, subsequent_month from pct_table
 ) AS original_table
PIVOT (
    min(pct)
FOR subsequent_month IN( [0], [1], [2], [3], [4], [5], [6], [7], [8], [9], [10], [11])
) AS pivoted_table
ORDER BY acquisition_month

/*
14. The first step in building an RFM model is to assign Recency, Frequency and Monetary values
to each customer. Let’s calculate these metrics for all successful paying customer of ‘Telco Card’
in 2019 and 2020:
● Recency: Difference between each customer's last payment date and '2020-12-31'
● Frequency: Number of successful payment days of each customer
● Monetary: Total charged amount of each customer
*/

WITH fact_table AS (
    SELECT fact_1920.*
    FROM( SELECT *
        FROM fact_transaction_2019
        UNION
        SELECT *
        FROM fact_transaction_2020) AS fact_1920
    JOIN dim_scenario AS sce 
    ON fact_1920.scenario_id = sce.scenario_id
    WHERE status_id ='1'
        AND sub_category ='Telco Card'
)
SELECT DISTINCT customer_id
    , DATEDIFF(day, max(transaction_time) OVER (PARTITION BY customer_id), '2022-12-31') AS Recency
    , COUNT( Transaction_id) OVER(PARTITION BY customer_id) AS Frequency
    , SUM(charged_amount) OVER (PARTITION BY customer_id) AS Monetary
FROM fact_table
