-- get the most recent date in the transactions table and all of the transactions on that day
-- so I know what date to pull my new extract in starting from
with most_recent_trx_date as (SELECT max(transaction_date) as most_recent_date FROM transactions)
select * from transactions, most_recent_trx_date  where transaction_date = most_recent_trx_date.most_recent_date;
