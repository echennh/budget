---- LOAD NEW DATA ----
-- Categorize transactions by owner


-- Categorize all fidelity_transaction_type=expenses to transaction_type = debit
-- and all fidelity_transaction_type = Income to transaction_type = credit
-- TODO: figure out what I want to do with transfers
-- Make sure that no negative credit transactions exist

-- Recategorize transactions to my own categories and subcategories

-- Create account table with owners
-- Clean account name values

-- Itemize transactions

-- Clean labels values

-- Clean merchants values

-- Update shared column values

-- Clean tags values

-- create amount_owed_to_partner column
-- update values


CREATE TABLE mart.transactions (
row_id serial,
owner text,
transaction_date date,
amount numeric,
fidelity_transaction_type text,
transaction_type text,
account_name text, 
fidelity_category text,
category text,
fidelity_subcategory text,
subcategory text,
original_description text,
description text,
itemized boolean,
shared boolean,
amount_owed_to_partner numeric,
merchant text,
labels text,
tags text
);

-- TODO: figure out upsert logic

insert into mart.transactions (transaction_date, amount, fidelity_transaction_type, account_name, fidelity_category, fidelity_subcategory, original_description, itemized)
select transaction_date,
amount,
transaction_type,
account_name,
category,
subcategory,
description,
false
from staging.transactions;

select count(*), count(row_id)
from mart.transactions;

select fidelity_transaction_type, amount>0 as net_positive, count(*) as cnt
from mart.transactions
group by fidelity_transaction_type, net_positive
order by cnt desc;

update mart.transactions
set transaction_type = 'debit'
where amount < 0;

update mart.transactions
set transaction_type = 'credit'
where amount > 0;


-- TODO: Sort out these 29 transactions that don't make sense because they're categorized as expenses but are net positives
select * from mart.transactions t  where fidelity_transaction_type = 'Expenses' and amount > 0 order by amount desc;

update mart.transactions 
set amount = abs(amount);

--- MERGE NEW DATA WITH HISTORICAL DATA ----
select min(transaction_date::date), max(transaction_date::date) from mart.transactions;
-- 2024-03-25	2026-03-23

select min(transaction_date::date), max(transaction_date::date) from staging.historical_transactions;
-- 2018-01-31	2025-05-13

-- load in data from 2018-01-31 to 2024-03-24 as-is
-- overlap is from 2024-03-25 to 2025-05-13
-- can apply new logic to all the new data that hasn't been enriched yet (2025-05-13 to current date)

-- clean up the historical_transactions data a bit to standardize
create schema silver;
-- standardize transaction_date

select count(*) from staging.historical_transactions ht;

select count(*)
from staging.historical_transactions ht where transaction_date ~ '^\d{4}-\d{2}-\d{2}$';

-- thankfully, all 3,811 rows are in the format YYYY-mm-dd, so they should convert just fine.
-- and I double checked in my sandbox, if any of the values are empty string, 0000-00-00, or YYYY-dd-MM, then the *whole* update statement will be aborted 
-- because of `'ERROR: date/time field value out of range`
-- which is what I would want.


create table silver.historical_transactions (
    transaction_owner text,
    transaction_date date,
    description text,
    original_description text,
    amount numeric,
    transaction_type text,
    category text,
    subcategory text,
    account_name text,
    itemized boolean,
    labels text,
    merchant text,
    shared boolean,
    tags text,
    amount_owed_to_partner numeric,
    row_id integer,
    serial_id integer
);
insert into silver.historical_transactions
    (transaction_owner,
    transaction_date,
    description,
    original_description,
    amount,
    transaction_type,
    category,
    subcategory,
    account_name,
    itemized,
    labels,
    merchant,
    shared,
    tags,
    amount_owed_to_partner,
    row_id,
    serial_id)
select 
transaction_owner,
    transaction_date::date,
    description,
    original_description,
    amount,
    transaction_type,
    category,
    subcategory,
    account_name,
    itemized,
    labels,
    merchant,
    shared,
    tags,
    amount_owed_to_partner,
    row_id,
    serial_id
from staging.historical_transactions;

select * from silver.historical_transactions order by random() limit 10;

drop table overlap_enriched;
drop table overlap_unenriched;
create temporary table overlap_enriched as select * from silver.historical_transactions where transaction_date >= '2024-03-25' and transaction_date <= '2025-05-13';
create temporary table overlap_unenriched as select * from mart.transactions where transaction_date >= '2024-03-25' and transaction_date <= '2025-05-13';


select * from overlap_enriched order by random() limit 1;

-- merge the rows on date and amount
create temporary table overlap_sidebyside as 
select enrich.row_id as row_id_enriched,
unenrich.row_id as row_id_raw,
unenrich.row_id as row_id_final,
enrich.transaction_owner as transaction_owner_enriched,
unenrich.owner as transaction_owner_raw,
coalesce(enrich.transaction_owner,unenrich.owner ) as owner_final,
enrich.transaction_date as transaction_date,
enrich.amount as amount,
unenrich.fidelity_transaction_type as transaction_type_raw,
enrich.transaction_type as transaction_type_enriched,
unenrich.transaction_type as transaction_type_unenriched,
coalesce(enrich.transaction_type, unenrich.transaction_type) as transaction_type_final,
enrich.account_name as account_name_enriched,
unenrich.account_name as account_name_raw,
coalesce(enrich.account_name, unenrich.account_name) as account_name_final,
-- if itemized, then prefer the category and subcategory from the enriched data
-- else (not itemized), then prefer the category and subcategory from the unenriched data, because my preference goes
-- 1. I've itemized (best)
-- 2. Fidelity's auto categorization
-- 3. my previous auto categorization/old version of same data from Fidelity
enrich.category as category_enriched,
unenrich.fidelity_category as fidelity_category_raw,
case 
	when enrich.itemized
	then coalesce(enrich.category, unenrich.fidelity_category)
	else coalesce(unenrich.fidelity_category, enrich.category)
end as category_final,
enrich.subcategory as subcategory_enriched,
unenrich.fidelity_subcategory as fidelity_subcategory_raw,
case 
	when enrich.itemized
	then coalesce(enrich.subcategory, unenrich.fidelity_subcategory)
	else coalesce(unenrich.fidelity_subcategory, enrich.subcategory)
end as subcategory_final,
enrich.original_description as original_description_enriched,
unenrich.original_description as original_description_raw,
coalesce(enrich.original_description, unenrich.original_description) as original_description_final,
enrich.description as description_enriched,
enrich.itemized as itemized_enriched,
unenrich.itemized as itemized_raw,
coalesce(enrich.itemized, unenrich.itemized) as itemized_final,
enrich.shared as shared_enriched,
unenrich.shared as shared_raw,
coalesce(enrich.shared, unenrich.shared) as shared_final,
enrich.amount_owed_to_partner as amount_owed_to_partner_enriched,
unenrich.amount_owed_to_partner as amount_owed_to_partner_raw,
coalesce(enrich.amount_owed_to_partner, unenrich.amount_owed_to_partner) as amount_owed_to_partner_final,
enrich.merchant as merchant_enriched,
unenrich.merchant as merchant_raw,
coalesce(enrich.merchant, unenrich.merchant) as merchant_final,
enrich.labels as labels_enriched,
unenrich.labels as labels_raw,
coalesce(enrich.labels, unenrich.labels) as labels_final,
enrich.tags as tags_enriched,
unenrich.tags as tags_raw,
coalesce(enrich.tags, unenrich.tags) as tags_final
from overlap_enriched enrich
full outer join overlap_unenriched unenrich
on enrich.transaction_date = unenrich.transaction_date and enrich.amount = unenrich.amount;


-- prefer the account name in the enriched data
-- fill out the category and subcategory in the unenriched data with what it is in the enriched dataset
-- keep the row_id in the unenriched data
-- TODO: add rule that if 'xbox' is in description, to assign to some subcategory like 'Games'

select * from overlap_sidebyside order by random() limit 10;

-- figure out how many rows weren't merged after the full outer join







