-- transform the data in transactions_staged and put it into transactions_stage2 for review before appending to transactions

select * from transactions_stage2 order by random() limit 10;

-- validate that all negative transactions were categorized as debit
select * from transactions_stage2 where amount < 0 and type <> 'debit';

-- now update all amounts to be the absolute value
update transactions_stage2 set amount = abs(amount);

-- move over category values to subcategory
update transactions_stage2 set subcategory = category;

-- clean subcategory values a bit
select count(*) from transactions_stage2 where subcategory = 'Fast Food & Convenience';
update transactions_stage2 set subcategory = 'Fast Food' where subcategory = 'Fast Food & Convenience';

-- categorize by subcategory
-- preview changes first
SELECT 
  t.subcategory,
  t.category AS old_category,
  COALESCE(s.category, t.subcategory) AS new_category
FROM transactions_stage2 t
LEFT JOIN subcategories s ON t.subcategory = s.subcategory;

UPDATE transactions_stage2 t
SET category = COALESCE(s.category, t.subcategory)
FROM subcategories s
WHERE t.subcategory = s.subcategory OR s.subcategory IS NULL;

-- append to final transactions table
insert
	into
	transactions (person_name,
	transaction_date,
	cleaned_description,
	original_description,
	amount,
	transaction_type,
	category,
	subcategory,
	account_name)
	select
	owner,
	date,
	description,
	original_description,
	amount,
	type,
	category,
	subcategory,
	account_name
from
	transactions_stage2;

-- update data retroactively in transactions table
-- TODO: categorize all student loan payments
select
	*
from
	transactions
where
	account_name in 
		('DIR UNSUB STAFFORD', 'U.S. DEPARTMENT OF EDUCATION  Direct Loan - Subsidized - Fixed - U.S. DEPARTMENT OF EDUCATION - DEPT OF ED', 
		'Direct Loan - Subsidized', 'Direct Loan - Unsubsidized', 'DIRECT SUB STAFFORD', 'DIRECT UNSUB STAFFORD LOAN')
order by transaction_date desc;

select * from transactions t 
where account_name = 'U.S. DEPARTMENT OF EDUCATION  Direct Loan - Subsidized - Fixed - U.S. DEPARTMENT OF EDUCATION - DEPT OF ED'
and transaction_type = 'credit'
order by t.transaction_date  desc;

-- for student loan payments, update the subcategory to be 'Transfer to Loan Account', shared=False, strictly_shared=False date_updated to be today
update transactions t 
set subcategory = 'Transfer to Loan Account', shared=false, strictly_shared=false, date_updated = current_date
where account_name = 'U.S. DEPARTMENT OF EDUCATION  Direct Loan - Subsidized - Fixed - U.S. DEPARTMENT OF EDUCATION - DEPT OF ED'
and transaction_type = 'credit';

-- then find the associated transactions on the account I paid it from and update the subcategory to be 'Student Loan' as well
with student_loans_payments as (select * from transactions t 
where account_name = 'U.S. DEPARTMENT OF EDUCATION  Direct Loan - Subsidized - Fixed - U.S. DEPARTMENT OF EDUCATION - DEPT OF ED'
and transaction_type = 'credit')

select * from transactions where amount=37.59::money and transaction_type='debit';


-- then refresh the categories based on the updated subcategories

-- prepare transactions table for export to google sheets



