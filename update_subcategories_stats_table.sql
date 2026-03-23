select subcategory, sum(amount) as amt_in_subcat_alltime
from transactions
group by subcategory,
order by amt_in_subcat_alltime desc;


with stats as (select s.subcategory, sum(t.amount) as amt_in_subcat_alltime
from subcategories s left join transactions t on s.subcategory = t.subcategory
group by s.subcategory
order by amt_in_subcat_alltime desc)
update subcategories_stats 
set total_in_subcategory_over_alltime = amt_in_subcat_alltime::money
from stats
where subcategories_stats.subcategory = stats.subcategory;

select * from transactions where subcategory = 'Gifts & Donations';