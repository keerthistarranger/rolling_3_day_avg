with summary as (
select 
distinct
date(transaction_time) as transaction_date,
sum(transaction_amount) over(partition by date(transaction_time)) as amt_per_day,
count(*) over(partition by date(transaction_time)) as count_per_day
from transactions),
final_cte as (
select 
transaction_date,
sum(amt_per_day)  over (order by transaction_date rows between 2 preceding and current row) as total_amt,
sum(count_per_day)  over (order by transaction_date rows between 2 preceding and current row) as total_count
from summary
order by transaction_date desc)
select 
transaction_date,
total_amt,
total_count,
total_amt/total_count as rolling_3_day_avg
from final_cte
order by transaction_date desc
