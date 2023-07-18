USE project_3;

## A. Calculate the number of jobs reviewed per hour per day for November 2020?
SELECT 
    DATE(ds) AS review_date,
    round(COUNT(job_id) / (SUM(time_spent) / 3600)) AS jobs_reviewed_per_hour
FROM
    job_data
WHERE
    MONTH(ds) = 11 AND YEAR(ds) = 2020
GROUP BY review_date;


## B. Calculate 7 day rolling average of throughput?

with throughput_data as(
		select 
			ds, 
			count(event)/sum(time_spent)as throughput
            from job_data 
            group by ds 
            order by ds
            )
	select 
		ds, 
        avg(throughput) over(order by ds rows between 6 preceding and current row) as 7day_rolling_avg
	from throughput_data;



## C. Calculate the percentage share of each language in the last 30 days?

select language, count(job_id) as num_of_jobs, count(job_id)*100/sum(count(*)) over() as percent_share
from job_data
where ds between '2020-11-01' and '2020-11-30'
group by language;

## D. How will you display duplicates from the table?

select dupli_data.ds, dupli_data.job_id, dupli_data.actor_id, dupli_data.event, dupli_data.language, dupli_data.time_spent, dupli_data.org,
case when dupli_data.duplicates = 1 then 'No duplicate' else 'Duplicate' end as 'duplicacy'
from (select *,
row_number() over(partition by ds, job_id, actor_id, event, language, time_spent, org) as duplicates 
from job_data) dupli_data;

alter table events
modify column user_type varchar(20);


LOAD DATA INFILE "C:/ProgramData/MySQL/MySQL Server 8.0/Uploads/Table-2 events.csv"
INTO TABLE events
FIELDS TERMINATED BY ','
LINES TERMINATED BY '\n'
IGNORE 1 ROWS;

## 2-A. Calculate the weekly user engagement?
SELECT week(occurred_at) as num_week,
COUNT(distinct(user_id)) as num_users
FROM events
WHERE event_type = 'engagement' 
GROUP BY num_week;

## 2-B.  Calculate the user growth for product?

select growth_data.year, growth_data.quarter,
growth_data.active_user_count,
active_user_count - lag(active_user_count,1) over(order by year, quarter) as growth_count
from (
select year(created_at) as year,
quarter(created_at) as quarter,
count(distinct(user_id)) as active_user_count
from users
where state = 'active' and activated_at is not null
group by year, quarter) as growth_data
;


## 2-C.  Calculate the weekly retention of users-sign up cohort?
SELECT
  week_period,
  FIRST_VALUE(weekly_retention) OVER (ORDER BY week_period) AS cohort_size,
  weekly_retention
FROM
  (SELECT
    TIMESTAMPDIFF(WEEK, a.activated_at, b.occurred_at) AS week_period,
    COUNT(DISTINCT a.user_id) AS weekly_retention
  FROM
    (SELECT user_id, activated_at
     FROM users
     WHERE state = 'active') a
  INNER JOIN
    (SELECT user_id, occurred_at
     FROM events
     WHERE event_type = 'engagement') b
  ON a.user_id = b.user_id
  GROUP BY 1) c;
  
  
## 2- D. Calculate the weekly engagement per device?


SELECT 
    device,
    AVG(users) AS users_weekly,
    AVG(device_used) AS device_used_weekly
FROM
    (SELECT 
        WEEK(occurred_at) AS week,
            device,
            COUNT(DISTINCT (user_id)) AS users,
            COUNT(device) AS device_used
    FROM
        events
    WHERE
        event_name = 'login'
    GROUP BY week , device
    ORDER BY week) d
GROUP BY device;


## 2- E. Calculate the email engagement metrics?
SELECT 
    WEEK(occurred_at) AS week,
    COUNT(DISTINCT (CASE
            WHEN action = 'sent_weekly_digest' THEN user_id
            ELSE 0
        END)) AS weekly_digest_receivers,
    COUNT(DISTINCT (CASE
            WHEN action = 'email_open' THEN user_id
            ELSE 0
        END)) AS weekly_email_open,
    COUNT(DISTINCT (CASE
            WHEN action = 'email_clickthrough' THEN user_id
            ELSE 0
        END)) AS weekly_email_clickthrough,
    COUNT(DISTINCT (CASE
            WHEN action = 'sent_reengagement_email' THEN user_id
            ELSE 0
        END)) AS weekly_reengagement_emails
FROM
    email_events
GROUP BY week
ORDER BY week;





