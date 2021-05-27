import os
import logging
import requests
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from psycopg2.extras import execute_values
from airflow import AirflowException


########################################################
#
#   DAG Settings
#
#########################################################

from airflow import DAG

dag_default_args = {
    'owner': 'Werner',
    'start_date': datetime.now() - timedelta(days=2),
    'email': [],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'depends_on_past': False,
    'wait_for_downstream': False,
}

dag = DAG(
    dag_id='create_populate_refresh_datamart',
    default_args=dag_default_args,
    schedule_interval="@daily",
    catchup=True,
    max_active_runs=1,
    concurrency=5
)

#########################################################
#
#   DAG Operator Setup
#
#########################################################

from airflow.operators.python_operator import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

################## Datamart ############################

create_dm_schema_task = PostgresOperator(
    task_id="create_dm_schema_task_id",
    postgres_conn_id="postgres",
    sql="""
        CREATE SCHEMA IF NOT EXISTS at2_datamart;
    """,
    dag=dag
)

view_1_1_task = PostgresOperator(
    task_id="view_1_1_id",
    postgres_conn_id="postgres",
    sql="""
        CREATE MATERIALIZED VIEW IF NOT EXISTS 
    at2_datamart.Question_1_1_active_listings_rate AS

WITH cte AS
    (
SELECT
	h.neighbourhood_cleansed,
	t.year_month,
	count(case when p.has_availability = 't' then 1 ELSE NULL END) as active_listings,
	count(*) as total_listings
FROM	
	bde_at2.air_bnb_fact f
INNER JOIN 
	bde_at2.dim_property p 
   	ON p.p_id = f.p_id
INNER JOIN 
	bde_at2.dim_host h 
   	ON h.h_id = f.h_id
INNER JOIN 
	bde_at2.dim_time t 
   	ON t.t_id = f.t_id
GROUP BY
	h.neighbourhood_cleansed,
	t.year_month
)

SELECT
    neighbourhood_cleansed,
	year_month,
	active_listings,
	total_listings,
	ROUND(CAST(active_listings AS numeric)/(total_listings),2) AS active_listings_rate
		
FROM
    cte
   
ORDER BY
	year_month asc,
	neighbourhood_cleansed
;

REFRESH MATERIALIZED VIEW at2_datamart.Question_1_1_active_listings_rate;
	
    """,
    dag=dag
)

view_1_2_task = PostgresOperator(
    task_id="view_1_2_id",
    postgres_conn_id="postgres",
    sql="""
	CREATE MATERIALIZED VIEW IF NOT EXISTS 
	at2_datamart.Question_1_2_min_max_med_avg AS

SELECT
	h.neighbourhood_cleansed,
	t.year_month,
	ROUND(min(price),2) as min_price,
	ROUND(max(price),2) as max_price,
    ROUND(percentile_disc(0.5) within group (order by price),2) median_price,
	ROUND(avg(price),2) as average_price

FROM	
	bde_at2.air_bnb_fact f
INNER JOIN 
	bde_at2.dim_property p 
   	ON p.p_id = f.p_id
INNER JOIN 
	bde_at2.dim_host h 
   	ON h.h_id = f.h_id
INNER JOIN 
	bde_at2.dim_time t 
   	ON t.t_id = f.t_id
GROUP BY
	h.neighbourhood_cleansed,
	t.year_month
ORDER BY
   	t.year_month asc,
	h.neighbourhood_cleansed
;

REFRESH MATERIALIZED VIEW at2_datamart.Question_1_2_min_max_med_avg;
    """,
    dag=dag
)

view_1_3_task = PostgresOperator(
    task_id="view_1_3_id",
    postgres_conn_id="postgres",
    sql="""
	CREATE MATERIALIZED VIEW IF NOT EXISTS 
	at2_datamart.Question_1_3_distinct_hosts AS

SELECT
	h.neighbourhood_cleansed,
	t.year_month,
	COUNT(DISTINCT host_id) as distinct_host_count
FROM	
	bde_at2.dim_host h

INNER JOIN 
	bde_at2.air_bnb_fact f 
   	ON f.h_id = h.h_id
INNER JOIN 
	bde_at2.dim_time t 
   	ON t.t_id = f.t_id
GROUP BY
	h.neighbourhood_cleansed,
	t.year_month
ORDER BY
   	t.year_month asc,
	h.neighbourhood_cleansed
;
REFRESH MATERIALIZED VIEW at2_datamart.Question_1_3_distinct_hosts
;
    """,
    dag=dag
)

view_1_4_task = PostgresOperator(
    task_id="view_1_4_id",
    postgres_conn_id="postgres",
    sql="""
	CREATE MATERIALIZED VIEW IF NOT EXISTS 
	at2_datamart.Question_1_4_superhost_rate AS

WITH cte AS
    (
SELECT
	
	h.neighbourhood_cleansed,
	t.year_month,
	COUNT(DISTINCT CASE
    	WHEN h.host_is_superhost ='t'
    	THEN h.host_id
    	ELSE NULL END)
	AS distinct_super_host_count,
COUNT(DISTINCT h.host_id) as distinct_host_count
	
FROM	
	bde_at2.air_bnb_fact f

INNER JOIN 
	bde_at2.dim_host h 
   	ON h.h_id = f.h_id
INNER JOIN 
	bde_at2.dim_time t 
   	ON t.t_id = f.t_id
GROUP BY
	h.neighbourhood_cleansed,
	t.year_month
)

SELECT
	neighbourhood_cleansed,
	year_month,
	ROUND(CAST(distinct_super_host_count AS numeric)/(distinct_host_count),2) AS superhost_rate	
FROM
    cte c
ORDER BY
	year_month asc,
	neighbourhood_cleansed    
;
REFRESH MATERIALIZED VIEW at2_datamart.Question_1_4_superhost_rate
;
    """,
    dag=dag
)

view_1_5_task = PostgresOperator(
    task_id="view_1_5_id",
    postgres_conn_id="postgres",
    sql="""
	CREATE MATERIALIZED VIEW IF NOT EXISTS 
    at2_datamart.Question_1_5_active_listing_avg_review_score_rating AS

SELECT
	h.neighbourhood_cleansed,
	t.year_month,
	ROUND(avg(f.review_scores_rating),2) as average_review_scores_rating
FROM	
	bde_at2.air_bnb_fact f
INNER JOIN 
	bde_at2.dim_property p 
   	ON p.p_id = f.p_id
INNER JOIN 
	bde_at2.dim_host h 
   	ON h.h_id = f.h_id
INNER JOIN 
	bde_at2.dim_time t 
   	ON t.t_id = f.t_id
WHERE
	p.has_availability = 't' and 	-- active listings
	review_scores_rating != 'NaN' 	-- not including NaN values, which will not allow an aggregate to be calculated
GROUP BY
	h.neighbourhood_cleansed,
	t.year_month
ORDER BY
	year_month asc,
	neighbourhood_cleansed
;
REFRESH MATERIALIZED VIEW at2_datamart.Question_1_5_active_listing_avg_review_score_rating
;
    """,
    dag=dag
)

view_1_6_task = PostgresOperator(
    task_id="view_1_6_id",
    postgres_conn_id="postgres",
    sql="""
	CREATE MATERIALIZED VIEW IF NOT EXISTS 
at2_datamart.Question_1_6_percentage_change_for_active_listings as

WITH cte AS
    (
	SELECT
		h.neighbourhood_cleansed,
		t.year_month,
		count(case when p.has_availability = 't' then 1 ELSE NULL END) as active_listings
	FROM	
		bde_at2.air_bnb_fact f
	INNER JOIN 
		bde_at2.dim_property p 
		ON p.p_id = f.p_id
	INNER JOIN 
		bde_at2.dim_host h 
		ON h.h_id = f.h_id
	INNER JOIN 
		bde_at2.dim_time t 
		ON t.t_id = f.t_id
	GROUP BY
		h.neighbourhood_cleansed,
		t.year_month
	ORDER BY
	year_month asc,
	neighbourhood_cleansed
)

SELECT 
	neighbourhood_cleansed,
	year_month,
	active_listings,
	(CAST(active_listings AS numeric)/LAG(SUM(active_listings), 1) OVER (ORDER BY neighbourhood_cleansed,year_month)-1) AS Running_Diff_Per		
FROM
    cte

GROUP BY
    1,
    year_month,
    active_listings
;
REFRESH MATERIALIZED VIEW at2_datamart.Question_1_6_percentage_change_for_active_listings
;
    """,
    dag=dag
)

view_1_7_task = PostgresOperator(
    task_id="view_1_7_id",
    postgres_conn_id="postgres",
    sql="""
	CREATE MATERIALIZED VIEW IF NOT EXISTS 
at2_datamart.Question_1_7_percentage_change_for_inactive_listings as

WITH cte AS
    (
	SELECT
		h.neighbourhood_cleansed,
		t.year_month,
		count(case when p.has_availability = 'f' then 1 ELSE NULL END) as inactive_listings
	FROM	
		bde_at2.air_bnb_fact f
	INNER JOIN 
		bde_at2.dim_property p 
		ON p.p_id = f.p_id
	INNER JOIN 
		bde_at2.dim_host h 
		ON h.h_id = f.h_id
	INNER JOIN 
		bde_at2.dim_time t 
		ON t.t_id = f.t_id
	GROUP BY
		h.neighbourhood_cleansed,
		t.year_month
	ORDER BY
	year_month asc,
	neighbourhood_cleansed
)

SELECT 
	neighbourhood_cleansed,
	year_month,
	inactive_listings,
	(CAST(inactive_listings AS numeric)/LAG(SUM(inactive_listings), 1) OVER (ORDER BY neighbourhood_cleansed,year_month)-1) AS Running_Diff_Per		
FROM
    cte
WHERE inactive_listings <> 0
GROUP BY
    1,
    year_month,
    inactive_listings
;
REFRESH MATERIALIZED VIEW at2_datamart.Question_1_7_percentage_change_for_inactive_listings
;
    """,
    dag=dag
)

view_1_8_task = PostgresOperator(
    task_id="view_1_8_id",
    postgres_conn_id="postgres",
    sql="""
	CREATE MATERIALIZED VIEW IF NOT EXISTS 
	at2_datamart.Question_1_8_no_of_stays as
	
WITH cte AS
    (
SELECT
	h.neighbourhood_cleansed,
	t.year_month,
	30 - availability_30 number_of_stays
FROM	
	bde_at2.air_bnb_fact f
INNER JOIN 
	bde_at2.dim_host h 
   	ON h.h_id = f.h_id
INNER JOIN 
	bde_at2.dim_time t 
   	ON t.t_id = f.t_id
)
SELECT
	neighbourhood_cleansed,
	year_month,
	SUM(number_of_stays) number_of_stays
FROM
	cte
GROUP BY
	neighbourhood_cleansed,
	year_month
ORDER BY
   	neighbourhood_cleansed,
   	year_month asc
;
REFRESH MATERIALIZED VIEW at2_datamart.Question_1_8_no_of_stays
;
    """,
    dag=dag
)

view_1_9_task = PostgresOperator(
    task_id="view_1_9_id",
    postgres_conn_id="postgres",
    sql="""
	CREATE MATERIALIZED VIEW IF NOT EXISTS 
	at2_datamart.Question_1_9_estimated_revenue_active_listings AS

-- First CTE introduces calculation elements i.e.number_of_stays and price. 
	
WITH cte1 AS
    (
SELECT
	h.neighbourhood_cleansed,
	t.year_month,
	30 - availability_30 number_of_stays,
	price
FROM	
	bde_at2.air_bnb_fact f
INNER JOIN 
	bde_at2.dim_host h 
   	ON h.h_id = f.h_id
INNER JOIN 
	bde_at2.dim_time t 
   	ON t.t_id = f.t_id
GROUP BY
	h.neighbourhood_cleansed,
	t.year_month,
	number_of_stays,
	price
),

-- Second cte defines revenue (estimated revenue per active listings) calculation.

	cte2 AS
    (
SELECT
	neighbourhood_cleansed,
	year_month,
	number_of_stays*price revenue	
FROM
	cte1
ORDER BY
   	neighbourhood_cleansed,
   	year_month asc
)

-- Third query aggregates and produces estimated_revenue_per_active_listings metric at the required level.
SELECT
	neighbourhood_cleansed,
	year_month,
	SUM(revenue) estimated_revenue_per_active_listings
FROM
	cte2
GROUP BY
	neighbourhood_cleansed,
	year_month
ORDER BY
   	neighbourhood_cleansed,
   	year_month asc
;
REFRESH MATERIALIZED VIEW at2_datamart.Question_1_9_estimated_revenue_active_listings
;
    """,
    dag=dag
)

view_2_1_task = PostgresOperator(
    task_id="view_2_1_id",
    postgres_conn_id="postgres",
    sql="""
    CREATE MATERIALIZED VIEW IF NOT EXISTS 
    at2_datamart.Question_2_1_active_listings_rate AS

WITH cte AS
    (
SELECT
	p.property_type,
	p.room_type,
	f.accommodates,
	t.year_month,
	count(case when p.has_availability = 't' then 1 ELSE NULL END) as active_listings,
	count(*) as total_listings
FROM	
	bde_at2.air_bnb_fact f
INNER JOIN 
	bde_at2.dim_property p 
   	ON p.p_id = f.p_id
INNER JOIN 
	bde_at2.dim_host h 
   	ON h.h_id = f.h_id
INNER JOIN 
	bde_at2.dim_time t 
   	ON t.t_id = f.t_id
GROUP BY
	p.property_type,
	p.room_type,
	f.accommodates,
	t.year_month
)

SELECT
    property_type,
	room_type,
	accommodates,
	year_month,
	active_listings,
	total_listings,
	ROUND(CAST(active_listings AS numeric)/(total_listings),2) AS active_listings_rate
		
FROM
    cte
   
ORDER BY
	year_month asc,
	property_type,
	room_type,
	accommodates
;
REFRESH MATERIALIZED VIEW at2_datamart.Question_2_1_active_listings_rate
;
	
    """,
    dag=dag
)

view_2_2_task = PostgresOperator(
    task_id="view_2_2_id",
    postgres_conn_id="postgres",
    sql="""
CREATE MATERIALIZED VIEW IF NOT EXISTS 
	at2_datamart.Question_2_2_min_max_med_avg AS

SELECT
	p.property_type,
	p.room_type,
	f.accommodates,
	t.year_month,
	ROUND(min(price),2) as min_price,
	ROUND(max(price),2) as max_price,
    ROUND(percentile_disc(0.5) within group (order by price),2) median_price,
	ROUND(avg(price),2) as average_price

FROM	
	bde_at2.air_bnb_fact f
INNER JOIN 
	bde_at2.dim_property p 
   	ON p.p_id = f.p_id
INNER JOIN 
	bde_at2.dim_host h 
   	ON h.h_id = f.h_id
INNER JOIN 
	bde_at2.dim_time t 
   	ON t.t_id = f.t_id
GROUP BY
	p.property_type,
	p.room_type,
	f.accommodates,
	t.year_month
ORDER BY
   	t.year_month asc,
	p.property_type,
	p.room_type,
	f.accommodates
;
REFRESH MATERIALIZED VIEW at2_datamart.Question_2_2_min_max_med_avg
;
    """,
    dag=dag
)

view_2_3_task = PostgresOperator(
    task_id="view_2_3_id",
    postgres_conn_id="postgres",
    sql="""

CREATE MATERIALIZED VIEW IF NOT EXISTS 
	at2_datamart.Question_2_3_distinct_hosts AS

SELECT
	p.property_type,
	p.room_type,
	f.accommodates,
	t.year_month,
	COUNT(DISTINCT host_id) as distinct_host_count
FROM	
	bde_at2.dim_host h

INNER JOIN 
	bde_at2.air_bnb_fact f 
   	ON f.h_id = h.h_id
INNER JOIN 
	bde_at2.dim_time t 
   	ON t.t_id = f.t_id
INNER JOIN 
	bde_at2.dim_property p 
   	ON p.p_id = f.p_id
GROUP BY
	p.property_type,
	p.room_type,
	f.accommodates,
	t.year_month
ORDER BY
   	t.year_month asc,
	p.property_type,
	p.room_type,
	f.accommodates
;
REFRESH MATERIALIZED VIEW at2_datamart.Question_2_3_distinct_hosts
;
    """,
    dag=dag
)

view_2_4_task = PostgresOperator(
    task_id="view_2_4_id",
    postgres_conn_id="postgres",
    sql="""
CREATE MATERIALIZED VIEW IF NOT EXISTS 
	at2_datamart.Question_2_4_superhost_rate AS

WITH cte AS
    (
SELECT

	p.property_type,
	p.room_type,
	f.accommodates,
	t.year_month,
	COUNT(DISTINCT CASE
    	WHEN h.host_is_superhost ='t'
    	THEN h.host_id
    	ELSE NULL END)
	AS distinct_super_host_count,
COUNT(DISTINCT h.host_id) as distinct_host_count
	
FROM	
	bde_at2.air_bnb_fact f

INNER JOIN 
	bde_at2.dim_property p 
   	ON p.p_id = f.p_id
INNER JOIN 
	bde_at2.dim_host h 
   	ON h.h_id = f.h_id
INNER JOIN 
	bde_at2.dim_time t 
   	ON t.t_id = f.t_id
GROUP BY
	p.property_type,
	p.room_type,
	f.accommodates,
	t.year_month
)

SELECT
	property_type,
	room_type,
	accommodates,
	year_month,
	ROUND(CAST(distinct_super_host_count AS numeric)/(distinct_host_count),2) AS superhost_rate	
FROM
    cte c
ORDER BY
	year_month asc,
	property_type,
	room_type,
	accommodates   
;
REFRESH MATERIALIZED VIEW at2_datamart.Question_2_4_superhost_rate
;
    """,
    dag=dag
)

view_2_5_task = PostgresOperator(
    task_id="view_2_5_id",
    postgres_conn_id="postgres",
    sql="""
CREATE MATERIALIZED VIEW IF NOT EXISTS 
    at2_datamart.Question_2_5_active_listing_avg_review_score_rating AS

SELECT
	p.property_type,
	p.room_type,
	f.accommodates,
	t.year_month,
	ROUND(avg(f.review_scores_rating),2) as average_review_scores_rating
FROM	
	bde_at2.air_bnb_fact f
INNER JOIN 
	bde_at2.dim_property p 
   	ON p.p_id = f.p_id
INNER JOIN 
	bde_at2.dim_host h 
   	ON h.h_id = f.h_id
INNER JOIN 
	bde_at2.dim_time t 
   	ON t.t_id = f.t_id
WHERE
	p.has_availability = 't' and 	-- active listings
	review_scores_rating != 'NaN' 	-- not including NaN values, which will not allow an aggregate to be calculated
GROUP BY
	p.property_type,
	p.room_type,
	f.accommodates,
	t.year_month
ORDER BY
	year_month asc,
	p.property_type,
	p.room_type,
	f.accommodates
;
REFRESH MATERIALIZED VIEW at2_datamart.Question_2_5_active_listing_avg_review_score_rating
;
    """,
    dag=dag
)

view_2_6_task = PostgresOperator(
    task_id="view_2_6_id",
    postgres_conn_id="postgres",
    sql="""
CREATE MATERIALIZED VIEW IF NOT EXISTS 
at2_datamart.Question_2_6_percentage_change_for_active_listings as

WITH cte AS
    (
	SELECT
		p.property_type,
		p.room_type,
		f.accommodates,
		t.year_month,
		count(case when p.has_availability = 't' then 1 ELSE NULL END) as active_listings
	FROM	
		bde_at2.air_bnb_fact f
	INNER JOIN 
		bde_at2.dim_property p 
		ON p.p_id = f.p_id
	INNER JOIN 
		bde_at2.dim_time t 
		ON t.t_id = f.t_id
	GROUP BY
		p.property_type,
		p.room_type,
		f.accommodates,
		t.year_month
	ORDER BY
	year_month asc,
	p.property_type,
	p.room_type,
	f.accommodates
)

SELECT 
	property_type,
	room_type,
	accommodates,
	year_month,
	active_listings,
	(CAST(active_listings AS numeric)/LAG(SUM(active_listings), 1) OVER (ORDER BY property_type, room_type,accommodates, year_month)-1) AS Running_Diff_Per		
FROM
    cte
WHERE 
	active_listings > 0
GROUP BY
    1,
    year_month,
    active_listings,
    room_type,
    accommodates
;
REFRESH MATERIALIZED VIEW at2_datamart.Question_2_6_percentage_change_for_active_listings
;
    """,
    dag=dag
)

view_2_7_task = PostgresOperator(
    task_id="view_2_7_id",
    postgres_conn_id="postgres",
    sql="""
CREATE MATERIALIZED VIEW IF NOT EXISTS 
at2_datamart.Question_2_7_percentage_change_for_inactive_listings as

WITH cte AS
    (
	SELECT
		p.property_type,
		p.room_type,
		f.accommodates,
		t.year_month,
		count(case when p.has_availability = 'f' then 1 ELSE NULL END) as inactive_listings
	FROM	
		bde_at2.air_bnb_fact f
	INNER JOIN 
		bde_at2.dim_property p 
		ON p.p_id = f.p_id
	INNER JOIN 
		bde_at2.dim_host h 
		ON h.h_id = f.h_id
	INNER JOIN 
		bde_at2.dim_time t 
		ON t.t_id = f.t_id
	GROUP BY
		p.property_type,
		p.room_type,
		f.accommodates,
		t.year_month
	ORDER BY
	year_month asc,
	p.property_type,
	p.room_type,
	f.accommodates
)

SELECT 
	property_type,
	room_type,
	accommodates,
	year_month,
	inactive_listings,
	(CAST(inactive_listings AS numeric)/LAG(SUM(inactive_listings), 1) OVER (ORDER BY property_type,room_type,accommodates,year_month)-1) AS Running_Diff_Per		
FROM
    cte
WHERE inactive_listings <> 0
GROUP BY
    1,
    year_month,
    inactive_listings,
    room_type,
	accommodates
;

REFRESH MATERIALIZED VIEW at2_datamart.Question_2_7_percentage_change_for_inactive_listings
;
    """,
    dag=dag
)

view_2_8_task = PostgresOperator(
    task_id="view_2_8_id",
    postgres_conn_id="postgres",
    sql="""
CREATE MATERIALIZED VIEW IF NOT EXISTS 
	at2_datamart.Question_2_8_no_of_stays as

WITH cte AS
    (
SELECT
	p.property_type,
	p.room_type,
	f.accommodates,
	t.year_month,
	30 - availability_30 number_of_stays
FROM	
	bde_at2.air_bnb_fact f
INNER JOIN 
	bde_at2.dim_property p 
   	ON p.p_id = f.p_id
INNER JOIN 
	bde_at2.dim_time t 
   	ON t.t_id = f.t_id
)
SELECT
	property_type,
	room_type,
	accommodates,
	year_month,
	SUM(number_of_stays) number_of_stays
FROM
	cte
GROUP BY
	property_type,
	room_type,
	accommodates,
	year_month
ORDER BY
   	property_type,
	room_type,
	accommodates,
   	year_month asc
;
REFRESH MATERIALIZED VIEW at2_datamart.Question_2_8_no_of_stays
;
    """,
    dag=dag
)

view_2_9_task = PostgresOperator(
    task_id="view_2_9_id",
    postgres_conn_id="postgres",
    sql="""
CREATE MATERIALIZED VIEW IF NOT EXISTS 
	at2_datamart.Question_2_9_estimated_revenue_active_listings AS

-- First CTE introduces calculation elements i.e.number_of_stays and price. 
	
WITH cte1 AS
    (
SELECT
	p.property_type,
	p.room_type,
	f.accommodates,
	t.year_month,
	30 - availability_30 number_of_stays,
	price
FROM	
	bde_at2.air_bnb_fact f
INNER JOIN 
	bde_at2.dim_property p 
   	ON p.p_id = f.p_id
INNER JOIN 
	bde_at2.dim_time t 
   	ON t.t_id = f.t_id
GROUP BY
	p.property_type,
	p.room_type,
	f.accommodates,
	t.year_month,
	number_of_stays,
	price
),

-- Second cte defines revenue (estimated revenue per active listings) calculation.

	cte2 AS
    (
SELECT
	property_type,
	room_type,
	accommodates,
	year_month,
	number_of_stays*price revenue	
FROM
	cte1
)

-- Third query aggregates and produces estimated_revenue_per_active_listings metric at the required level.
SELECT
	property_type,
	room_type,
	accommodates,
	year_month,
	SUM(revenue) estimated_revenue_per_active_listings
FROM
	cte2
GROUP BY
	property_type,
	room_type,
	accommodates,
	year_month
ORDER BY
   	property_type,
	room_type,
	accommodates,
   	year_month asc
;
REFRESH MATERIALIZED VIEW at2_datamart.Question_2_9_estimated_revenue_active_listings
;	
    """,
    dag=dag
)

view_3_1_task = PostgresOperator(
    task_id="view_3_1_id",
    postgres_conn_id="postgres",
    sql="""
CREATE MATERIALIZED VIEW IF NOT EXISTS 
	at2_datamart.Question_3_1_distinct_hosts AS

SELECT 
	l.lga_name host_lga_cleansed,
	t.year_month,
	COUNT(DISTINCT host_id) as distinct_host_count
FROM 
	bde_at2.dim_host h
INNER JOIN 
	bde_at2.lga_raw l
	ON l.neighbourhood_cleansed = h.neighbourhood_cleansed
INNER JOIN 
	bde_at2.air_bnb_fact f 
	ON h.h_id = f.h_id
INNER JOIN 
	bde_at2.dim_time t 
	ON t.t_id = f.t_id
group by
	host_lga_cleansed,
	year_month
order by
	host_lga_cleansed,
	year_month asc
;
REFRESH MATERIALIZED VIEW at2_datamart.Question_3_1_distinct_hosts
;
    """,
    dag=dag
)

view_3_2_task = PostgresOperator(
    task_id="view_3_2_id",
    postgres_conn_id="postgres",
    sql="""
CREATE MATERIALIZED VIEW IF NOT EXISTS 
	at2_datamart.Question_3_2_estimated_revenue_active_listings AS

WITH 
	cte1 AS
    (
SELECT 
	l.lga_name host_lga_cleansed,
	t.year_month,
	30 - f.availability_30 number_of_stays,
	f.price
FROM 
	bde_at2.dim_host h
INNER JOIN 
	bde_at2.lga_raw l
	ON l.neighbourhood_cleansed = h.neighbourhood_cleansed
INNER JOIN 
	bde_at2.air_bnb_fact f 
	ON h.h_id = f.h_id
INNER JOIN 
	bde_at2.dim_time t 
	ON t.t_id = f.t_id
),

-- Second cte defines revenue (estimated revenue per active listings) calculation.
	cte2 AS
    (
SELECT 
	host_lga_cleansed,
	year_month,
	number_of_stays*price revenue
FROM 
	cte1
ORDER BY
   	host_lga_cleansed,
   	year_month asc
)

-- Third query aggregates and produces estimated_revenue_per_active_listings metric at the required level.
SELECT
	host_lga_cleansed,
	year_month,
	SUM(revenue) estimated_revenue_per_active_listings
FROM
	cte2
GROUP BY
	host_lga_cleansed,
	year_month
ORDER BY
   	host_lga_cleansed,
   	year_month asc
;
REFRESH MATERIALIZED VIEW at2_datamart.Question_3_2_estimated_revenue_active_listings
;
    """,
    dag=dag
)

view_3_3_task = PostgresOperator(
    task_id="view_3_3_id",
    postgres_conn_id="postgres",
    sql="""
CREATE MATERIALIZED VIEW IF NOT EXISTS 
	at2_datamart.Question_3_3_estimated_revenue_per_host AS

-- First CTE introduces calculation elements i.e.number_of_stays and price. 

WITH 
	cte1 AS
    (
SELECT 
	host_id, 
	l.lga_name host_lga_cleansed,
	t.year_month,
	30 - f.availability_30 number_of_stays,
	f.price
FROM 
	bde_at2.dim_host h
INNER JOIN 
	bde_at2.lga_raw l
	ON l.neighbourhood_cleansed = h.neighbourhood_cleansed
INNER JOIN 
	bde_at2.air_bnb_fact f 
	ON h.h_id = f.h_id
INNER JOIN 
	bde_at2.dim_time t 
	ON t.t_id = f.t_id
),

-- Second cte defines revenue (estimated revenue per active listings) calculation.
	cte2 AS
    (
SELECT 
	host_id,
	host_lga_cleansed,
	year_month,
	number_of_stays*price revenue
FROM 
	cte1
ORDER BY
   	host_lga_cleansed,
   	year_month asc
)

-- Third query aggregates and produces estimated_revenue_per_active_listings metric at the required level.

SELECT DISTINCT
	host_id,
	host_lga_cleansed,
	year_month,
	SUM(revenue) estimated_revenue_per_active_listings
FROM
	cte2
GROUP BY
	host_id,
	host_lga_cleansed,
	year_month
ORDER BY
   	host_id,
   	host_lga_cleansed,
   	year_month asc
;
REFRESH MATERIALIZED VIEW at2_datamart.Question_3_3_estimated_revenue_per_host
;
    """,
    dag=dag
)

create_dm_schema_task >> view_1_1_task >> view_1_2_task >> view_1_3_task >> view_1_4_task >> view_1_5_task >> view_1_6_task >> view_1_7_task >> view_1_8_task >> view_1_9_task
view_2_1_task >> view_2_2_task >> view_2_3_task >> view_2_4_task >> view_2_5_task >> view_2_6_task >> view_2_7_task >> view_2_8_task >> view_2_9_task
view_3_1_task >> view_3_2_task >> view_3_3_task