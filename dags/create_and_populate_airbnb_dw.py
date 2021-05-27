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
    dag_id='create_and_populate_airbnb_dw',
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

##################Air BNB Data############################

create_psql_schema_task = PostgresOperator(
    task_id="create_psql_schema_task_id",
    postgres_conn_id="postgres",
    sql="""
        CREATE SCHEMA IF NOT EXISTS bde_at2;
    """,
    dag=dag
)

create_dim_host_task = PostgresOperator(
    task_id="create_dim_host_task_id",
    postgres_conn_id="postgres",
    sql="""
        CREATE TABLE IF NOT EXISTS 
	bde_at2.dim_host (
		h_id SERIAL,
		host_id text,
		host_name text,
		host_is_superhost text,
		host_neighbourhood text,
		neighbourhood_cleansed text,
		host_neighbourhood_cleansed text,
		
	CONSTRAINT dim_host_pkey PRIMARY KEY (h_id)
);

    """,
    dag=dag
)

create_dim_property_task = PostgresOperator(
    task_id="create_dim_property_task_id",
    postgres_conn_id="postgres",
    sql="""
        CREATE TABLE IF NOT EXISTS 
	bde_at2.dim_property (
		p_id SERIAL,
		property_type text,
		room_type text,
		has_availability text,
		
	CONSTRAINT dim_property_pkey PRIMARY KEY (p_id)
);
    """,
    dag=dag
)


create_dim_time_task = PostgresOperator(
    task_id="create_dim_time_task_id",
    postgres_conn_id="postgres",
    sql="""
        CREATE TABLE IF NOT EXISTS 
	bde_at2.dim_time (
		t_id SERIAL,
		month text,
		year numeric,
	CONSTRAINT dim_time_pkey PRIMARY KEY (t_id)
);

    """,
    dag=dag
)

create_fact_table_task = PostgresOperator(
    task_id="create_fact_table_task_id",
    postgres_conn_id="postgres",
    sql="""
        CREATE TABLE bde_at2.air_bnb_fact (
	f_id SERIAL,
	id int,
	h_id int4 NOT NULL,
	p_id int4 NOT NULL,
	t_id int4 NOT NULL,
	accommodates numeric,
	availability_30 numeric,
	number_of_reviews numeric,
	review_scores_rating numeric,
	price decimal (30,2) NULL
);

ALTER TABLE bde_at2.air_bnb_fact ADD CONSTRAINT fk_host FOREIGN KEY (h_id) REFERENCES bde_at2.dim_host(h_id);
ALTER TABLE bde_at2.air_bnb_fact ADD CONSTRAINT fk_property FOREIGN KEY (p_id) REFERENCES bde_at2.dim_property(p_id);
ALTER TABLE bde_at2.air_bnb_fact ADD CONSTRAINT fk_time FOREIGN KEY (t_id) REFERENCES bde_at2.dim_time(t_id);

    """,
    dag=dag
)


populate_dim_host_task = PostgresOperator(
    task_id="populate_dim_host_task_id",
    postgres_conn_id="postgres",
    sql="""
       INSERT INTO 
		bde_at2.dim_host
		(host_id, host_name, host_is_superhost, host_neighbourhood, neighbourhood_cleansed,host_neighbourhood_cleansed)
		select distinct 
			host_id,
			host_name,
			host_is_superhost,
			host_neighbourhood,
			neighbourhood_cleansed,
			host_neighbourhood_cleansed
		from 
			bde_at2.airbnb_raw

		ON CONFLICT (h_id) DO UPDATE SET 	host_name = EXCLUDED.host_name, 
											host_is_superhost = EXCLUDED.host_is_superhost, 
											host_neighbourhood = EXCLUDED.host_neighbourhood, 
											neighbourhood_cleansed = EXCLUDED.neighbourhood_cleansed
;

    """,
    dag=dag
)

populate_dim_property_task = PostgresOperator(
    task_id="populate_dim_property_task_id",
    postgres_conn_id="postgres",
    sql="""
       INSERT INTO 
		bde_at2.dim_property
		(property_type, room_type, has_availability)
		select distinct 
			property_type,
			room_type,
			has_availability
		from
			bde_at2.airbnb_raw

		ON CONFLICT (p_id) DO UPDATE SET 	property_type = EXCLUDED.property_type, 
											room_type = EXCLUDED.room_type, 
											has_availability = EXCLUDED.has_availability

;
    """,
    dag=dag
)

populate_dim_time_task = PostgresOperator(
    task_id="populate_dim_time_task_id",
    postgres_conn_id="postgres",
    sql="""
       INSERT INTO 
		bde_at2.dim_time
		(month, year)
		select distinct 
			month,
			year::numeric
		from 
			bde_at2.airbnb_raw

		ON CONFLICT (t_id) DO UPDATE SET 	month = EXCLUDED.month, 
											year = EXCLUDED.year
;
    """,
    dag=dag
)

populate_fact_table_task = PostgresOperator(
    task_id="populate_fact_table_task_id",
    postgres_conn_id="postgres",
    sql="""
					INSERT INTO bde_at2.air_bnb_fact
					(id,h_id, p_id, t_id, accommodates, availability_30, number_of_reviews, review_scores_rating, price)
					select
						r.id::int,
						h.h_id,
						p.p_id,
						t.t_id,
						r.accommodates::numeric,
						r.availability_30::numeric, 
						r.number_of_reviews::numeric, 
						r.review_scores_rating::numeric,
						r.price::money::numeric::float8
					from
						bde_at2.dim_host h
					inner join
						bde_at2.airbnb_raw r
						on 	r.host_id = h.host_id
						and r.host_name = h.host_name
						and r.host_is_superhost = h.host_is_superhost
						and r.host_neighbourhood = h.host_neighbourhood
						and r.neighbourhood_cleansed = h.neighbourhood_cleansed
					inner join
						bde_at2.dim_property p
						on 	r.property_type = p.property_type
						and r.room_type = p.room_type
						and r.has_availability = p.has_availability

					inner join
						bde_at2.dim_time t
						on 	r.month = t.month
						and r.year::numeric = t.year 
    """,
    dag=dag
)

create_psql_schema_task >> create_dim_host_task >> create_dim_property_task >> create_dim_time_task >> create_fact_table_task >> populate_dim_host_task >> populate_dim_property_task >> populate_dim_time_task >> populate_fact_table_task