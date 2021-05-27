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
    dag_id='create_populate_abs_fact_table',
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

create_dim_lga_task = PostgresOperator(
    task_id="create_dim_lga_task_id",
    postgres_conn_id="postgres",
    sql="""
        CREATE TABLE IF NOT EXISTS 
			bde_at2.dim_lga as (select * from bde_at2.lga_raw
);
    """,
    dag=dag
)

create_fact_table_task = PostgresOperator(
    task_id="create_fact_table_task_id",
    postgres_conn_id="postgres",
    sql="""
        
    CREATE TABLE IF NOT EXISTS 
	bde_at2.abs_fact (
	abs_id SERIAL,
	LGA_CODE_2016  text   NOT NULL,
	Age_0_4_yr_M   numeric,
	Age_0_4_yr_F   numeric,
	Age_0_4_yr_P   numeric,
	Age_5_14_yr_M   numeric,
	Age_5_14_yr_F   numeric,
	Age_5_14_yr_P   numeric,
	Age_15_19_yr_M   numeric,
	Age_15_19_yr_F   numeric,
	Age_15_19_yr_P   numeric,
	Age_20_24_yr_M   numeric,
	Age_20_24_yr_F   numeric,
	Age_20_24_yr_P   numeric,
	Age_25_34_yr_M   numeric,
	Age_25_34_yr_F   numeric,
	Age_25_34_yr_P   numeric,
	Age_35_44_yr_M   numeric,
	Age_35_44_yr_F   numeric,
	Age_35_44_yr_P   numeric,
	Age_45_54_yr_M   numeric,
	Age_45_54_yr_F   numeric,
	Age_45_54_yr_P   numeric,
	Age_55_64_yr_M   numeric,
	Age_55_64_yr_F   numeric,
	Age_55_64_yr_P   numeric,
	Age_65_74_yr_M   numeric,
	Age_65_74_yr_F   numeric,
	Age_65_74_yr_P   numeric,
	Age_75_84_yr_M   numeric,
	Age_75_84_yr_F   numeric,
	Age_75_84_yr_P   numeric,
	Age_85ov_M   numeric,
	Age_85ov_F   numeric,
	Age_85ov_P   numeric,
	Birthplace_Australia_M   numeric,
	Birthplace_Australia_F   numeric,
	Birthplace_Australia_P   numeric,
	Birthplace_Elsewhere_M   numeric,
	Birthplace_Elsewhere_F   numeric,
	Birthplace_Elsewhere_P   numeric,
	Lang_spoken_home_Eng_only_M   numeric,
	Lang_spoken_home_Eng_only_F   numeric,
	Lang_spoken_home_Eng_only_P   numeric,
	Lang_spoken_home_Oth_Lang_M   numeric,
	Lang_spoken_home_Oth_Lang_F   numeric,
	Lang_spoken_home_Oth_Lang_P   numeric,
	High_yr_schl_comp_Yr_12_eq_M   numeric,
	High_yr_schl_comp_Yr_12_eq_F   numeric,
	High_yr_schl_comp_Yr_12_eq_P   numeric,
	High_yr_schl_comp_Yr_11_eq_M   numeric,
	High_yr_schl_comp_Yr_11_eq_F   numeric,
	High_yr_schl_comp_Yr_11_eq_P   numeric,
	High_yr_schl_comp_Yr_10_eq_M   numeric,
	High_yr_schl_comp_Yr_10_eq_F   numeric,
	High_yr_schl_comp_Yr_10_eq_P   numeric,
	High_yr_schl_comp_Yr_9_eq_M   numeric,
	High_yr_schl_comp_Yr_9_eq_F   numeric,
	High_yr_schl_comp_Yr_9_eq_P   numeric,
	High_yr_schl_comp_Yr_8_belw_M   numeric,
	High_yr_schl_comp_Yr_8_belw_F   numeric,
	High_yr_schl_comp_Yr_8_belw_P   numeric,
	High_yr_schl_comp_D_n_g_sch_M   numeric,
	High_yr_schl_comp_D_n_g_sch_F   numeric,
	High_yr_schl_comp_D_n_g_sch_P   numeric,
	Median_age_persons   			numeric,
	Median_mortgage_repay_monthly   numeric,
	Median_tot_prsnl_inc_weekly   	numeric,
	Median_rent_weekly   			numeric,
	Median_tot_fam_inc_weekly   	numeric,
	Average_num_psns_per_bedroom   	numeric,
	Median_tot_hhd_inc_weekly   	numeric,
	Average_household_size   		numeric,
	lga_name						text

);

    """,
    dag=dag
)


populate_fact_table_task = PostgresOperator(
    task_id="populate_fact_table_task_id",
    postgres_conn_id="postgres",
    sql="""
		INSERT INTO bde_at2.abs_fact
		(LGA_CODE_2016, Age_0_4_yr_M, Age_0_4_yr_F, Age_0_4_yr_P, Age_5_14_yr_M, Age_5_14_yr_F, Age_5_14_yr_P, Age_15_19_yr_M, Age_15_19_yr_F, Age_15_19_yr_P, Age_20_24_yr_M, Age_20_24_yr_F, Age_20_24_yr_P, Age_25_34_yr_M, Age_25_34_yr_F, Age_25_34_yr_P, Age_35_44_yr_M, Age_35_44_yr_F, Age_35_44_yr_P, Age_45_54_yr_M, Age_45_54_yr_F, Age_45_54_yr_P, Age_55_64_yr_M, Age_55_64_yr_F, Age_55_64_yr_P, Age_65_74_yr_M, Age_65_74_yr_F, Age_65_74_yr_P, Age_75_84_yr_M, Age_75_84_yr_F, Age_75_84_yr_P, Age_85ov_M, Age_85ov_F, Age_85ov_P, Birthplace_Australia_M, Birthplace_Australia_F, Birthplace_Australia_P, Birthplace_Elsewhere_M, Birthplace_Elsewhere_F, Birthplace_Elsewhere_P, Lang_spoken_home_Eng_only_M, Lang_spoken_home_Eng_only_F, Lang_spoken_home_Eng_only_P, Lang_spoken_home_Oth_Lang_M, Lang_spoken_home_Oth_Lang_F, Lang_spoken_home_Oth_Lang_P, High_yr_schl_comp_Yr_12_eq_M, High_yr_schl_comp_Yr_12_eq_F, High_yr_schl_comp_Yr_12_eq_P, High_yr_schl_comp_Yr_11_eq_M, High_yr_schl_comp_Yr_11_eq_F, High_yr_schl_comp_Yr_11_eq_P, High_yr_schl_comp_Yr_10_eq_M, High_yr_schl_comp_Yr_10_eq_F, High_yr_schl_comp_Yr_10_eq_P, High_yr_schl_comp_Yr_9_eq_M, High_yr_schl_comp_Yr_9_eq_F, High_yr_schl_comp_Yr_9_eq_P, High_yr_schl_comp_Yr_8_belw_M, High_yr_schl_comp_Yr_8_belw_F, High_yr_schl_comp_Yr_8_belw_P, High_yr_schl_comp_D_n_g_sch_M, High_yr_schl_comp_D_n_g_sch_F, High_yr_schl_comp_D_n_g_sch_P, Median_age_persons, Median_mortgage_repay_monthly, Median_tot_prsnl_inc_weekly, Median_rent_weekly, Median_tot_fam_inc_weekly, Average_num_psns_per_bedroom, Median_tot_hhd_inc_weekly, Average_household_size, lga_name)
		select
			f.lga_code_2016,
			Age_0_4_yr_M:: numeric,
			Age_0_4_yr_F:: numeric,
			Age_0_4_yr_P:: numeric,
			Age_5_14_yr_M:: numeric,
			Age_5_14_yr_F:: numeric,
			Age_5_14_yr_P:: numeric,
			Age_15_19_yr_M:: numeric,
			Age_15_19_yr_F:: numeric,
			Age_15_19_yr_P:: numeric,
			Age_20_24_yr_M:: numeric,
			Age_20_24_yr_F:: numeric,
			Age_20_24_yr_P:: numeric,
			Age_25_34_yr_M:: numeric,
			Age_25_34_yr_F:: numeric,
			Age_25_34_yr_P:: numeric,
			Age_35_44_yr_M:: numeric,
			Age_35_44_yr_F:: numeric,
			Age_35_44_yr_P:: numeric,
			Age_45_54_yr_M:: numeric,
			Age_45_54_yr_F:: numeric,
			Age_45_54_yr_P:: numeric,
			Age_55_64_yr_M:: numeric,
			Age_55_64_yr_F:: numeric,
			Age_55_64_yr_P:: numeric,
			Age_65_74_yr_M:: numeric,
			Age_65_74_yr_F:: numeric,
			Age_65_74_yr_P:: numeric,
			Age_75_84_yr_M:: numeric,
			Age_75_84_yr_F:: numeric,
			Age_75_84_yr_P:: numeric,
			Age_85ov_M:: numeric,
			Age_85ov_F:: numeric,
			Age_85ov_P:: numeric,
			Birthplace_Australia_M:: numeric,
			Birthplace_Australia_F:: numeric,
			Birthplace_Australia_P:: numeric,
			Birthplace_Elsewhere_M:: numeric,
			Birthplace_Elsewhere_F:: numeric,
			Birthplace_Elsewhere_P:: numeric,
			Lang_spoken_home_Eng_only_M:: numeric,
			Lang_spoken_home_Eng_only_F:: numeric,
			Lang_spoken_home_Eng_only_P:: numeric,
			Lang_spoken_home_Oth_Lang_M:: numeric,
			Lang_spoken_home_Oth_Lang_F:: numeric,
			Lang_spoken_home_Oth_Lang_P:: numeric,
			High_yr_schl_comp_Yr_12_eq_M:: numeric,
			High_yr_schl_comp_Yr_12_eq_F:: numeric,
			High_yr_schl_comp_Yr_12_eq_P:: numeric,
			High_yr_schl_comp_Yr_11_eq_M:: numeric,
			High_yr_schl_comp_Yr_11_eq_F:: numeric,
			High_yr_schl_comp_Yr_11_eq_P:: numeric,
			High_yr_schl_comp_Yr_10_eq_M:: numeric,
			High_yr_schl_comp_Yr_10_eq_F:: numeric,
			High_yr_schl_comp_Yr_10_eq_P:: numeric,
			High_yr_schl_comp_Yr_9_eq_M:: numeric,
			High_yr_schl_comp_Yr_9_eq_F:: numeric,
			High_yr_schl_comp_Yr_9_eq_P:: numeric,
			High_yr_schl_comp_Yr_8_belw_M:: numeric,
			High_yr_schl_comp_Yr_8_belw_F:: numeric,
			High_yr_schl_comp_Yr_8_belw_P:: numeric,
			High_yr_schl_comp_D_n_g_sch_M:: numeric,
			High_yr_schl_comp_D_n_g_sch_F:: numeric,
			High_yr_schl_comp_D_n_g_sch_P:: numeric,
			g.Median_age_persons:: numeric,
			g.Median_mortgage_repay_monthly:: numeric,
			g.Median_tot_prsnl_inc_weekly:: numeric,
			g.Median_rent_weekly:: numeric,
			g.Median_tot_fam_inc_weekly:: numeric,
			g.Average_num_psns_per_bedroom:: numeric,
			g.Median_tot_hhd_inc_weekly:: numeric,
			g.Average_household_size:: numeric,
			l.lga_name
		from
			bde_at2.g1_raw f
		inner join
			bde_at2.g2_raw g
			on 	g.lga_code_2016 = f.lga_code_2016
		inner join
			bde_at2.dim_lga l
			on 	f.lga_code_2016 = l.code_2016
				
    """,
    dag=dag
)

create_psql_schema_task >> create_dim_lga_task >> create_fact_table_task >> populate_fact_table_task