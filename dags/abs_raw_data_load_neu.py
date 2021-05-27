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
    'owner': 'Nigel',
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
    dag_id='abs_raw_data_load_neu',
    default_args=dag_default_args,
    schedule_interval="@daily",
    catchup=True,
    max_active_runs=1,
    concurrency=5
)

#########################################################
#
#   Custom Logic for Operator
#
#########################################################

#ABS DATA: G1

def extract_g1_data_func():
    path = "./data/2016Census_G01_NSW_LGA.csv"
    df = pd.read_csv(path, header=0, sep=',', quotechar='"')
    df_dict = df.to_dict()
    return df_dict

def insert_g1_data_func(**kwargs):
         
    ps_pg_hook = PostgresHook(postgres_conn_id="postgres")
    conn_ps = ps_pg_hook.get_conn()

    ti = kwargs['ti']
    insert_df_dict = ti.xcom_pull(task_ids=f'extract_g1_data_task_id')
    insert_df = pd.DataFrame.from_dict(insert_df_dict)

    if len(insert_df) > 0:
        col_names = ['LGA_CODE_2016','Tot_P_M','Tot_P_F','Tot_P_P','Age_0_4_yr_M','Age_0_4_yr_F','Age_0_4_yr_P','Age_5_14_yr_M','Age_5_14_yr_F','Age_5_14_yr_P','Age_15_19_yr_M','Age_15_19_yr_F','Age_15_19_yr_P','Age_20_24_yr_M','Age_20_24_yr_F','Age_20_24_yr_P','Age_25_34_yr_M','Age_25_34_yr_F','Age_25_34_yr_P','Age_35_44_yr_M','Age_35_44_yr_F','Age_35_44_yr_P','Age_45_54_yr_M','Age_45_54_yr_F','Age_45_54_yr_P','Age_55_64_yr_M','Age_55_64_yr_F','Age_55_64_yr_P','Age_65_74_yr_M','Age_65_74_yr_F','Age_65_74_yr_P','Age_75_84_yr_M','Age_75_84_yr_F','Age_75_84_yr_P','Age_85ov_M','Age_85ov_F','Age_85ov_P','Counted_Census_Night_home_M','Counted_Census_Night_home_F','Counted_Census_Night_home_P','Count_Census_Nt_Ewhere_Aust_M','Count_Census_Nt_Ewhere_Aust_F','Count_Census_Nt_Ewhere_Aust_P','Indigenous_psns_Aboriginal_M','Indigenous_psns_Aboriginal_F','Indigenous_psns_Aboriginal_P','Indig_psns_Torres_Strait_Is_M','Indig_psns_Torres_Strait_Is_F','Indig_psns_Torres_Strait_Is_P','Indig_Bth_Abor_Torres_St_Is_M','Indig_Bth_Abor_Torres_St_Is_F','Indig_Bth_Abor_Torres_St_Is_P','Indigenous_P_Tot_M','Indigenous_P_Tot_F','Indigenous_P_Tot_P','Birthplace_Australia_M','Birthplace_Australia_F','Birthplace_Australia_P','Birthplace_Elsewhere_M','Birthplace_Elsewhere_F','Birthplace_Elsewhere_P','Lang_spoken_home_Eng_only_M','Lang_spoken_home_Eng_only_F','Lang_spoken_home_Eng_only_P','Lang_spoken_home_Oth_Lang_M','Lang_spoken_home_Oth_Lang_F','Lang_spoken_home_Oth_Lang_P','Australian_citizen_M','Australian_citizen_F','Australian_citizen_P','Age_psns_att_educ_inst_0_4_M','Age_psns_att_educ_inst_0_4_F','Age_psns_att_educ_inst_0_4_P','Age_psns_att_educ_inst_5_14_M','Age_psns_att_educ_inst_5_14_F','Age_psns_att_educ_inst_5_14_P','Age_psns_att_edu_inst_15_19_M','Age_psns_att_edu_inst_15_19_F','Age_psns_att_edu_inst_15_19_P','Age_psns_att_edu_inst_20_24_M','Age_psns_att_edu_inst_20_24_F','Age_psns_att_edu_inst_20_24_P','Age_psns_att_edu_inst_25_ov_M','Age_psns_att_edu_inst_25_ov_F','Age_psns_att_edu_inst_25_ov_P','High_yr_schl_comp_Yr_12_eq_M','High_yr_schl_comp_Yr_12_eq_F','High_yr_schl_comp_Yr_12_eq_P','High_yr_schl_comp_Yr_11_eq_M','High_yr_schl_comp_Yr_11_eq_F','High_yr_schl_comp_Yr_11_eq_P','High_yr_schl_comp_Yr_10_eq_M','High_yr_schl_comp_Yr_10_eq_F','High_yr_schl_comp_Yr_10_eq_P','High_yr_schl_comp_Yr_9_eq_M','High_yr_schl_comp_Yr_9_eq_F','High_yr_schl_comp_Yr_9_eq_P','High_yr_schl_comp_Yr_8_belw_M','High_yr_schl_comp_Yr_8_belw_F','High_yr_schl_comp_Yr_8_belw_P','High_yr_schl_comp_D_n_g_sch_M','High_yr_schl_comp_D_n_g_sch_F','High_yr_schl_comp_D_n_g_sch_P','Count_psns_occ_priv_dwgs_M','Count_psns_occ_priv_dwgs_F','Count_psns_occ_priv_dwgs_P','Count_Persons_other_dwgs_M','Count_Persons_other_dwgs_F','Count_Persons_other_dwgs_P']

        values = insert_df[col_names].to_dict('split')
        values = values['data']
        logging.info(values)

        insert_sql = """
                    INSERT INTO bde_at2.g1_raw (LGA_CODE_2016,Tot_P_M,Tot_P_F,Tot_P_P,Age_0_4_yr_M,Age_0_4_yr_F,Age_0_4_yr_P,Age_5_14_yr_M,Age_5_14_yr_F,Age_5_14_yr_P,Age_15_19_yr_M,Age_15_19_yr_F,Age_15_19_yr_P,Age_20_24_yr_M,Age_20_24_yr_F,Age_20_24_yr_P,Age_25_34_yr_M,Age_25_34_yr_F,Age_25_34_yr_P,Age_35_44_yr_M,Age_35_44_yr_F,Age_35_44_yr_P,Age_45_54_yr_M,Age_45_54_yr_F,Age_45_54_yr_P,Age_55_64_yr_M,Age_55_64_yr_F,Age_55_64_yr_P,Age_65_74_yr_M,Age_65_74_yr_F,Age_65_74_yr_P,Age_75_84_yr_M,Age_75_84_yr_F,Age_75_84_yr_P,Age_85ov_M,Age_85ov_F,Age_85ov_P,Counted_Census_Night_home_M,Counted_Census_Night_home_F,Counted_Census_Night_home_P,Count_Census_Nt_Ewhere_Aust_M,Count_Census_Nt_Ewhere_Aust_F,Count_Census_Nt_Ewhere_Aust_P,Indigenous_psns_Aboriginal_M,Indigenous_psns_Aboriginal_F,Indigenous_psns_Aboriginal_P,Indig_psns_Torres_Strait_Is_M,Indig_psns_Torres_Strait_Is_F,Indig_psns_Torres_Strait_Is_P,Indig_Bth_Abor_Torres_St_Is_M,Indig_Bth_Abor_Torres_St_Is_F,Indig_Bth_Abor_Torres_St_Is_P,Indigenous_P_Tot_M,Indigenous_P_Tot_F,Indigenous_P_Tot_P,Birthplace_Australia_M,Birthplace_Australia_F,Birthplace_Australia_P,Birthplace_Elsewhere_M,Birthplace_Elsewhere_F,Birthplace_Elsewhere_P,Lang_spoken_home_Eng_only_M,Lang_spoken_home_Eng_only_F,Lang_spoken_home_Eng_only_P,Lang_spoken_home_Oth_Lang_M,Lang_spoken_home_Oth_Lang_F,Lang_spoken_home_Oth_Lang_P,Australian_citizen_M,Australian_citizen_F,Australian_citizen_P,Age_psns_att_educ_inst_0_4_M,Age_psns_att_educ_inst_0_4_F,Age_psns_att_educ_inst_0_4_P,Age_psns_att_educ_inst_5_14_M,Age_psns_att_educ_inst_5_14_F,Age_psns_att_educ_inst_5_14_P,Age_psns_att_edu_inst_15_19_M,Age_psns_att_edu_inst_15_19_F,Age_psns_att_edu_inst_15_19_P,Age_psns_att_edu_inst_20_24_M,Age_psns_att_edu_inst_20_24_F,Age_psns_att_edu_inst_20_24_P,Age_psns_att_edu_inst_25_ov_M,Age_psns_att_edu_inst_25_ov_F,Age_psns_att_edu_inst_25_ov_P,High_yr_schl_comp_Yr_12_eq_M,High_yr_schl_comp_Yr_12_eq_F,High_yr_schl_comp_Yr_12_eq_P,High_yr_schl_comp_Yr_11_eq_M,High_yr_schl_comp_Yr_11_eq_F,High_yr_schl_comp_Yr_11_eq_P,High_yr_schl_comp_Yr_10_eq_M,High_yr_schl_comp_Yr_10_eq_F,High_yr_schl_comp_Yr_10_eq_P,High_yr_schl_comp_Yr_9_eq_M,High_yr_schl_comp_Yr_9_eq_F,High_yr_schl_comp_Yr_9_eq_P,High_yr_schl_comp_Yr_8_belw_M,High_yr_schl_comp_Yr_8_belw_F,High_yr_schl_comp_Yr_8_belw_P,High_yr_schl_comp_D_n_g_sch_M,High_yr_schl_comp_D_n_g_sch_F,High_yr_schl_comp_D_n_g_sch_P,Count_psns_occ_priv_dwgs_M,Count_psns_occ_priv_dwgs_F,Count_psns_occ_priv_dwgs_P,Count_Persons_other_dwgs_M,Count_Persons_other_dwgs_F,Count_Persons_other_dwgs_P)
                    VALUES %s
                    """

        result = execute_values(conn_ps.cursor(), insert_sql, values, page_size=len(insert_df))
        conn_ps.commit()
    else:
        None

    return None 

#ABS DATA: G2

def extract_g2_data_func():
    path = "./data/2016Census_G02_NSW_LGA.csv"
    df = pd.read_csv(path, header=0, sep=',', quotechar='"')
    df_dict = df.to_dict()
    return df_dict

def insert_g2_data_func(**kwargs):
         
    ps_pg_hook = PostgresHook(postgres_conn_id="postgres")
    conn_ps = ps_pg_hook.get_conn()

    ti = kwargs['ti']
    insert_df_dict = ti.xcom_pull(task_ids=f'extract_g2_data_task_id')
    insert_df = pd.DataFrame.from_dict(insert_df_dict)

    if len(insert_df) > 0:
        col_names = ['LGA_CODE_2016','Median_age_persons','Median_mortgage_repay_monthly','Median_tot_prsnl_inc_weekly','Median_rent_weekly','Median_tot_fam_inc_weekly','Average_num_psns_per_bedroom','Median_tot_hhd_inc_weekly','Average_household_size']

        values = insert_df[col_names].to_dict('split')
        values = values['data']
        logging.info(values)

        insert_sql = """
                    INSERT INTO bde_at2.g2_raw (LGA_CODE_2016,Median_age_persons,Median_mortgage_repay_monthly,Median_tot_prsnl_inc_weekly,Median_rent_weekly,Median_tot_fam_inc_weekly,Average_num_psns_per_bedroom,Median_tot_hhd_inc_weekly,Average_household_size)
                    VALUES %s
                    """

        result = execute_values(conn_ps.cursor(), insert_sql, values, page_size=len(insert_df))
        conn_ps.commit()
    else:
        None

    return None 

#LGA Names and Codes

def extract_lga_data_func():
    path = "./data/LGA_Codes_and_Names_NSW_2018.csv"
    df = pd.read_csv(path, header=0, sep=',', quotechar='"')
    df_dict = df.to_dict()
    return df_dict

def insert_lga_data_func(**kwargs):
         
    ps_pg_hook = PostgresHook(postgres_conn_id="postgres")
    conn_ps = ps_pg_hook.get_conn()

    ti = kwargs['ti']
    insert_df_dict = ti.xcom_pull(task_ids=f'extract_lga_data_task_id')
    insert_df = pd.DataFrame.from_dict(insert_df_dict)

    if len(insert_df) > 0:
        col_names = ['neighbourhood_cleansed', 'LGA_name', 'CODE_2016']

        values = insert_df[col_names].to_dict('split')
        values = values['data']
        logging.info(values)

        insert_sql = """
                    INSERT INTO bde_at2.lga_raw (neighbourhood_cleansed, LGA_name, CODE_2016)
                    VALUES %s
                    """

        result = execute_values(conn_ps.cursor(), insert_sql, values, page_size=len(insert_df))
        conn_ps.commit()
    else:
        None

    return None 


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

##################ABS Data: G1#############################

extract_g1_data_task = PythonOperator(
    task_id="extract_g1_data_task_id",
    python_callable=extract_g1_data_func,
    op_kwargs={},
    provide_context=True,
    dag=dag
)

insert_g1_data_task = PythonOperator(
    task_id="insert_g1_data_task_id",
    python_callable=insert_g1_data_func,
    op_kwargs={},
    provide_context=True,
    dag=dag
)

create_g1_table_task = PostgresOperator(
    task_id="create_g1_table_task_id",
    postgres_conn_id="postgres",
    sql="""
        CREATE TABLE IF NOT EXISTS bde_at2.g1_raw (
            LGA_CODE_2016                                TEXT,
            Tot_P_M                                      TEXT,
            Tot_P_F                                      TEXT,
            Tot_P_P                                      TEXT,
            Age_0_4_yr_M                                 TEXT,
            Age_0_4_yr_F                                 TEXT,
            Age_0_4_yr_P                                 TEXT,
            Age_5_14_yr_M                                TEXT,
            Age_5_14_yr_F                                TEXT,
            Age_5_14_yr_P                                TEXT,
            Age_15_19_yr_M                               TEXT,
            Age_15_19_yr_F                               TEXT,
            Age_15_19_yr_P                               TEXT,
            Age_20_24_yr_M                               TEXT,
            Age_20_24_yr_F                               TEXT,
            Age_20_24_yr_P                               TEXT,
            Age_25_34_yr_M                               TEXT,
            Age_25_34_yr_F                               TEXT,
            Age_25_34_yr_P                               TEXT,
            Age_35_44_yr_M                               TEXT,
            Age_35_44_yr_F                               TEXT,
            Age_35_44_yr_P                               TEXT,
            Age_45_54_yr_M                               TEXT,
            Age_45_54_yr_F                               TEXT,
            Age_45_54_yr_P                               TEXT,
            Age_55_64_yr_M                               TEXT,
            Age_55_64_yr_F                               TEXT,
            Age_55_64_yr_P                               TEXT,
            Age_65_74_yr_M                               TEXT,
            Age_65_74_yr_F                               TEXT,
            Age_65_74_yr_P                               TEXT,
            Age_75_84_yr_M                               TEXT,
            Age_75_84_yr_F                               TEXT,
            Age_75_84_yr_P                               TEXT,
            Age_85ov_M                                   TEXT,
            Age_85ov_F                                   TEXT,
            Age_85ov_P                                   TEXT,
            Counted_Census_Night_home_M                  TEXT,
            Counted_Census_Night_home_F                  TEXT,
            Counted_Census_Night_home_P                  TEXT,
            Count_Census_Nt_Ewhere_Aust_M                TEXT,
            Count_Census_Nt_Ewhere_Aust_F                TEXT,
            Count_Census_Nt_Ewhere_Aust_P                TEXT,
            Indigenous_psns_Aboriginal_M                 TEXT,
            Indigenous_psns_Aboriginal_F                 TEXT,
            Indigenous_psns_Aboriginal_P                 TEXT,
            Indig_psns_Torres_Strait_Is_M                TEXT,
            Indig_psns_Torres_Strait_Is_F                TEXT,
            Indig_psns_Torres_Strait_Is_P                TEXT,
            Indig_Bth_Abor_Torres_St_Is_M                TEXT,
            Indig_Bth_Abor_Torres_St_Is_F                TEXT,
            Indig_Bth_Abor_Torres_St_Is_P                TEXT,
            Indigenous_P_Tot_M                           TEXT,
            Indigenous_P_Tot_F                           TEXT,
            Indigenous_P_Tot_P                           TEXT,
            Birthplace_Australia_M                       TEXT,
            Birthplace_Australia_F                       TEXT,
            Birthplace_Australia_P                       TEXT,
            Birthplace_Elsewhere_M                       TEXT,
            Birthplace_Elsewhere_F                       TEXT,
            Birthplace_Elsewhere_P                       TEXT,
            Lang_spoken_home_Eng_only_M                  TEXT,
            Lang_spoken_home_Eng_only_F                  TEXT,
            Lang_spoken_home_Eng_only_P                  TEXT,
            Lang_spoken_home_Oth_Lang_M                  TEXT,
            Lang_spoken_home_Oth_Lang_F                  TEXT,
            Lang_spoken_home_Oth_Lang_P                  TEXT,
            Australian_citizen_M                         TEXT,
            Australian_citizen_F                         TEXT,
            Australian_citizen_P                         TEXT,
            Age_psns_att_educ_inst_0_4_M                 TEXT,
            Age_psns_att_educ_inst_0_4_F                 TEXT,
            Age_psns_att_educ_inst_0_4_P                 TEXT,
            Age_psns_att_educ_inst_5_14_M                TEXT,
            Age_psns_att_educ_inst_5_14_F                TEXT,
            Age_psns_att_educ_inst_5_14_P                TEXT,
            Age_psns_att_edu_inst_15_19_M                TEXT,
            Age_psns_att_edu_inst_15_19_F                TEXT,
            Age_psns_att_edu_inst_15_19_P                TEXT,
            Age_psns_att_edu_inst_20_24_M                TEXT,
            Age_psns_att_edu_inst_20_24_F                TEXT,
            Age_psns_att_edu_inst_20_24_P                TEXT,
            Age_psns_att_edu_inst_25_ov_M                TEXT,
            Age_psns_att_edu_inst_25_ov_F                TEXT,
            Age_psns_att_edu_inst_25_ov_P                TEXT,
            High_yr_schl_comp_Yr_12_eq_M                 TEXT,
            High_yr_schl_comp_Yr_12_eq_F                 TEXT,
            High_yr_schl_comp_Yr_12_eq_P                 TEXT,
            High_yr_schl_comp_Yr_11_eq_M                 TEXT,
            High_yr_schl_comp_Yr_11_eq_F                 TEXT,
            High_yr_schl_comp_Yr_11_eq_P                 TEXT,
            High_yr_schl_comp_Yr_10_eq_M                 TEXT,
            High_yr_schl_comp_Yr_10_eq_F                 TEXT,
            High_yr_schl_comp_Yr_10_eq_P                 TEXT,
            High_yr_schl_comp_Yr_9_eq_M                  TEXT,
            High_yr_schl_comp_Yr_9_eq_F                  TEXT,
            High_yr_schl_comp_Yr_9_eq_P                  TEXT,
            High_yr_schl_comp_Yr_8_belw_M                TEXT,
            High_yr_schl_comp_Yr_8_belw_F                TEXT,
            High_yr_schl_comp_Yr_8_belw_P                TEXT,
            High_yr_schl_comp_D_n_g_sch_M                TEXT,
            High_yr_schl_comp_D_n_g_sch_F                TEXT,
            High_yr_schl_comp_D_n_g_sch_P                TEXT,
            Count_psns_occ_priv_dwgs_M                   TEXT,
            Count_psns_occ_priv_dwgs_F                   TEXT,
            Count_psns_occ_priv_dwgs_P                   TEXT,
            Count_Persons_other_dwgs_M                   TEXT,
            Count_Persons_other_dwgs_F                   TEXT,
            Count_Persons_other_dwgs_P                   TEXT
            );
    """,
    dag=dag
)

##################ABS Data: G2#############################

extract_g2_data_task = PythonOperator(
    task_id="extract_g2_data_task_id",
    python_callable=extract_g2_data_func,
    op_kwargs={},
    provide_context=True,
    dag=dag
)

insert_g2_data_task = PythonOperator(
    task_id="insert_g2_data_task_id",
    python_callable=insert_g2_data_func,
    op_kwargs={},
    provide_context=True,
    dag=dag
)

create_g2_table_task = PostgresOperator(
    task_id="create_g2_table_task_id",
    postgres_conn_id="postgres",
    sql="""
        CREATE TABLE IF NOT EXISTS bde_at2.g2_raw (
            LGA_CODE_2016                   TEXT,
            Median_age_persons              TEXT,
            Median_mortgage_repay_monthly   TEXT,
            Median_tot_prsnl_inc_weekly     TEXT,
            Median_rent_weekly              TEXT,
            Median_tot_fam_inc_weekly       TEXT,
            Average_num_psns_per_bedroom    TEXT,
            Median_tot_hhd_inc_weekly       TEXT,
            Average_household_size          TEXT
            );
    """,
    dag=dag
)

################## LGA Names and Codes #############################

extract_lga_data_task = PythonOperator(
    task_id="extract_lga_data_task_id",
    python_callable=extract_lga_data_func,
    op_kwargs={},
    provide_context=True,
    dag=dag
)

insert_lga_data_task = PythonOperator(
    task_id="insert_lga_data_task_id",
    python_callable=insert_lga_data_func,
    op_kwargs={},
    provide_context=True,
    dag=dag
)

create_lga_table_task = PostgresOperator(
    task_id="create_lga_table_task_id",
    postgres_conn_id="postgres",
    sql="""
        CREATE TABLE IF NOT EXISTS bde_at2.lga_raw (
            neighbourhood_cleansed  TEXT,
            LGA_name                TEXT,
            CODE_2016               TEXT
            );
    """,
    dag=dag
)

create_psql_schema_task >> create_g1_table_task >> extract_g1_data_task >> insert_g1_data_task >> create_g2_table_task >> extract_g2_data_task >> insert_g2_data_task >> create_lga_table_task >> extract_lga_data_task >> insert_lga_data_task