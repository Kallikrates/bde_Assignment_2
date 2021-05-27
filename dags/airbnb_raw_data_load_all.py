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
    dag_id='airbnb_raw_data_load_all',
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

#Extract and Load Air BnB data: May 2020

    # Local file to df 
def air_05_2020_func():
    path = "./data/05_2020.csv"
    df = pd.read_csv(path, sep=',',header=0)
    df["month"] = "May"
    df["year"] = 2020

    # Create host_neighbourhood_cleansed column
    
    lga_path = "./data/LGA_Codes_to_Suburbs_NSW.csv"
    lga = pd.read_csv(lga_path, sep=',',header=0)

    df = pd.merge(df, 
                  lga, 
                  on ='host_neighbourhood', 
                  how ='left')

    df.rename(columns={'LGA':'host_neighbourhood_cleansed'}, inplace=True)

    # Transform to conform to raw file structure

    # Add blank columns
    df["bathrooms_text"] = ""
    df["number_of_reviews_l30d"] = ""
    
    # Drop columns
    df.drop(['access'], axis = 1)
    df.drop(['bed_type'], axis = 1)
    df.drop(['cancellation_policy'], axis = 1)
    df.drop(['city'], axis = 1)
    df.drop(['cleaning_fee'], axis = 1)
    df.drop(['country'], axis = 1)
    df.drop(['country_code'], axis = 1)
    df.drop(['experiences_offered'], axis = 1)
    df.drop(['extra_people'], axis = 1)
    df.drop(['guests_included'], axis = 1)
    df.drop(['house_rules'], axis = 1)
    df.drop(['interaction'], axis = 1)
    df.drop(['is_business_travel_ready'], axis = 1)
    df.drop(['is_location_exact'], axis = 1)
    df.drop(['jurisdiction_names'], axis = 1)
    df.drop(['market'], axis = 1)
    df.drop(['medium_url'], axis = 1)
    df.drop(['monthly_price'], axis = 1)
    df.drop(['notes'], axis = 1)
    df.drop(['require_guest_phone_verification'], axis = 1)
    df.drop(['require_guest_profile_picture'], axis = 1)
    df.drop(['requires_license'], axis = 1)
    df.drop(['security_deposit'], axis = 1)
    df.drop(['smart_location'], axis = 1)
    df.drop(['space'], axis = 1)
    df.drop(['square_feet'], axis = 1)
    df.drop(['state'], axis = 1)
    df.drop(['street'], axis = 1)
    df.drop(['summary'], axis = 1)
    df.drop(['thumbnail_url'], axis = 1)
    df.drop(['transit'], axis = 1)
    df.drop(['weekly_price'], axis = 1)
    df.drop(['xl_picture_url'], axis = 1)
    df.drop(['zipcode'], axis = 1)

    # Df to dictionary
    df_dict = df.to_dict()
    
    #Postgress connection         
    ps_pg_hook = PostgresHook(postgres_conn_id="postgres")
    conn_ps = ps_pg_hook.get_conn()
    
    # Dictionary to DF DB Insert
    insert_df_dict = df_dict
    insert_df = pd.DataFrame.from_dict(insert_df_dict)

    if len(insert_df) > 0:
        col_names = ['id','listing_url','scrape_id','last_scraped','name','description','neighborhood_overview','picture_url','host_id','host_url','host_name','host_since','host_location','host_about','host_response_time','host_response_rate','host_acceptance_rate','host_is_superhost','host_thumbnail_url','host_picture_url','host_neighbourhood','host_listings_count','host_total_listings_count','host_verifications','host_has_profile_pic','host_identity_verified','neighbourhood','neighbourhood_cleansed','neighbourhood_group_cleansed','latitude','longitude','property_type','room_type','accommodates','bathrooms','bathrooms_text','bedrooms','beds','amenities','price','minimum_nights','maximum_nights','minimum_minimum_nights','maximum_minimum_nights','minimum_maximum_nights','maximum_maximum_nights','minimum_nights_avg_ntm','maximum_nights_avg_ntm','calendar_updated','has_availability','availability_30','availability_60','availability_90','availability_365','calendar_last_scraped','number_of_reviews','number_of_reviews_ltm','number_of_reviews_l30d','first_review','last_review','review_scores_rating','review_scores_accuracy','review_scores_cleanliness','review_scores_checkin','review_scores_communication','review_scores_location','review_scores_value','license','instant_bookable','calculated_host_listings_count','calculated_host_listings_count_entire_homes','calculated_host_listings_count_private_rooms','calculated_host_listings_count_shared_rooms','reviews_per_month','month','year','host_neighbourhood_cleansed']

        values = insert_df[col_names].to_dict('split')
        values = values['data']
        logging.info(values)

        insert_sql = """
                    INSERT INTO bde_at2.airbnb_raw (id, listing_url,scrape_id, last_scraped, name, description, neighborhood_overview, picture_url, host_id, host_url, host_name, host_since, host_location, host_about, host_response_time, host_response_rate, host_acceptance_rate, host_is_superhost, host_thumbnail_url, host_picture_url, host_neighbourhood, host_listings_count, host_total_listings_count, host_verifications, host_has_profile_pic, host_identity_verified, neighbourhood, neighbourhood_cleansed, neighbourhood_group_cleansed, latitude, longitude, property_type, room_type, accommodates, bathrooms, bathrooms_text, bedrooms, beds, amenities, price, minimum_nights, maximum_nights, minimum_minimum_nights, maximum_minimum_nights, minimum_maximum_nights, maximum_maximum_nights, minimum_nights_avg_ntm, maximum_nights_avg_ntm, calendar_updated, has_availability, availability_30, availability_60, availability_90, availability_365, calendar_last_scraped, number_of_reviews, number_of_reviews_ltm, number_of_reviews_l30d, first_review, last_review, review_scores_rating, review_scores_accuracy, review_scores_cleanliness, review_scores_checkin, review_scores_communication, review_scores_location, review_scores_value, license, instant_bookable, calculated_host_listings_count, calculated_host_listings_count_entire_homes, calculated_host_listings_count_private_rooms, calculated_host_listings_count_shared_rooms, reviews_per_month,month,year,host_neighbourhood_cleansed)
                    VALUES %s
                    """

        result = execute_values(conn_ps.cursor(), insert_sql, values, page_size=len(insert_df))
        conn_ps.commit()
    else:
        None

    return None

#Extract and Load Air BnB data: June 2020

    # Local file to df 
def air_06_2020_func():
    path = "./data/06_2020.csv"
    df = pd.read_csv(path, sep=',',header=0)
    df["month"] = "June"
    df["year"] = 2020

    # Create host_neighbourhood_cleansed column
    
    lga_path = "./data/LGA_Codes_to_Suburbs_NSW.csv"
    lga = pd.read_csv(lga_path, sep=',',header=0)

    df = pd.merge(df, 
                  lga, 
                  on ='host_neighbourhood', 
                  how ='left')

    df.rename(columns={'LGA':'host_neighbourhood_cleansed'}, inplace=True)
    
    # Transform to conform to raw file structure

    # Add blank columns
    df["bathrooms_text"] = ""
    df["number_of_reviews_l30d"] = ""
    
    # Drop columns
    df.drop(['access'], axis = 1)
    df.drop(['bed_type'], axis = 1)
    df.drop(['cancellation_policy'], axis = 1)
    df.drop(['city'], axis = 1)
    df.drop(['cleaning_fee'], axis = 1)
    df.drop(['country'], axis = 1)
    df.drop(['country_code'], axis = 1)
    df.drop(['experiences_offered'], axis = 1)
    df.drop(['extra_people'], axis = 1)
    df.drop(['guests_included'], axis = 1)
    df.drop(['house_rules'], axis = 1)
    df.drop(['interaction'], axis = 1)
    df.drop(['is_business_travel_ready'], axis = 1)
    df.drop(['is_location_exact'], axis = 1)
    df.drop(['jurisdiction_names'], axis = 1)
    df.drop(['market'], axis = 1)
    df.drop(['medium_url'], axis = 1)
    df.drop(['monthly_price'], axis = 1)
    df.drop(['notes'], axis = 1)
    df.drop(['require_guest_phone_verification'], axis = 1)
    df.drop(['require_guest_profile_picture'], axis = 1)
    df.drop(['requires_license'], axis = 1)
    df.drop(['security_deposit'], axis = 1)
    df.drop(['smart_location'], axis = 1)
    df.drop(['space'], axis = 1)
    df.drop(['square_feet'], axis = 1)
    df.drop(['state'], axis = 1)
    df.drop(['street'], axis = 1)
    df.drop(['summary'], axis = 1)
    df.drop(['thumbnail_url'], axis = 1)
    df.drop(['transit'], axis = 1)
    df.drop(['weekly_price'], axis = 1)
    df.drop(['xl_picture_url'], axis = 1)
    df.drop(['zipcode'], axis = 1)

    # Df to dictionary
    df_dict = df.to_dict()

    #Postgress connection         
    ps_pg_hook = PostgresHook(postgres_conn_id="postgres")
    conn_ps = ps_pg_hook.get_conn()
    
    # Dictionary to DF DB Insert
    insert_df_dict = df_dict
    insert_df = pd.DataFrame.from_dict(insert_df_dict)

    if len(insert_df) > 0:
        col_names = ['id','listing_url','scrape_id','last_scraped','name','description','neighborhood_overview','picture_url','host_id','host_url','host_name','host_since','host_location','host_about','host_response_time','host_response_rate','host_acceptance_rate','host_is_superhost','host_thumbnail_url','host_picture_url','host_neighbourhood','host_listings_count','host_total_listings_count','host_verifications','host_has_profile_pic','host_identity_verified','neighbourhood','neighbourhood_cleansed','neighbourhood_group_cleansed','latitude','longitude','property_type','room_type','accommodates','bathrooms','bathrooms_text','bedrooms','beds','amenities','price','minimum_nights','maximum_nights','minimum_minimum_nights','maximum_minimum_nights','minimum_maximum_nights','maximum_maximum_nights','minimum_nights_avg_ntm','maximum_nights_avg_ntm','calendar_updated','has_availability','availability_30','availability_60','availability_90','availability_365','calendar_last_scraped','number_of_reviews','number_of_reviews_ltm','number_of_reviews_l30d','first_review','last_review','review_scores_rating','review_scores_accuracy','review_scores_cleanliness','review_scores_checkin','review_scores_communication','review_scores_location','review_scores_value','license','instant_bookable','calculated_host_listings_count','calculated_host_listings_count_entire_homes','calculated_host_listings_count_private_rooms','calculated_host_listings_count_shared_rooms','reviews_per_month','month','year','host_neighbourhood_cleansed']

        values = insert_df[col_names].to_dict('split')
        values = values['data']
        logging.info(values)

        insert_sql = """
                    INSERT INTO bde_at2.airbnb_raw (id, listing_url,scrape_id, last_scraped, name, description, neighborhood_overview, picture_url, host_id, host_url, host_name, host_since, host_location, host_about, host_response_time, host_response_rate, host_acceptance_rate, host_is_superhost, host_thumbnail_url, host_picture_url, host_neighbourhood, host_listings_count, host_total_listings_count, host_verifications, host_has_profile_pic, host_identity_verified, neighbourhood, neighbourhood_cleansed, neighbourhood_group_cleansed, latitude, longitude, property_type, room_type, accommodates, bathrooms, bathrooms_text, bedrooms, beds, amenities, price, minimum_nights, maximum_nights, minimum_minimum_nights, maximum_minimum_nights, minimum_maximum_nights, maximum_maximum_nights, minimum_nights_avg_ntm, maximum_nights_avg_ntm, calendar_updated, has_availability, availability_30, availability_60, availability_90, availability_365, calendar_last_scraped, number_of_reviews, number_of_reviews_ltm, number_of_reviews_l30d, first_review, last_review, review_scores_rating, review_scores_accuracy, review_scores_cleanliness, review_scores_checkin, review_scores_communication, review_scores_location, review_scores_value, license, instant_bookable, calculated_host_listings_count, calculated_host_listings_count_entire_homes, calculated_host_listings_count_private_rooms, calculated_host_listings_count_shared_rooms, reviews_per_month,month,year,host_neighbourhood_cleansed)
                    VALUES %s
                    """

        result = execute_values(conn_ps.cursor(), insert_sql, values, page_size=len(insert_df))
        conn_ps.commit()
    else:
        None

    return None

#Extract and Load Air BnB data: July 2020

    # Local file to dictionary 
def air_07_2020_func():
    path = "./data/07_2020.csv"
    df = pd.read_csv(path, sep=',',header=0)
    df["month"] = "July"
    df["year"] = 2020

    # Create host_neighbourhood_cleansed column
    
    lga_path = "./data/LGA_Codes_to_Suburbs_NSW.csv"
    lga = pd.read_csv(lga_path, sep=',',header=0)

    df = pd.merge(df, 
                  lga, 
                  on ='host_neighbourhood', 
                  how ='left')

    df.rename(columns={'LGA':'host_neighbourhood_cleansed'}, inplace=True)

 # Transform to conform to raw file structure

    # Add blank columns
    df["bathrooms_text"] = ""
    df["bedrooms"] = ""
    df["beds"] = ""
    df["amenities"] = ""
    df["minimum_nights"] = ""
    df["maximum_nights"] = ""
    df["number_of_reviews_l30d"] = ""


    # Drop columns
    df.drop(['access'], axis = 1)
    df.drop(['bed_type'], axis = 1)
    df.drop(['cancellation_policy'], axis = 1)
    df.drop(['city'], axis = 1)
    df.drop(['cleaning_fee'], axis = 1)
    df.drop(['country'], axis = 1)
    df.drop(['country_code'], axis = 1)
    df.drop(['experiences_offered'], axis = 1)
    df.drop(['extra_people'], axis = 1)
    df.drop(['guests_included'], axis = 1)
    df.drop(['house_rules'], axis = 1)
    df.drop(['interaction'], axis = 1)
    df.drop(['is_business_travel_ready'], axis = 1)
    df.drop(['is_location_exact'], axis = 1)
    df.drop(['jurisdiction_names'], axis = 1)
    df.drop(['market'], axis = 1)
    df.drop(['medium_url'], axis = 1)
    df.drop(['monthly_price'], axis = 1)
    df.drop(['notes'], axis = 1)
    df.drop(['require_guest_phone_verification'], axis = 1)
    df.drop(['require_guest_profile_picture'], axis = 1)
    df.drop(['requires_license'], axis = 1)
    df.drop(['security_deposit'], axis = 1)
    df.drop(['smart_location'], axis = 1)
    df.drop(['space'], axis = 1)
    df.drop(['square_feet'], axis = 1)
    df.drop(['state'], axis = 1)
    df.drop(['street'], axis = 1)
    df.drop(['summary'], axis = 1)
    df.drop(['thumbnail_url'], axis = 1)
    df.drop(['transit'], axis = 1)
    df.drop(['weekly_price'], axis = 1)
    df.drop(['xl_picture_url'], axis = 1)
    df.drop(['zipcode'], axis = 1)

    # Df to dictionary
    df_dict = df.to_dict()
    
    #Postgress connection         
    ps_pg_hook = PostgresHook(postgres_conn_id="postgres")
    conn_ps = ps_pg_hook.get_conn()
    
    # Dictionary to DF DB Insert
    insert_df_dict = df_dict
    insert_df = pd.DataFrame.from_dict(insert_df_dict)

    if len(insert_df) > 0:
        col_names = ['id','listing_url','scrape_id','last_scraped','name','description','neighborhood_overview','picture_url','host_id','host_url','host_name','host_since','host_location','host_about','host_response_time','host_response_rate','host_acceptance_rate','host_is_superhost','host_thumbnail_url','host_picture_url','host_neighbourhood','host_listings_count','host_total_listings_count','host_verifications','host_has_profile_pic','host_identity_verified','neighbourhood','neighbourhood_cleansed','neighbourhood_group_cleansed','latitude','longitude','property_type','room_type','accommodates','bathrooms','bathrooms_text','bedrooms','beds','amenities','price','minimum_nights','maximum_nights','minimum_minimum_nights','maximum_minimum_nights','minimum_maximum_nights','maximum_maximum_nights','minimum_nights_avg_ntm','maximum_nights_avg_ntm','calendar_updated','has_availability','availability_30','availability_60','availability_90','availability_365','calendar_last_scraped','number_of_reviews','number_of_reviews_ltm','number_of_reviews_l30d','first_review','last_review','review_scores_rating','review_scores_accuracy','review_scores_cleanliness','review_scores_checkin','review_scores_communication','review_scores_location','review_scores_value','license','instant_bookable','calculated_host_listings_count','calculated_host_listings_count_entire_homes','calculated_host_listings_count_private_rooms','calculated_host_listings_count_shared_rooms','reviews_per_month','month','year','host_neighbourhood_cleansed']

        values = insert_df[col_names].to_dict('split')
        values = values['data']
        logging.info(values)

        insert_sql = """
                    INSERT INTO bde_at2.airbnb_raw (id, listing_url,scrape_id, last_scraped, name, description, neighborhood_overview, picture_url, host_id, host_url, host_name, host_since, host_location, host_about, host_response_time, host_response_rate, host_acceptance_rate, host_is_superhost, host_thumbnail_url, host_picture_url, host_neighbourhood, host_listings_count, host_total_listings_count, host_verifications, host_has_profile_pic, host_identity_verified, neighbourhood, neighbourhood_cleansed, neighbourhood_group_cleansed, latitude, longitude, property_type, room_type, accommodates, bathrooms, bathrooms_text, bedrooms, beds, amenities, price, minimum_nights, maximum_nights, minimum_minimum_nights, maximum_minimum_nights, minimum_maximum_nights, maximum_maximum_nights, minimum_nights_avg_ntm, maximum_nights_avg_ntm, calendar_updated, has_availability, availability_30, availability_60, availability_90, availability_365, calendar_last_scraped, number_of_reviews, number_of_reviews_ltm, number_of_reviews_l30d, first_review, last_review, review_scores_rating, review_scores_accuracy, review_scores_cleanliness, review_scores_checkin, review_scores_communication, review_scores_location, review_scores_value, license, instant_bookable, calculated_host_listings_count, calculated_host_listings_count_entire_homes, calculated_host_listings_count_private_rooms, calculated_host_listings_count_shared_rooms, reviews_per_month,month,year,host_neighbourhood_cleansed)
                    VALUES %s
                    """

        result = execute_values(conn_ps.cursor(), insert_sql, values, page_size=len(insert_df))
        conn_ps.commit()
    else:
        None

    return None


#Extract and Load Air BnB data: August 2020

    # Local file to dictionary 
def air_08_2020_func():
    path = "./data/08_2020.csv"
    df = pd.read_csv(path, sep=',',header=0)
    df["month"] = "August"
    df["year"] = 2020

    # Create host_neighbourhood_cleansed column
    
    lga_path = "./data/LGA_Codes_to_Suburbs_NSW.csv"
    lga = pd.read_csv(lga_path, sep=',',header=0)

    df = pd.merge(df, 
                  lga, 
                  on ='host_neighbourhood', 
                  how ='left')

    df.rename(columns={'LGA':'host_neighbourhood_cleansed'}, inplace=True)

    df_dict = df.to_dict()
    
    #Postgress connection         
    ps_pg_hook = PostgresHook(postgres_conn_id="postgres")
    conn_ps = ps_pg_hook.get_conn()
    
    # Dictionary to DF DB Insert
    insert_df_dict = df_dict
    insert_df = pd.DataFrame.from_dict(insert_df_dict)

    if len(insert_df) > 0:
        col_names = ['id','listing_url','scrape_id','last_scraped','name','description','neighborhood_overview','picture_url','host_id','host_url','host_name','host_since','host_location','host_about','host_response_time','host_response_rate','host_acceptance_rate','host_is_superhost','host_thumbnail_url','host_picture_url','host_neighbourhood','host_listings_count','host_total_listings_count','host_verifications','host_has_profile_pic','host_identity_verified','neighbourhood','neighbourhood_cleansed','neighbourhood_group_cleansed','latitude','longitude','property_type','room_type','accommodates','bathrooms','bathrooms_text','bedrooms','beds','amenities','price','minimum_nights','maximum_nights','minimum_minimum_nights','maximum_minimum_nights','minimum_maximum_nights','maximum_maximum_nights','minimum_nights_avg_ntm','maximum_nights_avg_ntm','calendar_updated','has_availability','availability_30','availability_60','availability_90','availability_365','calendar_last_scraped','number_of_reviews','number_of_reviews_ltm','number_of_reviews_l30d','first_review','last_review','review_scores_rating','review_scores_accuracy','review_scores_cleanliness','review_scores_checkin','review_scores_communication','review_scores_location','review_scores_value','license','instant_bookable','calculated_host_listings_count','calculated_host_listings_count_entire_homes','calculated_host_listings_count_private_rooms','calculated_host_listings_count_shared_rooms','reviews_per_month','month','year','host_neighbourhood_cleansed']

        values = insert_df[col_names].to_dict('split')
        values = values['data']
        logging.info(values)

        insert_sql = """
                    INSERT INTO bde_at2.airbnb_raw (id, listing_url,scrape_id, last_scraped, name, description, neighborhood_overview, picture_url, host_id, host_url, host_name, host_since, host_location, host_about, host_response_time, host_response_rate, host_acceptance_rate, host_is_superhost, host_thumbnail_url, host_picture_url, host_neighbourhood, host_listings_count, host_total_listings_count, host_verifications, host_has_profile_pic, host_identity_verified, neighbourhood, neighbourhood_cleansed, neighbourhood_group_cleansed, latitude, longitude, property_type, room_type, accommodates, bathrooms, bathrooms_text, bedrooms, beds, amenities, price, minimum_nights, maximum_nights, minimum_minimum_nights, maximum_minimum_nights, minimum_maximum_nights, maximum_maximum_nights, minimum_nights_avg_ntm, maximum_nights_avg_ntm, calendar_updated, has_availability, availability_30, availability_60, availability_90, availability_365, calendar_last_scraped, number_of_reviews, number_of_reviews_ltm, number_of_reviews_l30d, first_review, last_review, review_scores_rating, review_scores_accuracy, review_scores_cleanliness, review_scores_checkin, review_scores_communication, review_scores_location, review_scores_value, license, instant_bookable, calculated_host_listings_count, calculated_host_listings_count_entire_homes, calculated_host_listings_count_private_rooms, calculated_host_listings_count_shared_rooms, reviews_per_month,month,year,host_neighbourhood_cleansed)
                    VALUES %s
                    """

        result = execute_values(conn_ps.cursor(), insert_sql, values, page_size=len(insert_df))
        conn_ps.commit()
    else:
        None

    return None

#Extract and Load Air BnB data: September 2020

    # Local file to dictionary 
def air_09_2020_func():
    path = "./data/09_2020.csv"
    df = pd.read_csv(path, sep=',',header=0)
    df["month"] = "September"
    df["year"] = 2020

    # Create host_neighbourhood_cleansed column
    
    lga_path = "./data/LGA_Codes_to_Suburbs_NSW.csv"
    lga = pd.read_csv(lga_path, sep=',',header=0)

    df = pd.merge(df, 
                  lga, 
                  on ='host_neighbourhood', 
                  how ='left')

    df.rename(columns={'LGA':'host_neighbourhood_cleansed'}, inplace=True)
    df_dict = df.to_dict()
    
    #Postgress connection         
    ps_pg_hook = PostgresHook(postgres_conn_id="postgres")
    conn_ps = ps_pg_hook.get_conn()
    
    # Dictionary to DF DB Insert
    insert_df_dict = df_dict
    insert_df = pd.DataFrame.from_dict(insert_df_dict)

    if len(insert_df) > 0:
        col_names = ['id','listing_url','scrape_id','last_scraped','name','description','neighborhood_overview','picture_url','host_id','host_url','host_name','host_since','host_location','host_about','host_response_time','host_response_rate','host_acceptance_rate','host_is_superhost','host_thumbnail_url','host_picture_url','host_neighbourhood','host_listings_count','host_total_listings_count','host_verifications','host_has_profile_pic','host_identity_verified','neighbourhood','neighbourhood_cleansed','neighbourhood_group_cleansed','latitude','longitude','property_type','room_type','accommodates','bathrooms','bathrooms_text','bedrooms','beds','amenities','price','minimum_nights','maximum_nights','minimum_minimum_nights','maximum_minimum_nights','minimum_maximum_nights','maximum_maximum_nights','minimum_nights_avg_ntm','maximum_nights_avg_ntm','calendar_updated','has_availability','availability_30','availability_60','availability_90','availability_365','calendar_last_scraped','number_of_reviews','number_of_reviews_ltm','number_of_reviews_l30d','first_review','last_review','review_scores_rating','review_scores_accuracy','review_scores_cleanliness','review_scores_checkin','review_scores_communication','review_scores_location','review_scores_value','license','instant_bookable','calculated_host_listings_count','calculated_host_listings_count_entire_homes','calculated_host_listings_count_private_rooms','calculated_host_listings_count_shared_rooms','reviews_per_month','month','year','host_neighbourhood_cleansed']

        values = insert_df[col_names].to_dict('split')
        values = values['data']
        logging.info(values)

        insert_sql = """
                    INSERT INTO bde_at2.airbnb_raw (id, listing_url,scrape_id, last_scraped, name, description, neighborhood_overview, picture_url, host_id, host_url, host_name, host_since, host_location, host_about, host_response_time, host_response_rate, host_acceptance_rate, host_is_superhost, host_thumbnail_url, host_picture_url, host_neighbourhood, host_listings_count, host_total_listings_count, host_verifications, host_has_profile_pic, host_identity_verified, neighbourhood, neighbourhood_cleansed, neighbourhood_group_cleansed, latitude, longitude, property_type, room_type, accommodates, bathrooms, bathrooms_text, bedrooms, beds, amenities, price, minimum_nights, maximum_nights, minimum_minimum_nights, maximum_minimum_nights, minimum_maximum_nights, maximum_maximum_nights, minimum_nights_avg_ntm, maximum_nights_avg_ntm, calendar_updated, has_availability, availability_30, availability_60, availability_90, availability_365, calendar_last_scraped, number_of_reviews, number_of_reviews_ltm, number_of_reviews_l30d, first_review, last_review, review_scores_rating, review_scores_accuracy, review_scores_cleanliness, review_scores_checkin, review_scores_communication, review_scores_location, review_scores_value, license, instant_bookable, calculated_host_listings_count, calculated_host_listings_count_entire_homes, calculated_host_listings_count_private_rooms, calculated_host_listings_count_shared_rooms, reviews_per_month,month,year,host_neighbourhood_cleansed)
                    VALUES %s
                    """

        result = execute_values(conn_ps.cursor(), insert_sql, values, page_size=len(insert_df))
        conn_ps.commit()
    else:
        None

    return None

#Extract and Load Air BnB data: October 2020

    # Local file to dictionary 
def air_10_2020_func():
    path = "./data/10_2020.csv"
    df = pd.read_csv(path, sep=',',header=0)
    df["month"] = "October"
    df["year"] = 2020

    # Create host_neighbourhood_cleansed column
    
    lga_path = "./data/LGA_Codes_to_Suburbs_NSW.csv"
    lga = pd.read_csv(lga_path, sep=',',header=0)

    df = pd.merge(df, 
                  lga, 
                  on ='host_neighbourhood', 
                  how ='left')

    df.rename(columns={'LGA':'host_neighbourhood_cleansed'}, inplace=True)

    df_dict = df.to_dict()
    
    #Postgress connection         
    ps_pg_hook = PostgresHook(postgres_conn_id="postgres")
    conn_ps = ps_pg_hook.get_conn()
    
    # Dictionary to DF DB Insert
    insert_df_dict = df_dict
    insert_df = pd.DataFrame.from_dict(insert_df_dict)

    if len(insert_df) > 0:
        col_names = ['id','listing_url','scrape_id','last_scraped','name','description','neighborhood_overview','picture_url','host_id','host_url','host_name','host_since','host_location','host_about','host_response_time','host_response_rate','host_acceptance_rate','host_is_superhost','host_thumbnail_url','host_picture_url','host_neighbourhood','host_listings_count','host_total_listings_count','host_verifications','host_has_profile_pic','host_identity_verified','neighbourhood','neighbourhood_cleansed','neighbourhood_group_cleansed','latitude','longitude','property_type','room_type','accommodates','bathrooms','bathrooms_text','bedrooms','beds','amenities','price','minimum_nights','maximum_nights','minimum_minimum_nights','maximum_minimum_nights','minimum_maximum_nights','maximum_maximum_nights','minimum_nights_avg_ntm','maximum_nights_avg_ntm','calendar_updated','has_availability','availability_30','availability_60','availability_90','availability_365','calendar_last_scraped','number_of_reviews','number_of_reviews_ltm','number_of_reviews_l30d','first_review','last_review','review_scores_rating','review_scores_accuracy','review_scores_cleanliness','review_scores_checkin','review_scores_communication','review_scores_location','review_scores_value','license','instant_bookable','calculated_host_listings_count','calculated_host_listings_count_entire_homes','calculated_host_listings_count_private_rooms','calculated_host_listings_count_shared_rooms','reviews_per_month','month','year','host_neighbourhood_cleansed']

        values = insert_df[col_names].to_dict('split')
        values = values['data']
        logging.info(values)

        insert_sql = """
                    INSERT INTO bde_at2.airbnb_raw (id, listing_url,scrape_id, last_scraped, name, description, neighborhood_overview, picture_url, host_id, host_url, host_name, host_since, host_location, host_about, host_response_time, host_response_rate, host_acceptance_rate, host_is_superhost, host_thumbnail_url, host_picture_url, host_neighbourhood, host_listings_count, host_total_listings_count, host_verifications, host_has_profile_pic, host_identity_verified, neighbourhood, neighbourhood_cleansed, neighbourhood_group_cleansed, latitude, longitude, property_type, room_type, accommodates, bathrooms, bathrooms_text, bedrooms, beds, amenities, price, minimum_nights, maximum_nights, minimum_minimum_nights, maximum_minimum_nights, minimum_maximum_nights, maximum_maximum_nights, minimum_nights_avg_ntm, maximum_nights_avg_ntm, calendar_updated, has_availability, availability_30, availability_60, availability_90, availability_365, calendar_last_scraped, number_of_reviews, number_of_reviews_ltm, number_of_reviews_l30d, first_review, last_review, review_scores_rating, review_scores_accuracy, review_scores_cleanliness, review_scores_checkin, review_scores_communication, review_scores_location, review_scores_value, license, instant_bookable, calculated_host_listings_count, calculated_host_listings_count_entire_homes, calculated_host_listings_count_private_rooms, calculated_host_listings_count_shared_rooms, reviews_per_month,month,year,host_neighbourhood_cleansed)
                    VALUES %s
                    """

        result = execute_values(conn_ps.cursor(), insert_sql, values, page_size=len(insert_df))
        conn_ps.commit()
    else:
        None

    return None

#Extract and Load Air BnB data: November 2020

    # Local file to dictionary 
def air_11_2020_func():
    path = "./data/11_2020.csv"
    df = pd.read_csv(path, sep=',',header=0)
    df["month"] = "November"
    df["year"] = 2020

    # Create host_neighbourhood_cleansed column
    
    lga_path = "./data/LGA_Codes_to_Suburbs_NSW.csv"
    lga = pd.read_csv(lga_path, sep=',',header=0)

    df = pd.merge(df, 
                  lga, 
                  on ='host_neighbourhood', 
                  how ='left')

    df.rename(columns={'LGA':'host_neighbourhood_cleansed'}, inplace=True)

    df_dict = df.to_dict()
    
    #Postgress connection         
    ps_pg_hook = PostgresHook(postgres_conn_id="postgres")
    conn_ps = ps_pg_hook.get_conn()
    
    # Dictionary to DF DB Insert
    insert_df_dict = df_dict
    insert_df = pd.DataFrame.from_dict(insert_df_dict)

    if len(insert_df) > 0:
        col_names = ['id','listing_url','scrape_id','last_scraped','name','description','neighborhood_overview','picture_url','host_id','host_url','host_name','host_since','host_location','host_about','host_response_time','host_response_rate','host_acceptance_rate','host_is_superhost','host_thumbnail_url','host_picture_url','host_neighbourhood','host_listings_count','host_total_listings_count','host_verifications','host_has_profile_pic','host_identity_verified','neighbourhood','neighbourhood_cleansed','neighbourhood_group_cleansed','latitude','longitude','property_type','room_type','accommodates','bathrooms','bathrooms_text','bedrooms','beds','amenities','price','minimum_nights','maximum_nights','minimum_minimum_nights','maximum_minimum_nights','minimum_maximum_nights','maximum_maximum_nights','minimum_nights_avg_ntm','maximum_nights_avg_ntm','calendar_updated','has_availability','availability_30','availability_60','availability_90','availability_365','calendar_last_scraped','number_of_reviews','number_of_reviews_ltm','number_of_reviews_l30d','first_review','last_review','review_scores_rating','review_scores_accuracy','review_scores_cleanliness','review_scores_checkin','review_scores_communication','review_scores_location','review_scores_value','license','instant_bookable','calculated_host_listings_count','calculated_host_listings_count_entire_homes','calculated_host_listings_count_private_rooms','calculated_host_listings_count_shared_rooms','reviews_per_month','month','year','host_neighbourhood_cleansed']

        values = insert_df[col_names].to_dict('split')
        values = values['data']
        logging.info(values)

        insert_sql = """
                    INSERT INTO bde_at2.airbnb_raw (id, listing_url,scrape_id, last_scraped, name, description, neighborhood_overview, picture_url, host_id, host_url, host_name, host_since, host_location, host_about, host_response_time, host_response_rate, host_acceptance_rate, host_is_superhost, host_thumbnail_url, host_picture_url, host_neighbourhood, host_listings_count, host_total_listings_count, host_verifications, host_has_profile_pic, host_identity_verified, neighbourhood, neighbourhood_cleansed, neighbourhood_group_cleansed, latitude, longitude, property_type, room_type, accommodates, bathrooms, bathrooms_text, bedrooms, beds, amenities, price, minimum_nights, maximum_nights, minimum_minimum_nights, maximum_minimum_nights, minimum_maximum_nights, maximum_maximum_nights, minimum_nights_avg_ntm, maximum_nights_avg_ntm, calendar_updated, has_availability, availability_30, availability_60, availability_90, availability_365, calendar_last_scraped, number_of_reviews, number_of_reviews_ltm, number_of_reviews_l30d, first_review, last_review, review_scores_rating, review_scores_accuracy, review_scores_cleanliness, review_scores_checkin, review_scores_communication, review_scores_location, review_scores_value, license, instant_bookable, calculated_host_listings_count, calculated_host_listings_count_entire_homes, calculated_host_listings_count_private_rooms, calculated_host_listings_count_shared_rooms, reviews_per_month,month,year,host_neighbourhood_cleansed)
                    VALUES %s
                    """

        result = execute_values(conn_ps.cursor(), insert_sql, values, page_size=len(insert_df))
        conn_ps.commit()
    else:
        None

    return None

#Extract and Load Air BnB data: December 2020

    # Local file to dictionary 
def air_12_2020_func():
    path = "./data/12_2020.csv"
    df = pd.read_csv(path, sep=',',header=0)
    df["month"] = "December"
    df["year"] = 2020

    # Create host_neighbourhood_cleansed column
    
    lga_path = "./data/LGA_Codes_to_Suburbs_NSW.csv"
    lga = pd.read_csv(lga_path, sep=',',header=0)

    df = pd.merge(df, 
                  lga, 
                  on ='host_neighbourhood', 
                  how ='left')

    df.rename(columns={'LGA':'host_neighbourhood_cleansed'}, inplace=True)

    df_dict = df.to_dict()
    
    #Postgress connection         
    ps_pg_hook = PostgresHook(postgres_conn_id="postgres")
    conn_ps = ps_pg_hook.get_conn()
    
    # Dictionary to DF DB Insert
    insert_df_dict = df_dict
    insert_df = pd.DataFrame.from_dict(insert_df_dict)

    if len(insert_df) > 0:
        col_names = ['id','listing_url','scrape_id','last_scraped','name','description','neighborhood_overview','picture_url','host_id','host_url','host_name','host_since','host_location','host_about','host_response_time','host_response_rate','host_acceptance_rate','host_is_superhost','host_thumbnail_url','host_picture_url','host_neighbourhood','host_listings_count','host_total_listings_count','host_verifications','host_has_profile_pic','host_identity_verified','neighbourhood','neighbourhood_cleansed','neighbourhood_group_cleansed','latitude','longitude','property_type','room_type','accommodates','bathrooms','bathrooms_text','bedrooms','beds','amenities','price','minimum_nights','maximum_nights','minimum_minimum_nights','maximum_minimum_nights','minimum_maximum_nights','maximum_maximum_nights','minimum_nights_avg_ntm','maximum_nights_avg_ntm','calendar_updated','has_availability','availability_30','availability_60','availability_90','availability_365','calendar_last_scraped','number_of_reviews','number_of_reviews_ltm','number_of_reviews_l30d','first_review','last_review','review_scores_rating','review_scores_accuracy','review_scores_cleanliness','review_scores_checkin','review_scores_communication','review_scores_location','review_scores_value','license','instant_bookable','calculated_host_listings_count','calculated_host_listings_count_entire_homes','calculated_host_listings_count_private_rooms','calculated_host_listings_count_shared_rooms','reviews_per_month','month','year','host_neighbourhood_cleansed']

        values = insert_df[col_names].to_dict('split')
        values = values['data']
        logging.info(values)

        insert_sql = """
                    INSERT INTO bde_at2.airbnb_raw (id, listing_url,scrape_id, last_scraped, name, description, neighborhood_overview, picture_url, host_id, host_url, host_name, host_since, host_location, host_about, host_response_time, host_response_rate, host_acceptance_rate, host_is_superhost, host_thumbnail_url, host_picture_url, host_neighbourhood, host_listings_count, host_total_listings_count, host_verifications, host_has_profile_pic, host_identity_verified, neighbourhood, neighbourhood_cleansed, neighbourhood_group_cleansed, latitude, longitude, property_type, room_type, accommodates, bathrooms, bathrooms_text, bedrooms, beds, amenities, price, minimum_nights, maximum_nights, minimum_minimum_nights, maximum_minimum_nights, minimum_maximum_nights, maximum_maximum_nights, minimum_nights_avg_ntm, maximum_nights_avg_ntm, calendar_updated, has_availability, availability_30, availability_60, availability_90, availability_365, calendar_last_scraped, number_of_reviews, number_of_reviews_ltm, number_of_reviews_l30d, first_review, last_review, review_scores_rating, review_scores_accuracy, review_scores_cleanliness, review_scores_checkin, review_scores_communication, review_scores_location, review_scores_value, license, instant_bookable, calculated_host_listings_count, calculated_host_listings_count_entire_homes, calculated_host_listings_count_private_rooms, calculated_host_listings_count_shared_rooms, reviews_per_month,month,year,host_neighbourhood_cleansed)
                    VALUES %s
                    """

        result = execute_values(conn_ps.cursor(), insert_sql, values, page_size=len(insert_df))
        conn_ps.commit()
    else:
        None

    return None

#Extract and Load Air BnB data: January 2021

    # Local file to dictionary 
def air_01_2021_func():
    path = "./data/01_2021.csv"
    df = pd.read_csv(path, sep=',',header=0)
    df["month"] = "January"
    df["year"] = 2021

    # Create host_neighbourhood_cleansed column
    
    lga_path = "./data/LGA_Codes_to_Suburbs_NSW.csv"
    lga = pd.read_csv(lga_path, sep=',',header=0)

    df = pd.merge(df, 
                  lga, 
                  on ='host_neighbourhood', 
                  how ='left')

    df.rename(columns={'LGA':'host_neighbourhood_cleansed'}, inplace=True)

    df_dict = df.to_dict()
    
    #Postgress connection         
    ps_pg_hook = PostgresHook(postgres_conn_id="postgres")
    conn_ps = ps_pg_hook.get_conn()
    
    # Dictionary to DF DB Insert
    insert_df_dict = df_dict
    insert_df = pd.DataFrame.from_dict(insert_df_dict)

    if len(insert_df) > 0:
        col_names = ['id','listing_url','scrape_id','last_scraped','name','description','neighborhood_overview','picture_url','host_id','host_url','host_name','host_since','host_location','host_about','host_response_time','host_response_rate','host_acceptance_rate','host_is_superhost','host_thumbnail_url','host_picture_url','host_neighbourhood','host_listings_count','host_total_listings_count','host_verifications','host_has_profile_pic','host_identity_verified','neighbourhood','neighbourhood_cleansed','neighbourhood_group_cleansed','latitude','longitude','property_type','room_type','accommodates','bathrooms','bathrooms_text','bedrooms','beds','amenities','price','minimum_nights','maximum_nights','minimum_minimum_nights','maximum_minimum_nights','minimum_maximum_nights','maximum_maximum_nights','minimum_nights_avg_ntm','maximum_nights_avg_ntm','calendar_updated','has_availability','availability_30','availability_60','availability_90','availability_365','calendar_last_scraped','number_of_reviews','number_of_reviews_ltm','number_of_reviews_l30d','first_review','last_review','review_scores_rating','review_scores_accuracy','review_scores_cleanliness','review_scores_checkin','review_scores_communication','review_scores_location','review_scores_value','license','instant_bookable','calculated_host_listings_count','calculated_host_listings_count_entire_homes','calculated_host_listings_count_private_rooms','calculated_host_listings_count_shared_rooms','reviews_per_month','month','year','host_neighbourhood_cleansed']

        values = insert_df[col_names].to_dict('split')
        values = values['data']
        logging.info(values)

        insert_sql = """
                    INSERT INTO bde_at2.airbnb_raw (id, listing_url,scrape_id, last_scraped, name, description, neighborhood_overview, picture_url, host_id, host_url, host_name, host_since, host_location, host_about, host_response_time, host_response_rate, host_acceptance_rate, host_is_superhost, host_thumbnail_url, host_picture_url, host_neighbourhood, host_listings_count, host_total_listings_count, host_verifications, host_has_profile_pic, host_identity_verified, neighbourhood, neighbourhood_cleansed, neighbourhood_group_cleansed, latitude, longitude, property_type, room_type, accommodates, bathrooms, bathrooms_text, bedrooms, beds, amenities, price, minimum_nights, maximum_nights, minimum_minimum_nights, maximum_minimum_nights, minimum_maximum_nights, maximum_maximum_nights, minimum_nights_avg_ntm, maximum_nights_avg_ntm, calendar_updated, has_availability, availability_30, availability_60, availability_90, availability_365, calendar_last_scraped, number_of_reviews, number_of_reviews_ltm, number_of_reviews_l30d, first_review, last_review, review_scores_rating, review_scores_accuracy, review_scores_cleanliness, review_scores_checkin, review_scores_communication, review_scores_location, review_scores_value, license, instant_bookable, calculated_host_listings_count, calculated_host_listings_count_entire_homes, calculated_host_listings_count_private_rooms, calculated_host_listings_count_shared_rooms, reviews_per_month,month,year,host_neighbourhood_cleansed)
                    VALUES %s
                    """

        result = execute_values(conn_ps.cursor(), insert_sql, values, page_size=len(insert_df))
        conn_ps.commit()
    else:
        None

    return None

#Extract and Load Air BnB data: February 2021

    # Local file to dictionary 
def air_02_2021_func():
    path = "./data/02_2021.csv"
    df = pd.read_csv(path, sep=',',header=0)
    df["month"] = "February"
    df["year"] = 2021

    # Create host_neighbourhood_cleansed column
    
    lga_path = "./data/LGA_Codes_to_Suburbs_NSW.csv"
    lga = pd.read_csv(lga_path, sep=',',header=0)

    df = pd.merge(df, 
                  lga, 
                  on ='host_neighbourhood', 
                  how ='left')

    df.rename(columns={'LGA':'host_neighbourhood_cleansed'}, inplace=True)

    df_dict = df.to_dict()
    
    #Postgress connection         
    ps_pg_hook = PostgresHook(postgres_conn_id="postgres")
    conn_ps = ps_pg_hook.get_conn()
    
    # Dictionary to DF DB Insert
    insert_df_dict = df_dict
    insert_df = pd.DataFrame.from_dict(insert_df_dict)

    if len(insert_df) > 0:
        col_names = ['id','listing_url','scrape_id','last_scraped','name','description','neighborhood_overview','picture_url','host_id','host_url','host_name','host_since','host_location','host_about','host_response_time','host_response_rate','host_acceptance_rate','host_is_superhost','host_thumbnail_url','host_picture_url','host_neighbourhood','host_listings_count','host_total_listings_count','host_verifications','host_has_profile_pic','host_identity_verified','neighbourhood','neighbourhood_cleansed','neighbourhood_group_cleansed','latitude','longitude','property_type','room_type','accommodates','bathrooms','bathrooms_text','bedrooms','beds','amenities','price','minimum_nights','maximum_nights','minimum_minimum_nights','maximum_minimum_nights','minimum_maximum_nights','maximum_maximum_nights','minimum_nights_avg_ntm','maximum_nights_avg_ntm','calendar_updated','has_availability','availability_30','availability_60','availability_90','availability_365','calendar_last_scraped','number_of_reviews','number_of_reviews_ltm','number_of_reviews_l30d','first_review','last_review','review_scores_rating','review_scores_accuracy','review_scores_cleanliness','review_scores_checkin','review_scores_communication','review_scores_location','review_scores_value','license','instant_bookable','calculated_host_listings_count','calculated_host_listings_count_entire_homes','calculated_host_listings_count_private_rooms','calculated_host_listings_count_shared_rooms','reviews_per_month','month','year','host_neighbourhood_cleansed']

        values = insert_df[col_names].to_dict('split')
        values = values['data']
        logging.info(values)

        insert_sql = """
                    INSERT INTO bde_at2.airbnb_raw (id, listing_url,scrape_id, last_scraped, name, description, neighborhood_overview, picture_url, host_id, host_url, host_name, host_since, host_location, host_about, host_response_time, host_response_rate, host_acceptance_rate, host_is_superhost, host_thumbnail_url, host_picture_url, host_neighbourhood, host_listings_count, host_total_listings_count, host_verifications, host_has_profile_pic, host_identity_verified, neighbourhood, neighbourhood_cleansed, neighbourhood_group_cleansed, latitude, longitude, property_type, room_type, accommodates, bathrooms, bathrooms_text, bedrooms, beds, amenities, price, minimum_nights, maximum_nights, minimum_minimum_nights, maximum_minimum_nights, minimum_maximum_nights, maximum_maximum_nights, minimum_nights_avg_ntm, maximum_nights_avg_ntm, calendar_updated, has_availability, availability_30, availability_60, availability_90, availability_365, calendar_last_scraped, number_of_reviews, number_of_reviews_ltm, number_of_reviews_l30d, first_review, last_review, review_scores_rating, review_scores_accuracy, review_scores_cleanliness, review_scores_checkin, review_scores_communication, review_scores_location, review_scores_value, license, instant_bookable, calculated_host_listings_count, calculated_host_listings_count_entire_homes, calculated_host_listings_count_private_rooms, calculated_host_listings_count_shared_rooms, reviews_per_month,month,year,host_neighbourhood_cleansed)
                    VALUES %s
                    """

        result = execute_values(conn_ps.cursor(), insert_sql, values, page_size=len(insert_df))
        conn_ps.commit()
    else:
        None

    return None

#Extract and Load Air BnB data: March 2021

    # Local file to dictionary 
def air_03_2021_func():
    path = "./data/03_2021.csv"
    df = pd.read_csv(path, sep=',',header=0)
    df["month"] = "March"
    df["year"] = 2021

    # Create host_neighbourhood_cleansed column
    
    lga_path = "./data/LGA_Codes_to_Suburbs_NSW.csv"
    lga = pd.read_csv(lga_path, sep=',',header=0)

    df = pd.merge(df, 
                  lga, 
                  on ='host_neighbourhood', 
                  how ='left')

    df.rename(columns={'LGA':'host_neighbourhood_cleansed'}, inplace=True)

    df_dict = df.to_dict()
    
    #Postgress connection         
    ps_pg_hook = PostgresHook(postgres_conn_id="postgres")
    conn_ps = ps_pg_hook.get_conn()
    
    # Dictionary to DF DB Insert
    insert_df_dict = df_dict
    insert_df = pd.DataFrame.from_dict(insert_df_dict)

    if len(insert_df) > 0:
        col_names = ['id','listing_url','scrape_id','last_scraped','name','description','neighborhood_overview','picture_url','host_id','host_url','host_name','host_since','host_location','host_about','host_response_time','host_response_rate','host_acceptance_rate','host_is_superhost','host_thumbnail_url','host_picture_url','host_neighbourhood','host_listings_count','host_total_listings_count','host_verifications','host_has_profile_pic','host_identity_verified','neighbourhood','neighbourhood_cleansed','neighbourhood_group_cleansed','latitude','longitude','property_type','room_type','accommodates','bathrooms','bathrooms_text','bedrooms','beds','amenities','price','minimum_nights','maximum_nights','minimum_minimum_nights','maximum_minimum_nights','minimum_maximum_nights','maximum_maximum_nights','minimum_nights_avg_ntm','maximum_nights_avg_ntm','calendar_updated','has_availability','availability_30','availability_60','availability_90','availability_365','calendar_last_scraped','number_of_reviews','number_of_reviews_ltm','number_of_reviews_l30d','first_review','last_review','review_scores_rating','review_scores_accuracy','review_scores_cleanliness','review_scores_checkin','review_scores_communication','review_scores_location','review_scores_value','license','instant_bookable','calculated_host_listings_count','calculated_host_listings_count_entire_homes','calculated_host_listings_count_private_rooms','calculated_host_listings_count_shared_rooms','reviews_per_month','month','year','host_neighbourhood_cleansed']

        values = insert_df[col_names].to_dict('split')
        values = values['data']
        logging.info(values)

        insert_sql = """
                    INSERT INTO bde_at2.airbnb_raw (id, listing_url,scrape_id, last_scraped, name, description, neighborhood_overview, picture_url, host_id, host_url, host_name, host_since, host_location, host_about, host_response_time, host_response_rate, host_acceptance_rate, host_is_superhost, host_thumbnail_url, host_picture_url, host_neighbourhood, host_listings_count, host_total_listings_count, host_verifications, host_has_profile_pic, host_identity_verified, neighbourhood, neighbourhood_cleansed, neighbourhood_group_cleansed, latitude, longitude, property_type, room_type, accommodates, bathrooms, bathrooms_text, bedrooms, beds, amenities, price, minimum_nights, maximum_nights, minimum_minimum_nights, maximum_minimum_nights, minimum_maximum_nights, maximum_maximum_nights, minimum_nights_avg_ntm, maximum_nights_avg_ntm, calendar_updated, has_availability, availability_30, availability_60, availability_90, availability_365, calendar_last_scraped, number_of_reviews, number_of_reviews_ltm, number_of_reviews_l30d, first_review, last_review, review_scores_rating, review_scores_accuracy, review_scores_cleanliness, review_scores_checkin, review_scores_communication, review_scores_location, review_scores_value, license, instant_bookable, calculated_host_listings_count, calculated_host_listings_count_entire_homes, calculated_host_listings_count_private_rooms, calculated_host_listings_count_shared_rooms, reviews_per_month,month,year,host_neighbourhood_cleansed)
                    VALUES %s
                    """

        result = execute_values(conn_ps.cursor(), insert_sql, values, page_size=len(insert_df))
        conn_ps.commit()
    else:
        None

    return None

#Extract and Load Air BnB data: April 2021
    # Local file to dictionary 
def air_04_2021_func():
    path = "./data/04_2021.csv"
    df = pd.read_csv(path, sep=',',header=0)
    df["month"] = "April"
    df["year"] = 2021

    # Create host_neighbourhood_cleansed column
    
    lga_path = "./data/LGA_Codes_to_Suburbs_NSW.csv"
    lga = pd.read_csv(lga_path, sep=',',header=0)

    df = pd.merge(df, 
                  lga, 
                  on ='host_neighbourhood', 
                  how ='left')

    df.rename(columns={'LGA':'host_neighbourhood_cleansed'}, inplace=True)

    df_dict = df.to_dict()
    
    #Postgress connection         
    ps_pg_hook = PostgresHook(postgres_conn_id="postgres")
    conn_ps = ps_pg_hook.get_conn()
    
    # Dictionary to DF DB Insert
    insert_df_dict = df_dict
    insert_df = pd.DataFrame.from_dict(insert_df_dict)

    if len(insert_df) > 0:
        col_names = ['id','listing_url','scrape_id','last_scraped','name','description','neighborhood_overview','picture_url','host_id','host_url','host_name','host_since','host_location','host_about','host_response_time','host_response_rate','host_acceptance_rate','host_is_superhost','host_thumbnail_url','host_picture_url','host_neighbourhood','host_listings_count','host_total_listings_count','host_verifications','host_has_profile_pic','host_identity_verified','neighbourhood','neighbourhood_cleansed','neighbourhood_group_cleansed','latitude','longitude','property_type','room_type','accommodates','bathrooms','bathrooms_text','bedrooms','beds','amenities','price','minimum_nights','maximum_nights','minimum_minimum_nights','maximum_minimum_nights','minimum_maximum_nights','maximum_maximum_nights','minimum_nights_avg_ntm','maximum_nights_avg_ntm','calendar_updated','has_availability','availability_30','availability_60','availability_90','availability_365','calendar_last_scraped','number_of_reviews','number_of_reviews_ltm','number_of_reviews_l30d','first_review','last_review','review_scores_rating','review_scores_accuracy','review_scores_cleanliness','review_scores_checkin','review_scores_communication','review_scores_location','review_scores_value','license','instant_bookable','calculated_host_listings_count','calculated_host_listings_count_entire_homes','calculated_host_listings_count_private_rooms','calculated_host_listings_count_shared_rooms','reviews_per_month','month','year','host_neighbourhood_cleansed']

        values = insert_df[col_names].to_dict('split')
        values = values['data']
        logging.info(values)

        insert_sql = """
                    INSERT INTO bde_at2.airbnb_raw (id, listing_url,scrape_id, last_scraped, name, description, neighborhood_overview, picture_url, host_id, host_url, host_name, host_since, host_location, host_about, host_response_time, host_response_rate, host_acceptance_rate, host_is_superhost, host_thumbnail_url, host_picture_url, host_neighbourhood, host_listings_count, host_total_listings_count, host_verifications, host_has_profile_pic, host_identity_verified, neighbourhood, neighbourhood_cleansed, neighbourhood_group_cleansed, latitude, longitude, property_type, room_type, accommodates, bathrooms, bathrooms_text, bedrooms, beds, amenities, price, minimum_nights, maximum_nights, minimum_minimum_nights, maximum_minimum_nights, minimum_maximum_nights, maximum_maximum_nights, minimum_nights_avg_ntm, maximum_nights_avg_ntm, calendar_updated, has_availability, availability_30, availability_60, availability_90, availability_365, calendar_last_scraped, number_of_reviews, number_of_reviews_ltm, number_of_reviews_l30d, first_review, last_review, review_scores_rating, review_scores_accuracy, review_scores_cleanliness, review_scores_checkin, review_scores_communication, review_scores_location, review_scores_value, license, instant_bookable, calculated_host_listings_count, calculated_host_listings_count_entire_homes, calculated_host_listings_count_private_rooms, calculated_host_listings_count_shared_rooms, reviews_per_month,month,year,host_neighbourhood_cleansed)
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

air_05_2020_task = PythonOperator(
    task_id="air_05_2020_task_id",
    python_callable=air_05_2020_func,
    op_kwargs={},
    provide_context=True,
    dag=dag
)

air_06_2020_task = PythonOperator(
    task_id="air_06_2020_task_id",
    python_callable=air_06_2020_func,
    op_kwargs={},
    provide_context=True,
    dag=dag
)

air_07_2020_task = PythonOperator(
    task_id="air_07_2020_task_id",
    python_callable=air_07_2020_func,
    op_kwargs={},
    provide_context=True,
    dag=dag
)

air_08_2020_task = PythonOperator(
    task_id="air_08_2020_task_id",
    python_callable=air_08_2020_func,
    op_kwargs={},
    provide_context=True,
    dag=dag
)

air_09_2020_task = PythonOperator(
    task_id="air_09_2020_task_id",
    python_callable=air_09_2020_func,
    op_kwargs={},
    provide_context=True,
    dag=dag
)

air_10_2020_task = PythonOperator(
    task_id="air_10_2020_task_id",
    python_callable=air_10_2020_func,
    op_kwargs={},
    provide_context=True,
    dag=dag
)

air_11_2020_task = PythonOperator(
    task_id="air_11_2020_task_id",
    python_callable=air_11_2020_func,
    op_kwargs={},
    provide_context=True,
    dag=dag
)

air_12_2020_task = PythonOperator(
    task_id="air_12_2020_task_id",
    python_callable=air_12_2020_func,
    op_kwargs={},
    provide_context=True,
    dag=dag
)

air_01_2021_task = PythonOperator(
    task_id="air_01_2021_task_id",
    python_callable=air_01_2021_func,
    op_kwargs={},
    provide_context=True,
    dag=dag
)

air_02_2021_task = PythonOperator(
    task_id="air_02_2021_task_id",
    python_callable=air_02_2021_func,
    op_kwargs={},
    provide_context=True,
    dag=dag
)

air_03_2021_task = PythonOperator(
    task_id="air_03_2021_task_id",
    python_callable=air_03_2021_func,
    op_kwargs={},
    provide_context=True,
    dag=dag
)

air_04_2021_task = PythonOperator(
    task_id="air_04_2021_task_id",
    python_callable=air_04_2021_func,
    op_kwargs={},
    provide_context=True,
    dag=dag
)

create_psql_schema_task = PostgresOperator(
    task_id="create_psql_schema_task_id",
    postgres_conn_id="postgres",
    sql="""
        CREATE SCHEMA IF NOT EXISTS bde_at2;
    """,
    dag=dag
)

create_airbnb_table_task = PostgresOperator(
    task_id="create_airbnb_table_task_id",
    postgres_conn_id="postgres",
    sql="""
        CREATE TABLE IF NOT EXISTS bde_at2.airbnb_raw (
            id                                              TEXT,
            listing_url                                     TEXT,
            scrape_id                                       TEXT,
            last_scraped                                    TEXT,
            name                                            TEXT,
            description                                     TEXT,
            neighborhood_overview                           TEXT,
            picture_url                                     TEXT,
            host_id                                         TEXT,
            host_url                                        TEXT,
            host_name                                       TEXT,
            host_since                                      TEXT,
            host_location                                   TEXT,
            host_about                                      TEXT,
            host_response_time                              TEXT,
            host_response_rate                              TEXT,
            host_acceptance_rate                            TEXT,
            host_is_superhost                               TEXT,
            host_thumbnail_url                              TEXT,
            host_picture_url                                TEXT,
            host_neighbourhood                              TEXT,
            host_listings_count                             TEXT,
            host_total_listings_count                       TEXT,
            host_verifications                              TEXT,
            host_has_profile_pic                            TEXT,
            host_identity_verified                          TEXT,
            neighbourhood                                   TEXT,
            neighbourhood_cleansed                          TEXT,
            neighbourhood_group_cleansed                    TEXT,
            latitude                                        TEXT,
            longitude                                       TEXT,
            property_type                                   TEXT,
            room_type                                       TEXT,
            accommodates                                    TEXT,
            bathrooms                                       TEXT,
            bathrooms_text                                  TEXT,
            bedrooms                                        TEXT,
            beds                                            TEXT,
            amenities                                       TEXT,
            price                                           TEXT,
            minimum_nights                                  TEXT,
            maximum_nights                                  TEXT,
            minimum_minimum_nights                          TEXT,
            maximum_minimum_nights                          TEXT,
            minimum_maximum_nights                          TEXT,
            maximum_maximum_nights                          TEXT,
            minimum_nights_avg_ntm                          TEXT,
            maximum_nights_avg_ntm                          TEXT,
            calendar_updated                                TEXT,
            has_availability                                TEXT,
            availability_30                                 TEXT,
            availability_60                                 TEXT,
            availability_90                                 TEXT,
            availability_365                                TEXT,
            calendar_last_scraped                           TEXT,
            number_of_reviews                               TEXT,
            number_of_reviews_ltm                           TEXT,
            number_of_reviews_l30d                          TEXT,
            first_review                                    TEXT,
            last_review                                     TEXT,
            review_scores_rating                            TEXT,
            review_scores_accuracy                          TEXT,
            review_scores_cleanliness                       TEXT,
            review_scores_checkin                           TEXT,
            review_scores_communication                     TEXT,
            review_scores_location                          TEXT,
            review_scores_value                             TEXT,
            license                                         TEXT,
            instant_bookable                                TEXT,
            calculated_host_listings_count                  TEXT,
            calculated_host_listings_count_entire_homes     TEXT,
            calculated_host_listings_count_private_rooms    TEXT,
            calculated_host_listings_count_shared_rooms     TEXT,
            reviews_per_month                               TEXT,
            month                                           TEXT,
            year                                            TEXT,
            host_neighbourhood_cleansed						TEXT
            );
    """,
    dag=dag
)


create_psql_schema_task >> create_airbnb_table_task >> air_05_2020_task >> air_06_2020_task >> air_07_2020_task >> air_08_2020_task >> air_09_2020_task >> air_10_2020_task >> air_11_2020_task >> air_12_2020_task >> air_01_2021_task >> air_02_2021_task >> air_03_2021_task >> air_04_2021_task