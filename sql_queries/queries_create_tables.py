import configparser

config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES
staging_business_table_drop = "DROP TABLE IF EXISTS staging_business"
staging_review_table_drop = "DROP TABLE IF EXISTS staging_review"
staging_tip_table_drop = "DROP TABLE IF EXISTS staging_tip"
staging_user_table_drop = "DROP TABLE IF EXISTS staging_user"
dim_users_table_drop = "DROP TABLE IF EXISTS dim_users"
dim_location_table_drop = "DROP TABLE IF EXISTS dim_location"
dim_business_table_drop = "DROP TABLE IF EXISTS dim_business"
dim_datetime_table_drop = "DROP TABLE IF EXISTS dim_datetime"
fact_tip_table_drop = "DROP TABLE IF EXISTS fact_tip"
fact_review_table_drop = "DROP TABLE IF EXISTS fact_review"

# CREATE TABLES
staging_tip_table_create= ("""
                            CREATE TABLE IF NOT EXISTS staging_tip (
                            user_id VARCHAR(256),
                            business_id VARCHAR(256),
                            text VARCHAR(65535),
                            date TIMESTAMP,
                            compliment_count INTEGER )
                            """)
            
staging_user_table_create= ("""
                            CREATE TABLE IF NOT EXISTS staging_user (
                            user_id VARCHAR(256),
                            name VARCHAR(256),
                            review_count INTEGER,
                            average_stars REAL,
                            yelping_since TIMESTAMP )
                            """)

staging_review_table_create= ("""
                            CREATE TABLE IF NOT EXISTS staging_review (
                            review_id VARCHAR(256),
                            user_id VARCHAR(256),
                            business_id VARCHAR(256),
                            stars INTEGER,
                            useful INTEGER ,
                            funny INTEGER,
                            cool INTEGER,
                            text VARCHAR(65535),
                            date TIMESTAMP )
                            """)

staging_business_table_create= ("""
                            CREATE TABLE IF NOT EXISTS staging_business (
                            business_id VARCHAR(256),
                            name VARCHAR(256),
                            address VARCHAR(256),
                            city VARCHAR(256),
                            state VARCHAR(32) ,
                            postal_code VARCHAR(32),
                            latitude REAL,
                            longitude REAL,
                            stars REAL,
                            review_count INTEGER,
                            is_open INTEGER,
                            ByAppointmentOnly VARCHAR(256),
                            RestaurantsTakeOut VARCHAR(256),
                            Alcohol VARCHAR(256),
                            WiFi VARCHAR(256),
                            BusinessAcceptsBitcoin VARCHAR(256) )
                            """)

dim_users_table_create = ("""
                   CREATE TABLE IF NOT EXISTS dim_users (
                        dim_user_id VARCHAR(256),
                        name VARCHAR(256),
                        yelping_since TIMESTAMP,
                        week_since_active INTEGER,
                        PRIMARY KEY (dim_user_id) )
                    """)

dim_location_table_create = ("""
                   CREATE TABLE IF NOT EXISTS dim_location (
                    dim_location_id VARCHAR(256) NOT NULL,
                    postal_code VARCHAR(128) NOT NULL,
                    city VARCHAR(128),
                    state VARCHAR(128) ,
                    PRIMARY KEY (dim_location_id) )
                    """)

dim_business_table_create = ("""
                   CREATE TABLE IF NOT EXISTS dim_business (
                    dim_business_id VARCHAR(256) NOT NULL,
                    name VARCHAR(256) NOT NULL,
                    is_open INTEGER,
                    address VARCHAR(1024),
                    ByAppointmentOnly VARCHAR(256),
                    RestaurantsTakeOut VARCHAR(256),
                    Alcohol VARCHAR(256),
                    WiFi VARCHAR(256),
                    BusinessAcceptsBitcoin VARCHAR(256),
                    PRIMARY KEY (dim_business_id) )
                    """)

dim_datetime_table_create = ("""
                    CREATE TABLE IF NOT EXISTS dim_datetime (
                        dim_datetime TIMESTAMP NOT NULL,
                        hour INTEGER NOT NULL,
                        minute INTEGER NOT NULL,
                        day INTEGER NOT NULL,
                        month INTEGER NOT NULL,
                        year INTEGER NOT NULL,
                        quarter INTEGER NOT NULL,
                        weekday INTEGER NOT NULL,
                        yearday INTEGER NOT NULL,
                        PRIMARY KEY (dim_datetime) )
                    """)

fact_tip_table_create = ("""
                    CREATE TABLE IF NOT EXISTS fact_tip (
                    fact_tip_id VARCHAR(32) NOT NULL,
                    fact_user_id VARCHAR(32) NOT NULL,
                    fact_business_id VARCHAR(32) NOT NULL,
                    fact_location_id VARCHAR(32),
                    text VARCHAR(65535),
                    fact_datetime TIMESTAMP,
                    compliment_count INTEGER,
                    PRIMARY KEY (fact_tip_id) )
                    """)

fact_review_table_create = ("""
                    CREATE TABLE IF NOT EXISTS fact_review (
                    fact_review_id VARCHAR(256) NOT NULL,
                    fact_user_id VARCHAR(256) NOT NULL,
                    fact_business_id VARCHAR(256) NOT NULL,
                    fact_location_id VARCHAR(256) NOT NULL,
                    stars DOUBLE PRECISION,
                    useful INTEGER,
                    funny INTEGER,
                    cool INTEGER,
                    text VARCHAR(65535),
                    fact_datetime TIMESTAMP,
                    PRIMARY KEY (fact_review_id) )
                    """)
 
# QUERY LISTS
drop_table_queries = [staging_tip_table_drop,staging_user_table_drop,staging_review_table_drop,
                      staging_business_table_drop,dim_users_table_drop, dim_location_table_drop, 
                      dim_business_table_drop, dim_datetime_table_drop, fact_tip_table_drop,
                      fact_review_table_drop]

create_table_queries = [staging_tip_table_create, staging_user_table_create, staging_review_table_create,
                        staging_business_table_create, dim_users_table_create, dim_location_table_create,
                        dim_business_table_create, dim_datetime_table_create, fact_tip_table_create,
                        fact_review_table_create]
