import configparser
config = configparser.ConfigParser()
config.read('dwh.cfg')

# STAGING TABLES
staging_tip_copy = ("""
                    copy staging_tip from {}
                    iam_role {}
                    json {};
                    """).format(config['S3']['TIP_DATA'],\
                                config['IAM_ROLE']['ARN_S3_REDSHIFT'],\
                                config['S3']['TIP_JSONPATH'])

staging_user_copy = ("""
                    copy staging_user from {}
                    iam_role {}
                    json {};
                    """).format(config['S3']['USER_DATA'],\
                                config['IAM_ROLE']['ARN_S3_REDSHIFT'],\
                                config['S3']['USER_JSONPATH'])

staging_review_copy = ("""
                    copy staging_review from {}
                    iam_role {}
                    json {};
                    """).format(config['S3']['REVIEW_DATA'],\
                                config['IAM_ROLE']['ARN_S3_REDSHIFT'],\
                                config['S3']['REVIEW_JSONPATH'])
            
staging_business_copy = ("""
                    copy staging_business from {}
                    iam_role {}
                    json {};
                    """).format(config['S3']['BUSINESS_DATA'],\
                                config['IAM_ROLE']['ARN_S3_REDSHIFT'],\
                                config['S3']['BUSINESS_JSONPATH'])

# DIMENTION TABLES
dim_users_table_insert = ("""
                        INSERT INTO dim_users(
                            dim_user_id,
                            name,
                            yelping_since,
                            week_since_active)
                        SELECT
                            user_id,
                            name,
                            yelping_since,
                            datediff(week, yelping_since, current_date)
                        FROM staging_user
                        WHERE user_id IS NOT NULL
                        """)

dim_location_table_insert = ("""
                        INSERT INTO dim_location(
                            dim_location_id,
                            postal_code,
                            city,
                            state)
                        SELECT 
                            DISTINCT md5(postal_code || city || state) as dim_location_id,
                            postal_code,  
                            city,
                            state 
                        FROM staging_business
                        WHERE postal_code IS NOT NULL
                        """)

dim_business_table_insert = ("""
                        INSERT INTO dim_business(
                            dim_business_id,
                            name, 
                            is_open,
                            address,
                            ByAppointmentOnly,
                            RestaurantsTakeOut,
                            Alcohol,
                            WiFi,
                            BusinessAcceptsBitcoin)
                        SELECT 
                            business_id as dim_business_id,
                            name,
                            is_open,
                            address,
                            ByAppointmentOnly,
                            RestaurantsTakeOut,
                            Alcohol,
                            WiFi,
                            BusinessAcceptsBitcoin
                        FROM staging_business
                        """)

dim_datetime_table_insert = ("""
                        INSERT INTO dim_datetime(
                            dim_datetime,
                            hour,
                            minute,
                            day,
                            month,
                            year,
                            quarter,
                            weekday,
                            yearday)
                        SELECT 
                            a.datetime as dim_datetime,
                            extract(hour from a.datetime) as hour,
                            extract(minute from a.datetime) as minute,
                            extract(day from a.datetime) as day,
                            extract(month from a.datetime) as month,
                            extract(year from a.datetime) as year,
                            extract(qtr from a.datetime) as quarter,
                            extract(weekday from a.datetime) as weekday,
                            extract(yearday from a.datetime) as yearday
                        FROM (
                            SELECT yelping_since as datetime
                            FROM staging_user
                            GROUP BY yelping_since
                            UNION
                            SELECT date as datetime
                            FROM staging_review
                            GROUP BY date
                            UNION
                            SELECT date as datetime
                            FROM staging_tip
                            GROUP BY date
                        ) a
                        WHERE a.datetime is not null
                        """)

# FACT TABLES
fact_tip_table_insert = ("""
                        INSERT INTO fact_tip(
                            fact_tip_id,
                            fact_user_id,
                            fact_business_id,
                            fact_location_id,
                            text,
                            fact_datetime,
                            compliment_count)
                        SELECT 
                            DISTINCT md5(user_id || business_id || date) as fact_tip_id,
                            T.user_id AS fact_user_id,
                            T.business_id as fact_business_id,
                            L.dim_location_id as fact_location_id,
                            T.text,
                            T.date as fact_datetime,
                            T.compliment_count
                        FROM staging_tip AS T
                        INNER JOIN staging_business AS B 
                            ON T.business_id = B.business_id
                        INNER JOIN dim_location AS L 
                            ON B.postal_code = L.postal_code AND B.city = L.city AND B.state = L.state
                        """)

fact_review_table_insert = ("""
                        INSERT INTO fact_review(
                            fact_review_id,
                            fact_user_id,
                            fact_business_id,
                            fact_location_id,
                            stars,
                            useful,
                            funny,
                            cool,
                            text,
                            fact_datetime)
                        SELECT 
                            R.review_id AS fact_review_id,
                            R.user_id AS fact_user_id,
                            R.business_id AS fact_business_id,
                            L.dim_location_id AS fact_location_id,
                            R.stars,
                            R.useful,
                            R.funny,
                            R.cool,
                            R.text,
                            date AS fact_datetime
                        FROM staging_review AS R
                        INNER JOIN staging_business AS B 
                        ON R.business_id = B.business_id
                        INNER JOIN dim_location AS L 
                        ON B.postal_code = L.postal_code AND B.city = L.city AND B.state = L.state
                        """)

# TEST
fact_review_table_test = "SELECT * FROM fact_review LIMIT 5"
fact_tip_table_test = "SELECT * FROM fact_tip LIMIT 5"
dim_datetime_table_test = "SELECT * FROM dim_datetime LIMIT 5"
dim_business_table_test = "SELECT * FROM dim_business LIMIT 5"
dim_location_table_test = "SELECT * FROM dim_location LIMIT 5"
dim_users_table_test = "SELECT * FROM dim_users LIMIT 5"

# QUERY LISTS
copy_table_queries = [staging_tip_copy, staging_user_copy, staging_review_copy, staging_business_copy]
insert_dim_queries = [dim_users_table_insert, dim_location_table_insert, dim_business_table_insert, dim_datetime_table_insert]
insert_fact_queries = [fact_review_table_insert , fact_tip_table_insert]
test_queries = [fact_review_table_test, fact_tip_table_test, dim_datetime_table_test,
                dim_business_table_test, dim_location_table_test,dim_users_table_test]
