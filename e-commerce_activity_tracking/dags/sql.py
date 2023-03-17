
create_orders_staging = ("""
    DROP TABLE IF EXISTS judendu4707_staging.orders_staging;
    CREATE TABLE judendu4707_staging.orders_staging(
	order_id INT NOT NULL GENERATED ALWAYS AS IDENTITY, 
	customer_id INT NOT NULL, 
	order_date DATE NOT NULL, 
	product_id INT NOT NULL,
	unit_price INT NOT NULL, 
	quantity INT NOT NULL, 
	total_price INT NOT NULL,
    PRIMARY KEY(order_id)
    );

""")



create_reviews_staging = ("""
    DROP TABLE IF EXISTS judendu4707_staging.reviews_staging;
    CREATE TABLE judendu4707_staging.reviews_staging(
    review INT, 
	product_id INT NOT NULL
    );

""")

create_shipment_delivery_staging = ("""
    DROP TABLE IF EXISTS judendu4707_staging.shipment_del_staging;
    CREATE TABLE judendu4707_staging.shipment_del_staging(
    shipment_id INT NOT NULL PRIMARY KEY, 
	order_id INT NOT NULL, 
	shipment_date DATE NULL, 
	delivery_date DATE NULL
    );

""")

create_agg_order_public_holiday_table = ("""
    DROP TABLE IF EXISTS judendu4707_analytics.agg_public_holiday;
    CREATE TABLE judendu4707_analytics.agg_public_holiday(
    ingestion_date DATE NOT NULL DEFAULT CURRENT_DATE,
    tt_order_hol_jan INT NOT NULL,
    tt_order_hol_feb INT NOT NULL,
    tt_order_hol_mar INT NOT NULL,
    tt_order_hol_apr INT NOT NULL,
    tt_order_hol_may INT NOT NULL,
    tt_order_hol_jun INT NOT NULL,
    tt_order_hol_jul INT NOT NULL,
    tt_order_hol_aug INT NOT NULL,
    tt_order_hol_sep INT NOT NULL,
    tt_order_hol_oct INT NOT NULL,
    tt_order_hol_nov INT NOT NULL,
    tt_order_hol_dec INT NOT NULL,
    PRIMARY KEY(ingestion_date));

""")


create_agg_shipment_table = ("""
    DROP TABLE IF EXISTS judendu4707_analytics.agg_shipments;
    CREATE TABLE judendu4707_analytics.agg_shipments(
    ingestion_date DATE NOT NULL DEFAULT CURRENT_DATE,
    tt_late_shipments INT NOT NULL,
    tt_undelivered_items INT NOT NULL,
    PRIMARY KEY(ingestion_date));

""")

create_best_performing_product_table = ("""
    DROP TABLE IF EXISTS judendu4707_analytics.best_performing_product;
    CREATE TABLE judendu4707_analytics.best_performing_product(
    ingestion_date DATE NOT NULL DEFAULT CURRENT_DATE,
    product_name VARCHAR(255) NULL,
    most_ordered_day DATE NULL,
    is_public_holiday boolean NULL,
    tt_review_points INT NOT NULL,
    pct_one_star_review FLOAT NOT NULL,
    pct_two_star_review FLOAT NOT NULL,
    pct_three_star_review FLOAT NOT NULL,
    pct_four_star_review FLOAT NOT NULL,
    pct_five_star_review FLOAT NOT NULL,
    pct_early_shipments FLOAT NOT NULL,
    pct_late_shipments FLOAT NOT NULL,
    PRIMARY KEY(ingestion_date));

""")


drop_staging_tables = ("""
    DROP TABLE IF EXISTS judendu4707_staging.reviews_staging;
    DROP TABLE IF EXISTS judendu4707_staging.orders_staging;
    DROP TABLE IF EXISTS judendu4707_staging.shipment_del_staging;

""")


load_agg_order_public_holiday_table = ("""
    INSERT INTO judendu4707_analytics.agg_public_holiday(tt_order_hol_jan, 
                                                         tt_order_hol_feb, 
                                                         tt_order_hol_mar, 
                                                         tt_order_hol_apr, 
                                                         tt_order_hol_may, 
                                                         tt_order_hol_jun, 
                                                         tt_order_hol_jul,
                                                         tt_order_hol_aug,
                                                         tt_order_hol_sep,
                                                         tt_order_hol_oct,
                                                         tt_order_hol_nov,
                                                         tt_order_hol_dec)
    WITH public_hol_cte AS 
    (
        SELECT ot.order_date, ot.order_id, dd.calendar_dt, dd.month_of_the_year_num,
            dd.day_of_the_week_num, dd.working_day                                                                     
        FROM judendu4707_staging.orders_staging ot
        JOIN
        if_common.dim_dates AS dd
        ON dd.calendar_dt = ot.order_date 
    )

    SELECT
        SUM(CASE
               WHEN (ph.day_of_the_week_num >= 1 AND ph.day_of_the_week_num <= 5) 
               AND (ph.month_of_the_year_num = 1 AND ph.working_day = false) THEN 1
               ELSE 0
	      END) AS "tt_order_hol_jan",

        SUM(CASE
                WHEN (ph.day_of_the_week_num >= 1 AND ph.day_of_the_week_num <= 5)
                AND (ph.month_of_the_year_num = 2 AND ph.working_day=false) THEN 1
                ELSE 0
            END) AS "tt_order_hol_feb",
        SUM(CASE
                WHEN (ph.day_of_the_week_num >= 1 AND ph.day_of_the_week_num <= 5)
                AND (ph.month_of_the_year_num = 3 AND ph.working_day=false) THEN 1
                ELSE 0 
          END) AS "tt_order_hol_mar",
        SUM(CASE
                WHEN (ph.day_of_the_week_num >= 1 AND ph.day_of_the_week_num <= 5)
                AND (ph.month_of_the_year_num = 4 AND ph.working_day=false) THEN 1
                ELSE 0 
            END) AS "tt_order_hol_apr",
        SUM(CASE
                WHEN (ph.day_of_the_week_num >= 1 AND ph.day_of_the_week_num <= 5)
                AND (ph.month_of_the_year_num = 5 AND ph.working_day=false) THEN 1
                ELSE 0 
            END) AS "tt_order_hol_may",
        SUM (CASE
                WHEN (ph.day_of_the_week_num >= 1 AND ph.day_of_the_week_num <= 5)
                AND (ph.month_of_the_year_num = 6 AND ph.working_day=false) THEN 1
                ELSE 0
            END) AS "tt_order_hol_jun",
        SUM (CASE
                WHEN (ph.day_of_the_week_num >= 1 AND ph.day_of_the_week_num <= 5)
                AND (ph.month_of_the_year_num = 7 AND ph.working_day=false) THEN 1
                ELSE 0 
            END) AS "tt_order_hol_jul",
        SUM (CASE
                WHEN (ph.day_of_the_week_num >= 1 AND ph.day_of_the_week_num <= 5)
                AND (ph.month_of_the_year_num = 8 AND ph.working_day=false) THEN 1
                ELSE 0 
            END) AS "tt_order_hol_aug",
        SUM (CASE
                WHEN (ph.day_of_the_week_num >= 1 AND ph.day_of_the_week_num <= 5)
                AND (ph.month_of_the_year_num = 9 AND ph.working_day=false) THEN 1
                ELSE 0
            END) AS "tt_order_hol_sep",
        SUM (CASE
                WHEN (ph.day_of_the_week_num >= 1 AND ph.day_of_the_week_num <= 5)
                AND (ph.month_of_the_year_num = 10 AND ph.working_day=false) THEN 1
                ELSE 0
            END) AS "tt_order_hol_oct",
        SUM (CASE 
                WHEN (ph.day_of_the_week_num >= 1 AND ph.day_of_the_week_num <= 5)
                AND (ph.month_of_the_year_num = 11 AND ph.working_day=false) THEN 1
                ELSE 0 
            END) AS "tt_order_hol_nov",
        SUM (CASE
                WHEN (ph.day_of_the_week_num >= 1 AND ph.day_of_the_week_num <= 5)
                AND (ph.month_of_the_year_num = 12 AND ph.working_day=false) THEN 1
                ELSE 0 
            END) AS "tt_order_hol_dec"

    FROM public_hol_cte AS ph;
""")


load_agg_shipment_table = ("""


    WITH dates_cte AS 
     (
     SELECT sd.order_id, sd.shipment_date, sd.delivery_date, ot.order_id, ot.order_date,
 		(ot.order_date + 6) AS order_deadline,
 		('2022-09-05'::date + 15) AS delivery_dl
	FROM judendu4707_staging.shipment_del_staging AS sd
	JOIN judendu4707_staging.orders_staging ot
	ON ot.order_id=sd.order_id
     )
	INSERT INTO judendu4707_analytics.agg_shipments(tt_late_shipments, tt_undelivered_items) 
    SELECT 
        SUM(CASE WHEN sh.shipment_date >= sh.order_deadline AND sh.delivery_date IS NULL THEN 1 ELSE 0 END) as "tt_late_shipments",
		SUM(CASE WHEN sh.shipment_date > sh.delivery_dl 
			AND (sh.delivery_date IS NULL AND sh.shipment_date IS NULL) THEN 1 ELSE 0 END) as "tt_undelivered_items" 
	FROM dates_cte as sh;  
""")
    

load_best_performing_product = """
    INSERT INTO judendu4707_analytics.best_performing_product(tt_review_points, pct_one_star_review, pct_two_star_review, 
                                                               pct_three_star_review, pct_four_star_review, 
                                                               pct_five_star_review, pct_early_shipments, pct_late_shipments)

    WITH transform_cte AS 
        (
            SELECT dd.calendar_dt, sd.shipment_date, sd.delivery_date, sd.order_id, ot.order_id,
				   dd.day_of_the_week_num as most_ordered_day, 
				   (ot.order_date + 6) AS deadline_date,
				   dd.working_day as is_public_holiday, dp.product_id, dp.product_name,
				   rt.review, rt.product_id, ot.order_date, ot.product_id
				FROM  judendu4707_staging.orders_staging as ot
				JOIN if_common.dim_dates dd ON ot.order_date = dd.calendar_dt
				JOIN judendu4707_staging.shipment_del_staging sd ON ot.order_id=sd.order_id
				JOIN if_common.dim_products dp ON dp.product_id = ot.product_id
				JOIN judendu4707_staging.reviews_staging rt ON rt.product_id = dp.product_id
          )	  
            
    (SELECT 
            
            ROUND(
            100.0 * (
                SUM(CASE WHEN tc.review = 1 THEN 1 ELSE 0 END)::DECIMAL / COUNT(tc.review)
                ), 2) pct_one_star_review,

            ROUND(
            100.0 * (
                SUM(CASE WHEN tc.review = 2 THEN 1 ELSE 0 END)::DECIMAL / COUNT(tc.review)
                ), 2) pct_two_star_review,

            ROUND(
            100.0 * (
                SUM(CASE WHEN tc.review = 3 THEN 1 ELSE 0 END)::DECIMAL / COUNT(tc.review)
                ), 2) pct_three_star_review,

            ROUND(
            100.0 * (
                SUM(CASE WHEN tc.review = 4 THEN 1 ELSE 0 END)::DECIMAL / COUNT(tc.review)
                ), 2) pct_four_star_review,

            ROUND(
            100.0 * (
                SUM(CASE WHEN tc.review = 5 THEN 1 ELSE 0 END)::DECIMAL / COUNT(tc.review)
                ), 2) pct_five_star_review,
                
            SUM(DISTINCT tc.review) as tt_review_points,
            
            ROUND(
                100.0 * (
                    SUM(CASE WHEN tc.shipment_date >= tc.deadline_date AND tc.delivery_date IS NULL THEN 1 ELSE 0 END)::DECIMAL / COUNT(tc.shipment_date)
                    ), 2) pct_late_shipments,
            ROUND(
                100.0 * (
                    SUM(CASE WHEN tc.shipment_date <= tc.deadline_date AND tc.delivery_date IS NOT NULL THEN 1 ELSE 0 END)::DECIMAL / COUNT(tc.shipment_date)
                    ), 2) pct_early_shipments  
    
    FROM transform_cte as tc);
 
"""

## this part queries the most reviewed product based on product_name, most_ordered_day and is_public_holiday

# WITH transform_cte AS 
#         (
#         SELECT dd.calendar_dt, 
#             dd.day_of_the_week_num as most_ordered_day, 
#             dd.working_day as is_public_holiday, dp.product_id, dp.product_name,
#             rt.review, rt.product_id, ot.order_date, ot.product_id
#             FROM  judendu4707_staging.orders_staging as ot
#             JOIN if_common.dim_dates dd ON ot.order_date = dd.calendar_dt
#             JOIN if_common.dim_products dp ON dp.product_id = ot.product_id
#             JOIN judendu4707_staging.reviews_staging rt ON rt.product_id = dp.product_id
#         )
# INSERT INTO judendu4707_analytics.best_performing_product(product_name, most_ordered_day, is_public_holiday)
# SELECT tc.most_ordered_day, tc.is_public_holiday, tc.product_name, tc.review
# FROM transform_cte tc
# ORDER BY review DESC
# LIMIT 1;


















####################   CONSTRAINTS ###########################
 
# alter table judendu4707_staging.orders_staging ADD CONSTRAINT fk_customer FOREIGN KEY(customer_id) REFERENCES if_common.dim_customers(customer_id);
# alter table judendu4707_staging.orders_staging ADD CONSTRAINT fk_product_id FOREIGN KEY(product_id) REFERENCES if_common.dim_products(product_id);
# alter table judendu4707_staging.reviews_staging ADD CONSTRAINT fk_product_id FOREIGN KEY(product_id) REFERENCES if_common.dim_products(product_id);
# alter table judendu4707_staging.shipment_del_staging ADD CONSTRAINT fk_orders FOREIGN KEY(order_id) REFERENCES judendu4707_staging.orders_staging(order_id);
