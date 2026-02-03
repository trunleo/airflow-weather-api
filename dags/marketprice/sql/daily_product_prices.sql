-- public.daily_product_prices definition

-- Drop table

-- DROP TABLE public.daily_product_prices;

CREATE TABLE IF NOT EXIST public.daily_product_prices (
	date_time timestamp(6) NULL,
	country_code varchar NULL,
	category_id varchar NULL,
	category_name varchar NULL,
	category_name_en varchar NULL,
	price_type varchar NULL,
	price_type_en varchar NULL,
	product_id varchar NULL,
	product_name varchar NULL,
	product_name_en varchar NULL,
	currency_code varchar NULL,
	min_price float8 NULL,
	max_price float8 NULL,
	avg_price float8 NULL,
	unit_id varchar NULL,
	unit_name varchar NULL,
	unit_name_en varchar NULL,
	CONSTRAINT daily_product_prices_unique UNIQUE (date_time, product_id)
);