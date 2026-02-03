-- public.mapping_list definition

-- Drop table

-- DROP TABLE public.mapping_list;

CREATE TABLE public.mapping_list (
	id varchar(50) PRIMARY KEY,
	name_th varchar(255) NULL,
	app_name_th varchar(255) NULL,
	name_en varchar(128) NULL,
	app_name_en varchar(128) NULL,
	group_type_th varchar(255) NULL,
	group_type_en varchar(255) NULL,
	category_th varchar(255) NULL,
	category_en varchar(255) NULL,
	unit_th varchar(50) NULL,
	unit_en varchar(50) NULL
);