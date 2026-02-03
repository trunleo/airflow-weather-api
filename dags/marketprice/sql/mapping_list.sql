-- public.mapping_list definition

-- Drop table

-- DROP TABLE public.mapping_list;

CREATE TABLE public.mapping_list (
	"ID" varchar(50) PRIMARY KEY,
	"NAME_TH" varchar(255) NULL,
	"APP_NAME_TH" varchar(255) NULL,
	"NAME_EN" varchar(128) NULL,
	"APP_NAME_EN" varchar(128) NULL,
	"GROUP_TYPE_TH" varchar(255) NULL,
	"GROUP_TYPE_EN" varchar(255) NULL,
	"CATEGORY_TH" varchar(255) NULL,
	"CATEGORY_EN" varchar(255) NULL,
	"UNIT_TH" varchar(50) NULL,
	"UNIT_EN" varchar(50) NULL
);