create database btc_db;

create schema btc;

create table btc.stg_btc(
	"time_period_start" timestamp,
	"time_period_end" timestamp,
	"time_open" timestamp,
	"time_close" timestamp,
	"price_open" float,
	"price_high" float,
	"price_low" float,
	"price_close" float,
	"volume_traded" float,
	"trades_count" int
)

create table btc.daily_btc_summary(
	"date" date,
	"volatility" float,
	"price_open" float,
	"price_close" float,
	"price_low" float,
	"price_high" float,
	"trades_count" float,
	"sample_count" int
)
