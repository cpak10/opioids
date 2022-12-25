/* drop table if already in the database */
drop table if exists opioids.arcos_ca_los_angeles_beverly_hills;

/* create the table */
create table opioids.arcos_ca_los_angeles_beverly_hills(
    id int not null,
    reporter_dea_no varchar(256),
    reporter_bus_act varchar(256),
    reporter_name varchar(256),
    reporter_addl_co_info varchar(256),
    reporter_address1 varchar(256),
    reporter_address2 varchar(256),
    reporter_city varchar(256),
    reporter_state varchar(256),
    reporter_zip varchar(256),
    reporter_county varchar(256),
    buyer_dea_no varchar(256),
    buyer_bus_act varchar(256),
    buyer_name varchar(256),
    buyer_addl_co_info varchar(256),
    buyer_address1 varchar(256),
    buyer_address2 varchar(256),
    buyer_city varchar(256),
    buyer_state varchar(256),
    buyer_zip varchar(256),
    buyer_county varchar(256),
    transaction_code varchar(256),
    drug_code varchar(256),
    ndc_no varchar(256),
    drug_name varchar(256),
    quantity float(2),
    unit varchar(256),
    action_indicator varchar(256),
    order_form_no varchar(256),
    correction_no varchar(256),
    strength varchar(256),
    transaction_date varchar(256),
    calc_base_wt_in_gm float(2),
    dosage_unit varchar(256),
    transaction_id varchar(256),
    product_name varchar(256),
    ingredient_name varchar(256),
    measure varchar(256),
    mme_conversion_factor float(2),
    combined_labeler_name varchar(256),
    revised_company_name varchar(256),
    reporter_family varchar(256),
    dos_str varchar(256),
    primary key (id)
);

/* set the global infile option on */
set global local_infile = True;
show global variables like "local_infile";

/* load data into the table shell */
load data local infile "C:/ProgramData/MySQL/MySQL Server 8.0/Uploads/ARCOS_CA_LOS ANGELES_BEVERLY HILLS.csv"
	into table opioids.arcos_ca_los_angeles_beverly_hills
    fields terminated by "|"
    lines terminated by "\n"
    ignore 1 lines
;

/* show sample of the table */
select *
from opioids.arcos_ca_los_angeles_beverly_hills
limit 10;