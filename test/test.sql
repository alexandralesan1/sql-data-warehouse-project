/*Script Purpose:
This script performs various quality checks for data consistency, accuracy,
and standardization across the 'silver' schema. It includes checks for:
- Null or duplicate primary keys.
- Unwanted spaces in string fields.
- Data standardization and consistency.
- Invalid date ranges and orders.
- Data consistency between related fields. */



-- Metadata Columns
-- extra columns added by data engineers that do not
-- originate from the source data
-- dwh_create_date DATETIME2 DEFAULT GETDATE()


-- Data Cleansing
-- Check For Nulls or Duplicates in Primary Key
-- Expectation: No Result
-- Solution : Aggregate the ids in groups


select * from bronze.crm_cust_info;

select cst_id, COUNT(*) from 
bronze.crm_cust_info
group by cst_id
Having COUNT(*) > 1 or cst_id IS NULL


select * from 
bronze.crm_cust_info
where cst_id =29466


---- Duplicates with old status
select * from (
select *,
ROW_NUMBER() over(partition by cst_id order by cst_create_date desc ) as flag_last
from bronze.crm_cust_info
)t where flag_last !=1

---- Check unwanted spaces
---- expectation: no results

Select cst_firstname 
from bronze.crm_cust_info
where cst_firstname != TRIM(cst_firstname)

Select cst_lastname
from bronze.crm_cust_info
where cst_lastname != TRIM(cst_lastname)


Select cst_gndr
from bronze.crm_cust_info
where cst_gndr != TRIM(cst_gndr)



-- Data Standardization & Consistency

select distinct cst_gndr 
from bronze.crm_cust_info


select cst_id, count(*) from silver.crm_cust_info
group by cst_id
having count(*) > 1 or cst_id is null

--check for unwanted spaces
select cst_firstname 
from silver.crm_cust_info
where cst_firstname != trim(cst_firstname)

select cst_firstname 
from silver.crm_cust_info
where cst_lastname != trim(cst_lastname)

---- Check for nulls or duplicates in primary key
---- Expectation: No results

select 
prd_id, 
count(*)
from bronze.crm_prd_info
group by prd_id
having count(*) > 1 or prd_id is null


select 
prd_id,
prd_key,
REPLACE(SUBSTRING(prd_key, 1, 5), '-', '_') as cat_id , -- extract part of a string
substring(prd_key, 7, LEN(prd_key)) AS prd_key,
prd_nm,
prd_cost,
prd_line,
prd_start_dt,
prd_end_dt
from bronze.crm_prd_info
where replace(substring(prd_key, 1,5), '-', '_') not in  -- (filter) Find what is not in bronze.erp_px_cat_g1v2
(select distinct id from bronze.erp_px_cat_g1v2);




select 
prd_id,
prd_key,
REPLACE(SUBSTRING(prd_key, 1, 5), '-', '_') as cat_id , -- extract part of a string
substring(prd_key, 7, LEN(prd_key)) AS prd_key,
prd_nm,
prd_cost,
prd_line,
prd_start_dt,
prd_end_dt
from bronze.crm_prd_info
where substring(prd_key, 7, LEN(prd_key)) not in  -- (filter) Find what is not in bronze.crm_sales_details
(select sls_prd_key from bronze.crm_sales_details where sls_prd_key like 'FK-16%' );


-- Check for unwanted spaces
select prd_nm
from bronze.crm_prd_info
where prd_nm != trim(prd_nm)

-- Check for negative values and nulls
select prd_cost
from bronze.crm_prd_info
where prd_cost < 0 or prd_cost is null


select sls_prd_key from bronze.crm_sales_details;
select distinct id from bronze.erp_px_cat_g1v2;



 Checking nulls from prd_line

select distinct prd_line
from bronze.crm_prd_info

-- Check for Invalid Date Orders
select * from bronze.crm_prd_info
where prd_end_dt < prd_start_dt


select 
prd_id,
prd_key,
prd_nm,
prd_start_dt,
prd_end_dt,
lead(prd_start_dt) over(partition by prd_key order by prd_start_dt )-1 as prd_end_dt_test -- Not overlaping 
from bronze.crm_prd_info
where prd_key in ('AC-HE-HL-U509-R', 'AC-HE-HL-U509')




-- Quality check of the silver table

select 
prd_id, count(*) from silver.crm_prd_info
group by prd_id
having count(*) > 1 or prd_id is null

-- data standardization & consistency
select prd_cost
from silver.crm_prd_info
where prd_cost < 0 or prd_cost is null

-- check for invalid date orders
select * from silver.crm_prd_info
where prd_end_dt < prd_start_dt

select * from silver.crm_prd_info



-- CRM SALES DETAILS 

select 
sls_ord_num,
sls_prd_key,
sls_cust_id,
sls_order_dt,
sls_ship_dt,
sls_due_dt,
sls_sales,
sls_quantity,
sls_price
from bronze.crm_sales_details


select 
sls_ord_num,
sls_prd_key,
sls_cust_id,
sls_order_dt,
sls_ship_dt,
sls_due_dt,
sls_sales,
sls_quantity,
sls_price
from bronze.crm_sales_details
where sls_ord_num != trim(sls_ord_num);

-- Check the integrity of the columns
select 
sls_ord_num,
sls_prd_key,
sls_cust_id,
sls_order_dt,
sls_ship_dt,
sls_due_dt,
sls_sales,
sls_quantity,
sls_price
from bronze.crm_sales_details
where sls_prd_key not in (select  prd_key from silver.crm_prd_info)

select 
sls_ord_num,
sls_prd_key,
sls_cust_id,
sls_order_dt,
sls_ship_dt,
sls_due_dt,
sls_sales,
sls_quantity,
sls_price
from bronze.crm_sales_details
where sls_cust_id not in (select cst_id from silver.crm_cust_info)


-- Check for Invalid Dates and Zeros and 
-- check the bad data quality ( if the data is not as longer as expected to be even though its an integer data type)

select 
NULLIF(sls_order_dt ,0) sls_order_dt
from bronze.crm_sales_details
where sls_order_dt <= 0 or Len(sls_order_dt) !=8 ;

-- any dates that are outside of the boundaries
select 
NULLIF(sls_order_dt ,0) sls_order_dt
from bronze.crm_sales_details
where sls_order_dt > 20500101 or sls_order_dt < 19000101 

-- Check for outliers by validating the boundaries of the data range
select 
NULLIF(sls_order_dt ,0) sls_order_dt
from bronze.crm_sales_details
where sls_order_dt <= 0 or Len(sls_order_dt) !=8 or sls_order_dt > 20500101 or sls_order_dt < 19000101;


select 
NULLIF(sls_ship_dt ,0) sls_ship_dt
from bronze.crm_sales_details
where sls_ship_dt <= 0 or Len(sls_ship_dt) !=8 or sls_ship_dt > 20500101 or sls_ship_dt < 19000101;


select 
NULLIF(sls_due_dt ,0) sls_due_dt
from bronze.crm_sales_details
where sls_due_dt <= 0 or Len(sls_due_dt) !=8 or sls_due_dt > 20500101 or sls_due_dt < 19000101;


 Check the invalid date orders
 should be smaller than shipping date
 Expected no reuslts
select * from bronze.crm_sales_details
where sls_order_dt > sls_ship_dt or sls_order_dt > sls_due_dt


-- Check the sales quantity and price columns
--sales = quantity * price
--so negative or zeros or nulls are not allowwed
-- We are searching where the results are not matching our expectations

select distinct 
sls_sales as old_sls_sales, 
sls_quantity, 
sls_price as old_sls_price,
case when sls_sales is null or sls_sales <= 0 or sls_sales != sls_quantity * ABS(sls_price)
		then sls_quantity * ABS(sls_price) -- recalculates those rows for sls_sales
		else sls_sales 
end sls_sales,
case when sls_price is null or sls_price <=0
		then sls_sales / nullif(sls_quantity, 0)
		else sls_price
end as sls_price
from bronze.crm_sales_details
where sls_sales != sls_quantity * sls_price
or sls_sales is null or sls_quantity is null or sls_price is null 
or sls_sales <=0  or sls_quantity <=0 or sls_price <= 0
order by sls_sales, sls_quantity, sls_price;

-- solution 1
-- data issues will be fiexd direct in source system
-- solution 2
-- data issues has to be fixed in data warehouse

-- Rules 
-- if sales is negative, zero, or null, derive it using Quantity and Price
-- if price is zero or null, calculate it using sales and quantity
-- if price is negative, convert it to positive value


-- Checks after insert
-- Expectations no results
select * from silver.crm_sales_details
where sls_order_dt > sls_ship_dt or sls_order_dt > sls_due_dt


select distinct
sls_sales,
sls_quantity,
sls_price
from silver.crm_sales_details
where sls_sales != sls_quantity * sls_price
or sls_sales is null or sls_quantity is null or sls_price is null 
or sls_sales <=0  or sls_quantity <=0 or sls_price <= 0
order by sls_sales, sls_quantity, sls_price;


select * from silver.crm_sales_details;




-- ERP CUST AZ 12
select * from bronze.erp_cust_az12;

-- Check for NAS
select 
case when cid like 'NAS%' then substring(cid, 4, len(cid))
		else cid 
end cid,
bdate,
gen
from bronze.erp_cust_az12
where case when cid like 'NAS%' then substring(cid, 4, len(cid))
		else cid 
end not in (select distinct cst_key from silver.crm_cust_info);



select distinct bdate
from silver.erp_cust_az12
where bdate < '1924-01-01' or bdate > getdate()

select distinct gen,
case when upper(trim(gen)) in ('F', 'FEMALE') then 'Female'
	 when upper(trim(gen)) in ('M', 'MALE') then 'Male'
	 else 'n/a'
	end as gen
from bronze.erp_cust_az12;


select distinct gen from silver.erp_cust_az12;


select distinct bdate
from silver.erp_cust_az12
where bdate < '1924-01-01' or bdate > getdate()

select distinct gen,
case when upper(trim(gen)) in ('F', 'FEMALE') then 'Female'
	 when upper(trim(gen)) in ('M', 'MALE') then 'Male'
	 else 'n/a'
	end as gen
from bronze.erp_cust_az12;


-- Get rid of - in cid column

select replace(cid, '-', '') cid,
cntry
from bronze.erp_loc_a101
where replace(cid, '-', '') not in
(select cst_key from silver.crm_cust_info);


-- ERP PX CAT g1v2
-- No results at trims check for unwanted spaces
select * from bronze.erp_px_cat_g1v2
where cat != trim(cat) or subcat != trim(subcat)
or maintenance != trim(maintenance);

---- Check for unwanted spaces
select distinct cat from bronze.erp_px_cat_g1v2;
select distinct subcat from bronze.erp_px_cat_g1v2;
select distinct maintenance from bronze.erp_px_cat_g1v2;



