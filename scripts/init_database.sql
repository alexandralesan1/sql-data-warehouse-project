-- Create Database 'DataWarehouse'


use master;
go
-- Drop and create the 'DataWarehouse' database
if exists(select 1 from sys.databases where name ='DataWarehouse')
begin 
  alter database DataWarehouse set single_user with rollback immediate;
  drop database DataWarehouse;
end;
go
  
create database DataWarehouse;

use DataWarehouse;

-- First Step
-- Create schemas for layers
-- for each layer we create a schema

create schema bronze;
go -- separated batches when working with multiple SQL statements
create schema silver;
go
create schema gold;
go
