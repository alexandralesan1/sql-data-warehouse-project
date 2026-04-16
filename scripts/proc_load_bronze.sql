create or alter procedure bronze.load_bronze  as 
				begin
				declare @start_time datetime, @end_time datetime, @start_batch datetime, @end_batch datetime;
				begin try
				-- Truncate - quickly delete all rows from a table, resetting it to an empty state
				print '--------------------------';
				print 'Loading Bronze Layer';
				print '--------------------------'


				print '--------------------------'
				print 'Loading CRM Tables';
				print '--------------------------'
				

				SELECT servicename, service_account
				FROM sys.dm_server_services;

				EXEC xp_fileexist 'C:\Users\alexa\Desktop\Data with Baraa\sql-data-warehouse-project\datasets\source_crm\cust_info.csv';

				-- BULK INSERTS
				
				print 'Truncating data for : bronze.crm_cust_info ';
				
				truncate table bronze.crm_cust_info;
				set @start_batch = getdate();
				set @start_time = getdate();
				print 'Inserting data into : bronze.crm_cust_info ';
				bulk insert bronze.crm_cust_info
				from 'C:\Users\alexa\Desktop\Data with Baraa\sql-data-warehouse-project\datasets\source_crm\cust_info.csv'
				with (
					firstrow = 2,
					fieldterminator = ',',
					tablock
				);
				set @end_time = GETDATE();
				print 'Load duration:' + CAST(Datediff(second, @start_time, @end_time) AS NVARCHAR) + 'seconds';
				
				print 'Truncating data for : bronze.crm_prd_info ';
				truncate table bronze.crm_prd_info;

				print 'Inserting data into : bronze.crm_prd_info ';

				set @start_time = getdate();
				bulk insert bronze.crm_prd_info
				from 'C:\Users\alexa\Desktop\Data with Baraa\sql-data-warehouse-project\datasets\source_crm\prd_info.csv'
				with (
					firstrow = 2,
					fieldterminator = ',',
					tablock
				);
				set @end_time = GETDATE();
				print 'Load duration:' + CAST(Datediff(second, @start_time, @end_time) AS NVARCHAR) + 'seconds';



				print 'Truncating data for : bronze.crm_sales_details ';
			 truncate table bronze.crm_sales_details;

			 print 'Inserting data into : bronze.crm_sales_details';
			 	set @start_time = getdate();
				bulk insert bronze.crm_sales_details
				from 'C:\Users\alexa\Desktop\Data with Baraa\sql-data-warehouse-project\datasets\source_crm\sales_details.csv'
				with (
					firstrow = 2,
					fieldterminator = ',',
					tablock
				);
				set @end_time = GETDATE();
				print 'Load duration:' + CAST(Datediff(second, @start_time, @end_time) AS NVARCHAR) + 'seconds';
			

			print '--------------------------';
			print 'Loading ERP Tables';
			print '--------------------------';

			print 'Truncating data for : bronze.erp_loc_a101 ';
				truncate table bronze.erp_loc_a101;

				print 'Inserting data into : bronze.erp_loc_a101 ';
					set @start_time = getdate();
				bulk insert bronze.erp_loc_a101
				from 'C:\Users\alexa\Desktop\Data with Baraa\sql-data-warehouse-project\datasets\source_erp\LOC_A101.csv'
				with (
					firstrow = 2,
					fieldterminator = ',',
					tablock
				);
				set @end_time = GETDATE();
				print 'Load duration:' + CAST(Datediff(second, @start_time, @end_time) AS NVARCHAR) + 'seconds';
				
				print 'Truncating data for : bronze.erp_cust_az12 ';
				truncate table bronze.erp_cust_az12;

				print 'Inserting data into : bronze.erp_cust_az12 ';

				set @start_time = getdate();
				bulk insert bronze.erp_cust_az12
				from 'C:\Users\alexa\Desktop\Data with Baraa\sql-data-warehouse-project\datasets\source_erp\CUST_AZ12.csv'
				with (
					firstrow = 2,
					fieldterminator = ',',
					tablock
				);
				set @end_time = GETDATE();
				print 'Load duration:' + CAST(Datediff(second, @start_time, @end_time) AS NVARCHAR) + 'seconds';
			
			print 'Truncating data for : bronze.erp_px_cat_g1v2 ';
				truncate table bronze.erp_px_cat_g1v2;

				set @start_time = getdate();
				bulk insert bronze.erp_px_cat_g1v2
				from 'C:\Users\alexa\Desktop\Data with Baraa\sql-data-warehouse-project\datasets\source_erp\PX_CAT_G1V2.csv'
				with (
					firstrow = 2,
					fieldterminator = ',',
					tablock
				);

				set @end_time = GETDATE();
				print 'Load duration:' + CAST(Datediff(second, @start_time, @end_time) AS NVARCHAR) + 'seconds';

				set @end_batch = GETDATE();
				PRINT '-----------------------------------------'
				print 'Load duration of the whole batch:' + cast(datediff(second, @start_batch, @end_batch) as nvarchar) + 'seconds';
				PRINT '-----------------------------------------'
				end try
				begin catch
				PRINT '-----------------------------------------'
				PRINT 'Error occured during loading bronze layer'
				PRINT 'Error Message' + Error_message();
				Print  'Error number' + CAST(Error_number() AS NVARCHAR);
				PRINT '-----------------------------------------'

				end catch

end;
