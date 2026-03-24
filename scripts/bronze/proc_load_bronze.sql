-- bulk insert to get data from csv files, also make a stored procedure to do this many times

CREATE OR ALTER PROCEDURE bronze.load_bronze AS
BEGIN
	DECLARE @start_time DATETIME, @batch_start DATETIME, @end_time DATETIME, @batch_end DATETIME;
	BEGIN TRY
		PRINT('=============================');
		PRINT('loading bronze layer');
		PRINT('=============================');


		PRINT('-----------------------------');
		PRINT('loading CRM tables...');
		PRINT('-----------------------------');

		SET @start_time = GETDATE();
		SET @batch_start = GETDATE();
		TRUNCATE TABLE bronze.crm_cust_info
		BULK INSERT bronze.crm_cust_info
		FROM 'E:\repos\DataAnalysis\SQL_practice\baraa\sql-data-warehouse-project\datasets\source_crm\cust_info.csv'
		WITH (
		FIRSTROW = 2, -- to skip the column names (first row)
		FIELDTERMINATOR = ',',
		TABLOCK -- locks the table for better performance
		);
		SET @end_time = GETDATE();
		PRINT('time for loading cust_info is ' + CAST(DATEDIFF(second,@end_time,@start_time) AS VARCHAR) + ' seconds');

		SET @start_time = GETDATE();
		TRUNCATE TABLE bronze.crm_prd_info
		BULK INSERT bronze.crm_prd_info
		FROM 'E:\repos\DataAnalysis\SQL_practice\baraa\sql-data-warehouse-project\datasets\source_crm\prd_info.csv'
		WITH (
		FIRSTROW = 2,
		FIELDTERMINATOR = ',',
		TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT('time for loading prd_info is ' + CAST(DATEDIFF(second,@end_time,@start_time) AS NVARCHAR) + ' seconds');


		SET @start_time = GETDATE()
		TRUNCATE TABLE bronze.crm_sales_details
		BULK INSERT bronze.crm_sales_details
		FROM 'E:\repos\DataAnalysis\SQL_practice\baraa\sql-data-warehouse-project\datasets\source_crm\sales_details.csv'
		WITH (
		FIRSTROW = 2,
		FIELDTERMINATOR = ',',
		TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT('time for loading sales_details is ' + CAST(DATEDIFF(second,@end_time,@start_time) AS NVARCHAR) + ' seconds');

		PRINT('-----------------------------');
		PRINT('loading ERP tables...');
		PRINT('-----------------------------');

		SET @start_time = GETDATE();
		TRUNCATE TABLE bronze.erp_cust_az12
		BULK INSERT bronze.erp_cust_az12
		FROM 'E:\repos\DataAnalysis\SQL_practice\baraa\sql-data-warehouse-project\datasets\source_erp\CUST_AZ12.csv'
		WITH (
		FIRSTROW = 2,
		FIELDTERMINATOR = ',',
		TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT('time for loading erp_customers is ' + CAST(DATEDIFF(second,@end_time,@start_time) AS NVARCHAR) + ' seconds');


		SET @start_time = GETDATE();
		TRUNCATE TABLE bronze.erp_loc_a101
		BULK INSERT bronze.erp_loc_a101
		FROM 'E:\repos\DataAnalysis\SQL_practice\baraa\sql-data-warehouse-project\datasets\source_erp\LOC_A101.csv'
		WITH (
		FIRSTROW = 2,
		FIELDTERMINATOR = ',',
		TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT('time for loading erp_loc is ' + CAST(DATEDIFF(second,@end_time,@start_time) AS NVARCHAR) + ' seconds');


		SET @start_time = GETDATE();
		TRUNCATE TABLE bronze.erp_px_cat_g1v2
		BULK INSERT bronze.erp_px_cat_g1v2
		FROM 'E:\repos\DataAnalysis\SQL_practice\baraa\sql-data-warehouse-project\datasets\source_erp\PX_CAT_G1V2.csv'
		WITH (
		FIRSTROW = 2,
		FIELDTERMINATOR = ',',
		TABLOCK
		);
		SET @end_time = GETDATE();
		SET @batch_end = GETDATE();
		PRINT('time for loading erp_px_cat is ' + CAST(DATEDIFF(second,@end_time,@start_time) AS NVARCHAR) + ' seconds');
		PRINT('time for whole batch is ' + CAST(DATEDIFF(second, @batch_end, @batch_start) AS NVARCHAR) + ' seconds');
	END TRY
	BEGIN CATCH
	PRINT('=============================');
	PRINT ('im sorry, but an error happened :c');
	PRINT ('error message: ' + ERROR_MESSAGE());
	PRINT ('error number: ' + CAST(ERROR_NUMBER() AS NVARCHAR));
	PRINT ('error state: ' + CAST(ERROR_STATE() AS NVARCHAR));
	PRINT('=============================');
	END CATCH
END
