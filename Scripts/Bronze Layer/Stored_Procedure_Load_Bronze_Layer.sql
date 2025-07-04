/*
=======================================================================================
	      Stored Procedure Name: Load Bronze Layer Data (Source to Bronze)
=======================================================================================
Description:
	This procedure imports data into the 'bronze' schema 
	using external CSV files as the source (CRM & ERP).
Steps Performed:
	Clears existing records in bronze tables via TRUNCATE.
	Loads new data using the BULK INSERT command from CSV files.
Input Parameters:
	None.
	This procedure runs without any inputs and does not return outputs.
How to Execute:
	EXEC bronze.load_bronze;
===========================================================
*/
CREATE OR ALTER PROCEDURE Bronze.Load_Bronze AS
BEGIN
	DECLARE @Start_Time DATETIME, @End_Time DATETIME, @Batch_Start_Time DATETIME, @Batch_End_Time DATETIME

	BEGIN TRY
	SET @Batch_Start_Time=GETDATE();
		PRINT '=========================================';
		PRINT			'LOADING BRONZE LAYER';
		PRINT '=========================================';
		PRINT '-----------------------------------------';
		PRINT			'LOADING CRM TABLES';
		PRINT '-----------------------------------------';
		PRINT '>> TRUNCATING TABLE: Bronze.CRM_Cust_info';
		SET @Start_Time =GetDate();
		TRUNCATE TABLE Bronze.CRM_Cust_info
		PRINT '>> INSERTING DATA INTO: Bronze.CRM_Cust_Info';
		BULK INSERT Bronze.CRM_Cust_Info
		FROM 'C:\Users\PMLS\Desktop\Course Notes\sql-data-warehouse-project\datasets\source_crm\cust_info.csv'
		WITH (
			FIRSTROW=2,
			FIELDTERMINATOR=',',
			TABLOCK
		);
		SET @End_Time =GetDate();
		PRINT '>> LOAD DURATION: '+ CAST (DATEDIFF(SECOND, @Start_Time,@End_Time)AS VARCHAR)+' Seconds';
		PRINT '-----------------';
		SET @Start_Time =GetDate();
		PRINT '>> TRUNCATING TABLE:  Bronze.CRM_Prd_Info';
		TRUNCATE TABLE Bronze.CRM_Prd_Info
		PRINT '>> INSERTING DATA INTO: Bronze.CRM_Prd_Info';
		BULK INSERT Bronze.CRM_Prd_Info
			FROM 'C:\Users\PMLS\Desktop\Course Notes\sql-data-warehouse-project\datasets\source_crm\prd_info.csv'
			WITH (
			FIRSTROW=2,
			FIELDTERMINATOR=',',
			TABLOCK
			);
		SET @End_Time =GetDate();
		PRINT '>> LOAD DURATION: '+ CAST (DATEDIFF(SECOND, @Start_Time,@End_Time)AS VARCHAR)+' Seconds';
		PRINT '-----------------'
		SET @Start_Time =GetDate();
		PRINT '>> TRUNCATING TABLE:  Bronze.CRM_Sales_Details';
		TRUNCATE TABLE Bronze.CRM_Sales_Details
		PRINT '>> INSERTING DATA INTO: Bronze.CRM_Sales_Details';
		BULK INSERT Bronze.CRM_Sales_Details
			FROM 'C:\Users\PMLS\Desktop\Course Notes\sql-data-warehouse-project\datasets\source_crm\sales_details.csv'
			with(
			FIRSTROW=2,
			FIELDTERMINATOR=',',
			TABLOCK
			);
		SET @End_Time =GetDate();
		PRINT '>> LOAD DURATION: '+ CAST (DATEDIFF(SECOND, @Start_Time,@End_Time)AS VARCHAR)+' Seconds';
		PRINT '-----------------'
		PRINT '-----------------------------------------';
		PRINT			'LOADING ERP TABLES';
		PRINT '-----------------------------------------';
		SET @Start_Time =GetDate();
		PRINT '>> TRUNCATING TABLE:  BRONZE.ERP_CUST_AZ12';
		TRUNCATE TABLE BRONZE.ERP_CUST_AZ12
		PRINT '>> INSERTING DATA INTO: BRONZE.ERP_CUST_AZ12';
		BULK INSERT BRONZE.ERP_CUST_AZ12
			FROM 'C:\Users\PMLS\Desktop\Course Notes\sql-data-warehouse-project\datasets\source_erp\CUST_AZ12.csv'
			WITH(
			FIRSTROW=2,
			FIELDTERMINATOR=',',
			TABLOCK
		);
		SET @End_Time =GetDate();
		PRINT '>> LOAD DURATION: '+ CAST (DATEDIFF(SECOND, @Start_Time,@End_Time)AS VARCHAR)+' Seconds';
		PRINT '-----------------'
		SET @Start_Time =GetDate();
		PRINT '>> TRUNCATING TABLE:  BRONZE.ERP_LOC_A101';
		TRUNCATE TABLE BRONZE.ERP_LOC_A101
		PRINT '>> INSERTING DATA INTO: BRONZE.ERP_LOC_A101';
		BULK INSERT BRONZE.ERP_LOC_A101
			FROM 'C:\Users\PMLS\Desktop\Course Notes\sql-data-warehouse-project\datasets\source_erp\LOC_A101.csv'
			WITH(
			FIRSTROW=2,
			FIELDTERMINATOR=',',
			TABLOCK
		);
		SET @End_Time =GetDate();
		PRINT '>> LOAD DURATION: '+ CAST (DATEDIFF(SECOND, @Start_Time,@End_Time)AS VARCHAR)+' Seconds';
		PRINT '-----------------'
		SET @Start_Time =GetDate();
		PRINT '>> TRUNCATING TABLE:  BRONZE.ERP_PX_CAT_G1V2';
		TRUNCATE TABLE BRONZE.ERP_PX_CAT_G1V2
		PRINT '>> INSERTING DATA INTO: BRONZE.ERP_PX_CAT_G1V2';
		BULK INSERT BRONZE.ERP_PX_CAT_G1V2
		FROM 'C:\Users\PMLS\Desktop\Course Notes\sql-data-warehouse-project\datasets\source_erp\PX_CAT_G1V2.csv'
		WITH(
		FIRSTROW=2,
		FIELDTERMINATOR=',',
		TABLOCK
	);
		SET @End_Time =GetDate();
		PRINT '>> LOAD DURATION: '+ CAST (DATEDIFF(SECOND, @Start_Time,@End_Time)AS VARCHAR)+' Seconds';
		PRINT '-----------------'
		SET @Batch_End_Time=GETDATE();
		PRINT '=====================================================';
		PRINT '				  BRONZE LAYER IS LOADED                ';
		PRINT 'BRONZE LAYER LOADING DURATION IS '+CAST(DATEDIFF(SECOND,@Batch_Start_Time,@Batch_End_Time) AS VARCHAR)+' Seconds' ;
		PRINT '=====================================================';
	END TRY
	BEGIN CATCH
	PRINT '====================================================';
	PRINT '      ERROR OCURRED DURING LOADING BRONZE LAYER      '
	PRINT 'ERROR MESSAGE ' + ERROR_MESSAGE();
	PRINT 'ERROR MESSAGE ' + CAST(ERROR_NUMBER() AS VARCHAR);
	PRINT 'ERROR MESSAGE ' + CAST(ERROR_STATE() AS VARCHAR);
	PRINT '====================================================';
	END CATCH
END

EXEC Bronze.Load_Bronze
