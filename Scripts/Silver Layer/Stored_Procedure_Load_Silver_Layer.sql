/*==============================================================================
  Stored Procedure: Load Silver Layer (Bronze to Silver)
==============================================================================

  Description:
    This procedure executes the ETL (Extract, Transform, Load) workflow to
    populate the tables within the 'silver' schema using data sourced from
    the 'bronze' layer.

  Key Operations:
    - Clears existing records from the Silver tables.
    - Loads processed and structured data from Bronze into Silver tables.

  Parameters:
    None.
    This procedure does not take any input parameters or return output values.

  Example Usage:
    EXEC Silver.load_silver;

==============================================================================*/
CREATE OR ALTER PROCEDURE Silver.Load_Silver AS
BEGIN
	DECLARE @Start_Time DATETIME, @End_Time DATETIME, @Batch_Start_Time DATETIME, @Batch_End_Time DATETIME
	BEGIN TRY
	SET @Batch_Start_Time =GETDATE();
	PRINT '=================================================='
	PRINT                 '  LOADING SILVER LAYER   '
	PRINT '=================================================='
	PRINT '=================================================='
	PRINT                 '      CRM TABLES   '
	PRINT '=================================================='
	SET @Start_Time=GETDATE();
		PRINT '>> TRUNCATING TABLE: Silver.CRM_Cust_Info';
		TRUNCATE TABLE Silver.CRM_Cust_Info;
		PRINT '>> INSERTING DATA INTO: Silver.CRM_Cust_Info';
		INSERT INTO Silver.CRM_Cust_Info(
			cst_id,
			cst_key,
			cst_firstname,
			cst_lastname,
			cst_martial_status,
			cst_gndr,
			cst_create_date
		)
		SELECT
			cst_id,
			cst_key,
			TRIM(cst_firstname) AS cst_firstname,
			TRIM(cst_lastname) AS cst_lastname,
				CASE WHEN UPPER(TRIM(cst_martial_status)) ='S' THEN 'Single'
				 WHEN UPPER(TRIM(cst_martial_status)) ='M' THEN 'Married'
				 ELSE 'N/A'
			END cst_martial_status,-----NORMALIZED MARTIAL STATUS TO READABLE VALUE FORMAT
			CASE WHEN UPPER(TRIM(cst_gndr)) ='M' THEN 'Male'
				 WHEN UPPER(TRIM(cst_gndr)) ='F' THEN 'Female'
				 ELSE 'N/A'
			END cst_gndr, -----NORMALIZED GENDER TO READABLE VALUE FORMAT
			cst_create_date
		FROM
		(
			SELECT *,
			ROW_NUMBER() OVER(PARTITION BY cst_id ORDER BY cst_create_date DESC) AS flag_data
			FROM Bronze.CRM_Cust_Info
			WHERE cst_id IS NOT NULL
		)T 
		WHERE flag_data = 1 ---- SELECT ONLY LATEST VALUE PER CUSTOMER
		SET @End_Time=GETDATE();
		PRINT '>> LOAD DURATION: '+ CAST (DATEDIFF(SECOND, @Start_Time,@End_Time)AS VARCHAR)+' Seconds';
		PRINT '-----------------'
		SET @Start_Time=GETDATE();
		PRINT '>> TRUNCATING TABLE: Silver.CRM_Prd_Info';
		TRUNCATE TABLE Silver.CRM_Prd_Info;
		PRINT '>> INSERTING DATA INTO: Silver.CRM_Prd_Info';
		INSERT INTO Silver.CRM_Prd_Info (
			prd_id,
			cat_id,
			prd_key,
			prd_nm,
			prd_cost,
			prd_line,
			prd_start_dt,
			prd_end_dt
		)
		SELECT prd_id,
			  REPLACE(SUBSTRING(prd_key,1,5),'-','_') AS cat_id,---EXTRACT CATEGORY ID
			  SUBSTRING(prd_key,7,LEN(prd_key)) AS prd_key,----EXTRACT PRODUCT KEY
			  prd_nm,
			  ISNULL(prd_cost,0) AS prd_cost,---HANDLING NULLS
			  CASE UPPER(TRIM(prd_line))
			  WHEN 'M' THEN 'Mountain'
			  WHEN 'R' THEN 'Road'
			  WHEN 'S' THEN 'Other Sales'	
			  WHEN 'T' THEN 'Touring'
				ELSE 'N/A' 
				END AS prd_line,----MAP PRODUCT LINE CODES TO DESCRIPTIVE VALUES
			  CAST(prd_start_dt AS DATE) AS prd_start_dt,
			  CAST(
				LEAD(prd_start_dt) OVER(PARTITION BY prd_key ORDER BY prd_start_dt )-1 AS DATE
				) AS prd_end_dt---CALCULATE END DATE AS ONE DAY BEFORE THE NEXT START DATE
		FROM
			Bronze.CRM_Prd_Info
		SET @End_Time=GETDATE();
		PRINT '>> LOAD DURATION: '+ CAST (DATEDIFF(SECOND, @Start_Time,@End_Time)AS VARCHAR)+' Seconds';
		PRINT '-----------------'
		SET @Start_Time=GETDATE();
		PRINT '>> TRUNCATING TABLE: Silver.CRM_Sales_Details';
		TRUNCATE TABLE Silver.CRM_Sales_Details;
		PRINT '>> INSERTING DATA INTO: Silver.CRM_Sales_Details';
		INSERT INTO Silver.CRM_Sales_Details(
			sls_ord_num,
			sls_prd_key,
			sls_cust_id,
			sls_order_dt,
			sls_ship_dt,
			sls_due_dt,
			sls_sales,
			sls_quantity,
			sls_price)
		SELECT 
			sls_ord_num,
			sls_prd_key,
			sls_cust_id,
		CASE WHEN sls_order_dt = 0 OR LEN(sls_order_dt) !=8 THEN NULL
			ELSE CAST(CAST(sls_order_dt AS VARCHAR) AS DATE)
			END sls_order_dt,
		CASE WHEN sls_ship_dt = 0 OR LEN(sls_ship_dt) !=8 THEN NULL
			ELSE CAST(CAST(sls_ship_dt AS VARCHAR) AS DATE)
			END sls_ship_dt,
		CASE WHEN sls_due_dt = 0 OR LEN(sls_due_dt) !=8 THEN NULL
			ELSE CAST(CAST(sls_due_dt AS VARCHAR) AS DATE)
			END sls_due_dt,
		CASE WHEN sls_sales IS NULL OR sls_sales <= 0 OR sls_sales != sls_quantity*ABS(sls_price)
			THEN sls_quantity*ABS(sls_price)
			ELSE sls_sales
			END AS sls_sales,
			sls_quantity,
			CASE WHEN sls_price IS NULL OR sls_price <= 0
			THEN sls_sales/NULLIF(sls_quantity,0)
			ELSE sls_price
			END AS sls_price
		FROM Bronze.CRM_Sales_Details
		SET @End_Time=GETDATE();
		PRINT '=================================================='
		PRINT                 '      CRM TABLES   '
		PRINT '=================================================='
		PRINT '>> LOAD DURATION: '+ CAST (DATEDIFF(SECOND, @Start_Time,@End_Time)AS VARCHAR)+' Seconds';
		PRINT '-----------------'
		SET @Start_Time=GETDATE();
		PRINT '>> TRUNCATING TABLE: Silver.ERP_Cust_Az12';
		TRUNCATE TABLE Silver.ERP_Cust_Az12;
		PRINT '>> INSERTING DATA INTO: Silver.ERP_Cust_Az12';
		INSERT INTO Silver.ERP_Cust_Az12(
		CID,
		BDATE,
		GEN
		)
		SELECT
		CASE WHEN CID LIKE 'NAS%' THEN SUBSTRING(CID,4,LEN(CID))-- REMOVE 'NAS' PREFIX IF PRESENT
			ELSE CID
			END AS Cid,
		CASE WHEN BDATE >= GETDATE() THEN NULL--REMOVE FUTURE BIRTH DATE
			ELSE BDATE
			END Bdate,
		CASE WHEN UPPER(TRIM(GEN)) IN ('F','FEMALE') THEN 'Female'--NORMALIZE GENDER VALUES AND HANDLE UNKNOWN CASES
			WHEN UPPER(TRIM(GEN)) IN ('M','MALE') THEN 'Male'
			ELSE 'N/A'
			END Gen
			FROM Bronze.ERP_Cust_Az12
		SET @End_Time=GETDATE();
		PRINT '>> LOAD DURATION: '+ CAST (DATEDIFF(SECOND, @Start_Time,@End_Time)AS VARCHAR)+' Seconds';
		PRINT '-----------------'
		SET @Start_Time=GETDATE();
		PRINT '>> TRUNCATING TABLE: Silver.ERP_Loc_A101';
		TRUNCATE TABLE Silver.ERP_Loc_A101;
		PRINT '>> INSERTING DATA INTO: Silver.ERP_Loc_A101';
		Insert Into Silver.ERP_Loc_A101(
		CID,
		CNTRY
		)
		Select  REPLACE(CID,'-','') AS Cid,
		Case When Cntry = '' OR cntry IS NULL Then 'N/A' 
			WHEN UPPER(TRIM(CNTRY)) IN ('USA','US','UNITED STATES') THEN ('United States')
			WHEN UPPER(TRIM(CNTRY)) IN ('GERMANY','DE') THEN ('Germany')
			WHEN UPPER(TRIM(CNTRY)) IN ('CANADA') THEN ('Canada')
			WHEN UPPER(TRIM(CNTRY)) IN ('AUSTRALIA') THEN ('Australia')
			WHEN UPPER(TRIM(CNTRY)) IN ('FRANCE') THEN ('FRANCE')
			WHEN UPPER(TRIM(CNTRY)) IN ('UNITED KINGDOM') THEN ('United Kingdom')
			ELSE TRIM(CNTRY)
			END AS Cntry
			FROM Bronze.ERP_LOC_A101
		SET @End_Time=GETDATE();
		PRINT '>> LOAD DURATION: '+ CAST (DATEDIFF(SECOND, @Start_Time,@End_Time)AS VARCHAR)+' Seconds';
		PRINT '-----------------'
		SET @Start_Time=GETDATE();
		PRINT '>> TRUNCATING TABLE: Silver.ERP_Px_Cat_G1v2';
		TRUNCATE TABLE Silver.ERP_Px_Cat_G1v2;
		PRINT '>> INSERTING DATA INTO: Silver.ERP_Px_Cat_G1v2';
		Insert Into Silver.ERP_Px_Cat_G1v2(
			Id,
			CAT,
			SUBCAT,
			MAINTENANCE
		)
		Select * from bronze.erp_px_cat_g1v2----Good Data Quality NO Alteration Required
		SET @End_Time=GETDATE();
		PRINT '>> LOAD DURATION: '+ CAST (DATEDIFF(SECOND, @Start_Time,@End_Time)AS VARCHAR)+' Seconds';
		PRINT '-----------------'
	SET @Batch_End_Time=GETDATE();
		PRINT '=====================================================';
		PRINT '				  SILVER LAYER IS LOADED                ';
		PRINT 'SILVER LAYER LOADING DURATION IS '+CAST(DATEDIFF(SECOND,@Batch_Start_Time,@Batch_End_Time) AS VARCHAR)+' Seconds' ;
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
