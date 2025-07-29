/*=========================================================================
  DDL Script: Initialize Silver Layer Tables
==========================================================================

  Description:
    This script is responsible for setting up the table structures within
    the 'silver' schema. It will remove any existing tables with the same
    names and recreate them to reflect updated definitions.

    Run this script to refresh the 'silver' layer based on cleaned and
    processed data from the 'bronze' layer.

=========================================================================*/
IF OBJECT_ID('Silver.CRM_Cust_Info', 'U') IS NOT NULL
	DROP TABLE Silver.CRM_Cust_Info;
CREATE TABLE Silver.CRM_Cust_Info(
	cst_id INT,
	cst_key VARCHAR(30),
	cst_firstname VARCHAR(50),
	cst_lastname VARCHAR(50),
	cst_martial_status VARCHAR(20),
	cst_gndr VARCHAR(20),
	cst_create_date DATE,
	mdwh_create_date DATETIME2 DEFAULT GETDATE()
);
GO
IF OBJECT_ID('Silver.CRM_Prd_Info', 'U') IS NOT NULL
	DROP TABLE Silver.CRM_Prd_Info;
CREATE TABLE Silver.CRM_Prd_Info(
	prd_id INT,
	cat_id VARCHAR(30),
	prd_key VARCHAR(30),
	prd_nm VARCHAR(50),
	prd_cost INT,
	prd_line VARCHAR(30),
	prd_start_dt DATETIME,
	prd_end_dt DATETIME,
	mdwh_create_date DATETIME2 DEFAULT GETDATE()
);
GO
IF OBJECT_ID('Silver.CRM_Sales_Details', 'U') IS NOT NULL
	DROP TABLE Silver.CRM_Sales_Details;
CREATE TABLE Silver.CRM_Sales_Details(
	sls_ord_num VARCHAR(20),
	sls_prd_key VARCHAR(20),
	sls_cust_id INT,
	sls_order_dt INT,
	sls_ship_dt INT,
	sls_due_dt INT,
	sls_sales INT,
	sls_quantity INT,
	sls_price INT,
	mdwh_create_date DATETIME2 DEFAULT GETDATE()
);
GO
IF OBJECT_ID ('Silver.ERP_Cust_Az12', 'U') IS NOT NULL
	DROP TABLE Silver.ERP_Cust_Az12;
CREATE TABLE Silver.ERP_Cust_Az12(
	CID VARCHAR(30),
	BDATE DATE,
	GEN VARCHAR(10),
	mdwh_create_date DATETIME2 DEFAULT GETDATE()
);
GO
IF OBJECT_ID ('Silver.ERP_Loc_A101', 'U') IS NOT NULL
	DROP TABLE Silver.ERP_Loc_A101;
CREATE TABLE Silver.ERP_Loc_A101(
	CID VARCHAR(30),
	CNTRY VARCHAR(30),
	mdwh_create_date DATETIME2 DEFAULT GETDATE()
);
GO
IF OBJECT_ID ('Silver.ERP_Px_Cat_G1v2', 'U') IS NOT NULL
	DROP TABLE Silver.ERP_Px_Cat_G1v2;
CREATE TABLE Silver.ERP_Px_Cat_G1v2(
	ID VARCHAR(20),
	CAT VARCHAR(50),
	SUBCAT VARCHAR(50),
	MAINTENANCE VARCHAR(20),
	mdwh_create_date DATETIME2 DEFAULT GETDATE()
);
GO
INSERT INTO Silver.CRM_Cust_Info(
	cst_id,
	cst_key,
	cst_firstname,
	cst_lastname,
	cst_martial_status,
	cst_gndr,
	cst_create_date)
SELECT
cst_id,
cst_key,
TRIM(cst_firstname) as cst_firstname,
TRIM(cst_lastname) as cst_lastname,
CASE WHEN TRIM(UPPER(cst_martial_status)) = 'M' THEN 'Married'
	WHEN TRIM(UPPER(cst_martial_status)) = 'S' THEN 'Single'
	ELSE 'N/A'
	END AS cst_martial_status,
CASE WHEN TRIM(UPPER(cst_gndr)) = 'M' THEN 'Male'
	WHEN TRIM(UPPER(cst_gndr)) = 'F' THEN 'Female'
	ELSE 'N/A'
	END AS cst_gndr,
cst_create_date
FROM
(
SELECT
	  *,
ROW_NUMBER() OVER(PARTITION BY cst_id ORDER BY cst_create_date DESC) AS flag_last
FROM Bronze.CRM_Cust_Info
WHERE cst_id IS NOT NULL)T WHERE flag_last=1
GO
