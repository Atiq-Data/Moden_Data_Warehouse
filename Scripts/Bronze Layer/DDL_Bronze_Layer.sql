===========================================================
           DDL:CREATE TABLES OF BRONZE LAYER
===========================================================
SCRIPT OVERVIEW:
	This script sets up tables within the 'bronze' schema.
	If any of the tables already exist, they will be removed first.
	Use this script to refresh and apply the latest DDL structure
	for the 'bronze' layer tables.
===========================================================
*/
IF OBJECT_ID('Bronze.CRM_Cust_Info', 'U') IS NOT NULL
	DROP TABLE Bronze.CRM_Cust_Info;
CREATE TABLE Bronze.CRM_Cust_Info(
	cst_id INT,
	cst_key VARCHAR(30),
	cst_firstname VARCHAR(50),
	cst_lastname VARCHAR(50),
	cst_martial_status VARCHAR(20),
	cst_gndr VARCHAR(20),
	cst_create_date DATE
);
GO
IF OBJECT_ID('Bronze.CRM_Prd_Info', 'U') IS NOT NULL
	DROP TABLE Bronze.CRM_Prd_Info;
CREATE TABLE Bronze.CRM_Prd_Info(
	prd_id INT,
	prd_key VARCHAR(30),
	prd_nm VARCHAR(50),
	prd_cost INT,
	prd_line VARCHAR(30),
	prd_start_dt DATETIME,
	prd_end_dt DATETIME
);
GO
IF OBJECT_ID('Bronze.CRM_Sales_Details', 'U') IS NOT NULL
	DROP TABLE Bronze.CRM_Sales_Details;
CREATE TABLE Bronze.CRM_Sales_Details(
	sls_ord_num VARCHAR(20),
	sls_prd_key VARCHAR(20),
	sls_cust_id INT,
	sls_order_dt INT,
	sls_ship_dt INT,
	sls_due_dt INT,
	sls_sales INT,
	sls_quantity INT,
	sls_price INT
);
GO
IF OBJECT_ID ('Bronze.ERP_Cust_Az12', 'U') IS NOT NULL
	DROP TABLE Bronze.ERP_Cust_Az12;
CREATE TABLE Bronze.ERP_Cust_Az12(
	CID VARCHAR(30),
	BDATE DATE,
	GEN VARCHAR(10)
);
GO
IF OBJECT_ID ('Bronze.ERP_Loc_A101', 'U') IS NOT NULL
	DROP TABLE Bronze.ERP_Loc_A101;
CREATE TABLE Bronze.ERP_Loc_A101(
	CID VARCHAR(30),
	CNTRY VARCHAR(30)
);
GO
IF OBJECT_ID ('Bronze.ERP_Px_Cat_G1v2', 'U') IS NOT NULL
	DROP TABLE Bronze.ERP_Px_Cat_G1v2;
CREATE TABLE Bronze.ERP_Px_Cat_G1v2(
	ID VARCHAR(20),
	CAT VARCHAR(50),
	SUBCAT VARCHAR(50),
	MAINTENANCE VARCHAR(20)
);
