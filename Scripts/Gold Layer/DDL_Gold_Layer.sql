-- =============================================
-- Title: Gold Layer View Creation
-- Description: This script generates curated views for the Gold layer of the data warehouse.
--              It transforms and combines data from the Silver layer to create the final star schema models.
-- Usage: These views are intended for direct use in analytics and reporting.
-- =============================================
--- ===============================================
-------         Dimension_Customer
--- ===============================================
CREATE VIEW Gold.Dim_Customer AS
SELECT 
	ROW_NUMBER() OVER(Order by ci.cst_id) AS customer_number,
	ci.cst_id AS customer_id,
	ci.cst_key AS customer_key,
	ci.cst_firstname AS first_name,
	ci.cst_lastname AS last_name,
	la.CNTRY AS country,
	CASE WHEN ci.cst_gndr!='N/A' THEN ci.cst_gndr
	ELSE COALESCE(ca.GEN, 'N/A')
	END AS gender,
	ci.cst_martial_status AS martial_status,
	ca.BDATE AS birth_date,
	ci.cst_create_date AS create_date
FROM Silver.CRM_Cust_Info AS ci
LEFT JOIN Silver.ERP_Cust_Az12 AS ca
ON ci.cst_key=ca.CID
LEFT JOIN Silver.ERP_Loc_A101 AS la
on ci.cst_key=la.CID
--- ===============================================
-------         Dimension_Product
--- ===============================================
CREATE VIEW Gold.Dim_Product AS
SELECT 
	ROW_NUMBER() OVER(ORDER BY prd_start_dt, prd_key) as product_number,
	pi.prd_id AS product_id,
	pi.cat_id as category_id,
	pi.prd_key as product_key,
	pi.prd_nm as product_name,
	pi.prd_cost as product_cost,
	pi.prd_line as product_line,
	pi.prd_start_dt as start_date,
	pc.CAT as category,
	pc.SUBCAT as subcategory,
	pc.MAINTENANCE as maintenance
FROM Silver.CRM_Prd_Info as pi
LEFT JOIN Silver.ERP_Px_Cat_G1v2 as pc
ON pi.cat_id=pc.ID
where pi.prd_end_dt is null---Historical Data is Filtered
SELECT * FROM Gold.Dim_Product
--- ===============================================
-------         Fact Sales_Details
--- ===============================================
CREATE VIEW GOLD.Fact_Sales AS 
SELECT 
	sd.sls_ord_num as order_number,
	dp.product_number,
	dc.customer_number,
	sd.sls_order_dt as order_date,
	sd.sls_ship_dt as ship_date,
	sd.sls_due_dt as due_date,
	sd.sls_sales,
	sd.sls_quantity,
	sd.sls_price
FROM Silver.CRM_Sales_Details as sd
LEFT JOIN Gold.Dim_Product AS dp
ON sd.sls_prd_key=dp.product_key
LEFT JOIN Gold.Dim_Customer as dc
ON sd.sls_cust_id=dc.customer_id

